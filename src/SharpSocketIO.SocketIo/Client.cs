using System.Collections.Generic;
using System.Linq;
using SharpSocketIO.SocketIo.Parser;
using SharpSocketIO.SocketIo.Parser.Commons;

namespace SharpSocketIO.SocketIo;

/// <summary>
/// Port of lib/client.ts. Wraps an engine.io connection; owns a socket.io Decoder/Encoder;
/// routes CONNECT/DISCONNECT/EVENT/ACK packets to the appropriate Namespace + Socket.
/// </summary>
public sealed class Client
{
    public IEngineIoConnection Conn { get; }
    public Server Server { get; }
    private readonly Encoder _encoder;
    private readonly Decoder _decoder;
    private readonly Dictionary<string, Socket> _socketsByNsp = new();

    public Client(Server server, IEngineIoConnection conn)
    {
        Server = server;
        Conn = conn;
        _encoder = server.Encoder;
        _decoder = new Decoder();
        _decoder.On("decoded", args => OnDecoded((Packet)args[0]));
        Conn.Events.On("data", args => OnData(args.Length > 0 ? args[0]?.ToString() ?? "" : ""));
        Conn.Events.On("close", args => OnClose(args.Length > 0 ? (args[0]?.ToString() ?? DisconnectReasons.ForcedClose) : DisconnectReasons.ForcedClose));
    }

    private void OnData(string data) => _decoder.Add(data);

    private void OnDecoded(Packet packet)
    {
        var nsp = string.IsNullOrEmpty(packet.Nsp) ? "/" : packet.Nsp;
        switch (packet.Type)
        {
            case PacketType.Connect:
                Connect(packet.Data, nsp);
                break;
            case PacketType.Disconnect:
                if (_socketsByNsp.TryGetValue(nsp, out var s)) s.OnDisconnect();
                break;
            case PacketType.Event:
            case PacketType.Ack:
            case PacketType.BinaryEvent:
            case PacketType.BinaryAck:
                if (_socketsByNsp.TryGetValue(nsp, out var s2)) s2.OnPacket(packet);
                break;
        }
    }

    private void Connect(object? auth, string nspName)
    {
        var nsp = Server.Of(nspName);
        var socket = nsp.Add(this, auth?.ToString());
        _socketsByNsp[nspName] = socket;
        // reply with CONNECT ack — protocol v5 requires {sid} in the data (as a JSON object, not a string)
        SendPacket(new Packet
        {
            Type = PacketType.Connect,
            Nsp = nspName,
            Data = new Dictionary<string, object> { ["sid"] = socket.Id },
        });
    }

    /// <summary>Encodes a socket.io packet and sends it down the engine.io connection.</summary>
    public void SendPacket(Packet packet)
    {
        foreach (var part in _encoder.Encode(packet))
        {
            if (part is string s) Conn.Send(s);
            else if (part is byte[] b) Conn.Send(b);
        }
    }

    /// <summary>Sends a pre-encoded string packet (used by Namespace broadcast delivery).</summary>
    public void SendRaw(string encoded) => Conn.Send(encoded);

    /// <summary>Sends pre-encoded parts (header + binary attachments) for broadcast delivery.</summary>
    public void SendRawParts(System.Collections.Generic.IReadOnlyList<object> parts)
    {
        foreach (var part in parts)
        {
            if (part is string s) Conn.Send(s);
            else if (part is byte[] b) Conn.Send(b);
        }
    }

    private void OnClose(string reason)
    {
        // Propagate the real disconnect reason (transport close, ping timeout, etc.)
        foreach (var s in _socketsByNsp.Values.ToList()) s.OnDisconnect(reason);
        _socketsByNsp.Clear();
    }
}
