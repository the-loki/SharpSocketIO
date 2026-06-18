using System.Collections.Generic;
using System.Linq;
using SharpSocketIO.ComponentEmitter;
using SharpSocketIO.SocketIo.Adapter;
using AdapterBase = SharpSocketIO.SocketIo.Adapter.Adapter;
using SharpSocketIO.SocketIo.Parser;
using SharpSocketIO.SocketIo.Parser.Commons;
using SiPacketType = SharpSocketIO.SocketIo.Parser.Commons.PacketType;

namespace SharpSocketIO.SocketIo;

/// <summary>Port of lib/socket.ts — a socket.io Socket bound to a Namespace + Client.</summary>
public sealed class Socket : Emitter<UnitEvents>
{
    public string Id { get; }
    public bool Connected { get; private set; }
    public bool Recovered { get; }
    public Handshake Handshake { get; }
    public object? Data { get; set; }
    public Namespace Namespace { get; }
    public Client Client { get; }
    public AdapterBase Adapter { get; }

    private readonly Dictionary<int, System.Action<object[]>> _acks = new();
    private int _ackId;

    public Socket(Namespace nsp, Client client, Handshake handshake, string id)
    {
        Namespace = nsp;
        Client = client;
        Adapter = nsp.Adapter;
        Handshake = handshake;
        Id = id;
    }

    public Socket Join(string room) { Adapter.Add(Id, room); return this; }
    public Socket Leave(string room) { Adapter.Del(Id, room); return this; }

    public async System.Threading.Tasks.Task<IReadOnlyCollection<string>> RoomsAsync()
        => await Adapter.RoomsAsync(Id);

    /// <summary>Emits an event to this client (server→client).</summary>
    public new void Emit(string eventName, params object[] args)
    {
        var packet = new Packet
        {
            Type = SiPacketType.Event,
            Nsp = Namespace.Name,
            Data = new[] { eventName }.Concat(args).ToArray(),
        };
        Client.SendPacket(packet);
    }

    /// <summary>Emits an event with an ack; the ack callback is invoked when the client replies.</summary>
    public void EmitWithAck(string eventName, System.Action<object[]> ack, params object[] args)
    {
        int id = _ackId++;
        _acks[id] = ack;
        var packet = new Packet
        {
            Type = SiPacketType.Event,
            Nsp = Namespace.Name,
            Data = new[] { eventName }.Concat(args).ToArray(),
            Id = id,
        };
        Client.SendPacket(packet);
    }

    /// <summary>Called by the Client when a socket.io packet arrives for this socket.</summary>
    public void OnPacket(Packet packet)
    {
        if (packet.Type == SiPacketType.Event || packet.Type == SiPacketType.BinaryEvent)
        {
            if (!Connected) return; // ignore events received after disconnection
            var data = packet.Data as System.Collections.IList;
            if (data == null || data.Count == 0) return;
            var eventName = data[0]?.ToString();
            var args = data.Cast<object>().Skip(1).ToList();
            if (packet.Id.HasValue)
            {
                int ackId = packet.Id.Value;
                var ackInvoker = new AckCallback(data2 => EmitAck(ackId, data2));
                args.Add(ackInvoker);
                base.Emit(eventName!, args.ToArray());
            }
            else
            {
                base.Emit(eventName!, args.ToArray());
            }
        }
        else if (packet.Type == SiPacketType.Ack)
        {
            int ackId = packet.Id ?? -1;
            if (_acks.TryGetValue(ackId, out var ack))
            {
                var data = (packet.Data as System.Collections.IList)?.Cast<object>().ToArray() ?? System.Array.Empty<object>();
                ack(data);
                _acks.Remove(ackId);
            }
        }
    }

    private void EmitAck(int ackId, object[] data)
    {
        Client.SendPacket(new Packet
        {
            Type = SiPacketType.Ack,
            Nsp = Namespace.Name,
            Id = ackId,
            Data = data.ToList(),
        });
    }

    /// <summary>Connects the socket to its namespace (called by Namespace.Add).</summary>
    public void OnConnect()
    {
        Connected = true;
        EmitReserved("connect");
    }

    public void OnDisconnect(string reason = DisconnectReasons.ClientNamespaceDisconnect)
    {
        if (!Connected) return;
        Connected = false;
        // Emit "disconnecting" while socket is still in its rooms (handlers can read socket rooms)
        EmitReserved("disconnecting", reason);
        Adapter.DelAll(Id);
        EmitReserved("disconnect", reason);
        Namespace.Remove(this);
    }

    /// <summary>Disconnects this socket (server-initiated).</summary>
    public Socket Disconnect()
    {
        if (!Connected) return this;
        Client.SendPacket(new Packet { Type = SiPacketType.Disconnect, Nsp = Namespace.Name });
        OnDisconnect(DisconnectReasons.ServerNamespaceDisconnect);
        return this;
    }
}

/// <summary>Ack callback passed as the last arg of an event handler when the client expects an ack.</summary>
public sealed class AckCallback
{
    private readonly System.Action<object[]> _fn;
    public AckCallback(System.Action<object[]> fn) { _fn = fn; }
    public void Send(params object[] args) => _fn(args);
}
