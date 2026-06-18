using SharpSocketIO.ComponentEmitter;
using SharpSocketIO.EngineIo;
using SharpSocketIO.EngineIo.Parser.Commons;
using EioPacketType = SharpSocketIO.EngineIo.Parser.Commons.PacketType;

namespace SharpSocketIO.SocketIo;

/// <summary>
/// Adapts a SharpSocketIO.EngineIo.Socket (the server-side engine.io session) to the
/// IEngineIoConnection abstraction the socket.io Client consumes. engine.io "message"
/// packets become "data" events (string payload); socket.io sends become engine.io
/// "message" packets.
/// </summary>
public sealed class EngineIoSocketConnection : IEngineIoConnection
{
    private readonly EngineIo.Socket _socket;
    private readonly Emitter _events = new();

    public EngineIoSocketConnection(EngineIo.Socket socket)
    {
        _socket = socket;
        Id = socket.Id;
        Protocol = socket.Protocol;
        Handshake = new Handshake { Id = socket.Id };

        socket.On("message", args =>
        {
            if (args.Length > 0 && args[0] is RawData rd)
            {
                var text = rd.Kind == RawDataKind.String ? rd.AsString() : null;
                if (text != null) _events.Emit("data", text);
            }
        });
        socket.On("close", args => _events.Emit("close", args.Length > 0 ? args[0] : DisconnectReasons.TransportClose));
        socket.On("error", args => _events.Emit("error", args));
    }

    public string Id { get; }
    public int Protocol { get; }
    public Handshake Handshake { get; }
    public Emitter Events => _events;

    public void Send(string encodedPayload)
    {
        _socket.Send(new[] { new EngineIo.Parser.Commons.Packet(EioPacketType.Message, new RawData(encodedPayload)) });
    }

    public void Close(bool discard = false) => _socket.Close();
}
