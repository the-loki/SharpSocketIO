using SharpSocketIO.ComponentEmitter;

namespace SharpSocketIO.SocketIo;

/// <summary>
/// Abstraction over an engine.io connection (the raw transport session). The socket.io
/// Client uses this to send/receive engine.io "message" packets (which carry socket.io
/// packets). 5B provides the implementation that adapts a SharpSocketIO.EngineIo.Socket.
/// </summary>
public interface IEngineIoConnection
{
    string Id { get; }
    int Protocol { get; }
    Handshake Handshake { get; }
    void Send(string encodedPayload);
    void Close(bool discard = false);
    Emitter Events { get; } // emits "data" (string), "close", "error"
}
