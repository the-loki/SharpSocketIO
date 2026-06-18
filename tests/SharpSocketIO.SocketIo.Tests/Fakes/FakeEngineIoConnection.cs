using System.Collections.Generic;
using SharpSocketIO.ComponentEmitter;
using SharpSocketIO.SocketIo;

namespace SharpSocketIO.SocketIo.Tests.Fakes;

/// <summary>Fake engine.io connection capturing sent packets; emits "data"/"close".</summary>
internal sealed class FakeEngineIoConnection : IEngineIoConnection
{
    public string Id { get; set; } = "conn-1";
    public int Protocol { get; set; } = 4;
    public Handshake Handshake { get; set; } = new();
    public List<string> Sent { get; } = new();
    public List<byte[]> SentBinary { get; } = new();
    public Emitter Events { get; } = new Emitter();
    public bool Closed { get; private set; }

    public void Send(string encodedPayload) => Sent.Add(encodedPayload);
    public void Send(byte[] binaryPayload) => SentBinary.Add(binaryPayload);
    public void Close(bool discard = false)
    {
        Closed = true;
        Events.Emit("close", discard ? "forced close" : "transport close");
    }

    public void ReceiveFromClient(string data) => Events.Emit("data", data);
}
