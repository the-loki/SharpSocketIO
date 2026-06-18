using System;
using System.Collections.Generic;
using SharpSocketIO.ComponentEmitter;
using SharpSocketIO.EngineIo.Commons;
using SharpSocketIO.EngineIo.Parser.Commons;
using EioParser = SharpSocketIO.EngineIo.Parser;

namespace SharpSocketIO.EngineIo.Transports;

/// <summary>Port of lib/transport.ts (abstract Transport base).</summary>
public abstract class Transport : Emitter<UnitEvents>
{
    public string? Sid { get; set; }
    public bool Writable { get; set; }
    public int Protocol { get; }
    public ReadyState ReadyState { get; protected set; } = ReadyState.Open;
    public bool Discarded { get; private set; }
    public bool SupportsBinary { get; }

    protected Transport(IEngineRequest req)
    {
        Protocol = req.Query.TryGetValue("EIO", out var eio) && eio == "4" ? 4 : 3;
        SupportsBinary = !(req.Query.TryGetValue("b64", out var b64) && !string.IsNullOrEmpty(b64));
    }

    public void Discard() => Discarded = true;

    public virtual void OnRequest(IEngineRequest req) { }

    public void Close(Action? callback = null)
    {
        if (ReadyState == ReadyState.Closed || ReadyState == ReadyState.Closing) return;
        ReadyState = ReadyState.Closing;
        DoClose(callback ?? (() => { }));
    }

    protected void OnError(string msg, object? desc = null)
    {
        var err = new TransportException(msg) { Description = desc };
        Emit("error", err);
    }

    protected void OnPacket(Packet packet) => Emit("packet", packet);

    protected void OnData(RawData data)
    {
        // For v4 we use the v4 parser; v3 paths route through ParserV3 at the call site.
        if (Protocol == 4) OnPacket(EioParser.DecodePacket.Decode(data));
    }

    protected void OnClose()
    {
        ReadyState = ReadyState.Closed;
        Emit("close");
    }

    public abstract string Name { get; }
    public abstract void Send(IReadOnlyList<Packet> packets);
    public abstract void DoClose(Action? callback = null);
}

public sealed class TransportException : Exception
{
    public TransportException(string msg) : base(msg) { Type = "TransportError"; }
    public string Type { get; }
    public object? Description { get; set; }
}
