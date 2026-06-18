using System;
using System.Collections.Generic;
using SharpSocketIO.ComponentEmitter;
using SharpSocketIO.EngineIo.Parser;
using SharpSocketIO.EngineIo.Parser.Commons;

namespace SharpSocketIO.EngineIo.Client;

public enum TransportState { Opening, Open, Closed, Pausing, Paused }

/// <summary>Port of lib/transport.ts (client).</summary>
public abstract class Transport : Emitter<UnitEvents>
{
    public SocketOptions Opts { get; }
    public Dictionary<string, string> Query { get; }
    public bool Writable { get; protected set; }
    public TransportState ReadyState { get; protected set; } = TransportState.Closed;

    protected Transport(SocketOptions opts)
    {
        Opts = opts;
        Query = new Dictionary<string, string>(opts.Query);
    }

    public Transport Open()
    {
        ReadyState = TransportState.Opening;
        DoOpen();
        return this;
    }

    public Transport Close()
    {
        if (ReadyState == TransportState.Opening || ReadyState == TransportState.Open)
        {
            DoClose();
            OnClose();
        }
        return this;
    }

    public void Send(IReadOnlyList<Packet> packets)
    {
        if (ReadyState == TransportState.Open) Write(packets);
    }

    protected void OnOpen()
    {
        ReadyState = TransportState.Open;
        Writable = true;
        EmitReserved("open");
    }

    protected void OnData(RawData data)
    {
        var packet = DecodePacket.Decode(data);
        OnPacket(packet);
    }

    protected void OnPacket(Packet packet) => EmitReserved("packet", packet);

    protected void OnClose(CloseDetails? details = null)
    {
        ReadyState = TransportState.Closed;
        EmitReserved("close", details ?? new CloseDetails());
    }

    protected void OnError(string reason, object? description = null, object? context = null)
    {
        EmitReserved("error", new TransportError(reason, description, context));
    }

    public abstract string Name { get; }
    public virtual void Pause(Action onPause) { onPause(); }
    protected abstract void DoOpen();
    protected abstract void DoClose();
    protected abstract void Write(IReadOnlyList<Packet> packets);
}

public sealed class CloseDetails
{
    public string? Description { get; set; }
    public object? Context { get; set; }
}

public sealed class TransportError : Exception
{
    public TransportError(string reason, object? description, object? context) : base(reason)
    {
        Description = description;
        Context = context;
        Data["type"] = "TransportError";
    }
    public object? Description { get; }
    public object? Context { get; }
}
