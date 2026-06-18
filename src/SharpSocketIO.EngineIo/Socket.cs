using System;
using System.Collections.Generic;
using System.Threading;
using SharpSocketIO.ComponentEmitter;
using SharpSocketIO.EngineIo.Commons;
using SharpSocketIO.EngineIo.Parser.Commons;
using SharpSocketIO.EngineIo.Transports;
using EioPacketType = SharpSocketIO.EngineIo.Parser.Commons.PacketType;

namespace SharpSocketIO.EngineIo;

/// <summary>
/// Port of lib/socket.ts (lifecycle subset for 3A). Full upgrade flow is 3C.
/// Timers use System.Threading.Timer with the JS pingInterval/pingTimeout semantics.
/// </summary>
public sealed class Socket : Emitter<UnitEvents>
{
    public string Id { get; }
    public int Protocol { get; }
    public ReadyState ReadyState { get; private set; } = ReadyState.Opening;
    public Transport Transport { get; private set; }

    private readonly object _gate = new();
    private readonly List<Packet> _writeBuffer = new();
    private Timer? _pingIntervalTimer;
    private Timer? _pingTimeoutTimer;
    private readonly int _pingInterval;   // ms
    private readonly int _pingTimeout;    // ms

    public Socket(string id, object server, Transport transport, int protocol)
    {
        Id = id;
        Protocol = protocol;
        Transport = transport;
        _pingInterval = 25000;
        _pingTimeout = 20000;
    }

    public void OnOpen()
    {
        lock (_gate)
        {
            ReadyState = ReadyState.Open;
            Transport.Send(new[] { new Packet(EioPacketType.Open, MakeHandshakeData()) });
            SchedulePing();
            Emit("open");
        }
    }

    private RawData MakeHandshakeData()
    {
        var json = "{\"sid\":\"" + Id + "\",\"upgrades\":[\"websocket\"],\"pingInterval\":" + _pingInterval +
                   ",\"pingTimeout\":" + _pingTimeout + ",\"maxPayload\":1000000}";
        return new RawData(json);
    }

    public void Send(IReadOnlyList<Packet> packets)
    {
        lock (_gate)
        {
            if (ReadyState == ReadyState.Open) { _writeBuffer.AddRange(packets); Flush(); }
        }
    }

    private void Flush()
    {
        if (_writeBuffer.Count == 0) return;
        var snapshot = _writeBuffer.ToArray();
        _writeBuffer.Clear();
        Transport.Writable = true;
        Transport.Send(snapshot);
    }

    public void OnPacket(Packet packet)
    {
        lock (_gate)
        {
            switch (packet.Type)
            {
                case EioPacketType.Ping:
                    Transport.Send(new[] { new Packet(EioPacketType.Pong, packet.Data ?? default) });
                    break;
                case EioPacketType.Message:
                    Emit("message", packet.Data ?? default);
                    break;
                case EioPacketType.Close:
                    OnClose();
                    break;
            }
        }
    }

    private void SchedulePing()
    {
        _pingIntervalTimer?.Dispose();
        _pingIntervalTimer = new Timer(_ =>
        {
            lock (_gate)
            {
                if (ReadyState != ReadyState.Open) return;
                Transport.Send(new[] { new Packet(EioPacketType.Ping, default) });
                _pingTimeoutTimer?.Dispose();
                _pingTimeoutTimer = new Timer(_2 => OnClose(), null, _pingTimeout, Timeout.Infinite);
            }
        }, null, _pingInterval, Timeout.Infinite);
    }

    public void OnClose()
    {
        lock (_gate)
        {
            if (ReadyState == ReadyState.Closed) return;
            ReadyState = ReadyState.Closed;
            _pingIntervalTimer?.Dispose();
            _pingTimeoutTimer?.Dispose();
            Transport.Close();
            Emit("close");
        }
    }

    public void Close() => OnClose();
}
