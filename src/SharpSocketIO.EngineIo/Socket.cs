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
    public bool Upgraded { get; private set; }

    private readonly object _gate = new();
    private readonly List<Packet> _writeBuffer = new();
    private Timer? _pingIntervalTimer;
    private Timer? _pingTimeoutTimer;
    private readonly int _pingInterval;   // ms
    private readonly int _pingTimeout;    // ms
    private bool _upgrading;
    private Timer? _upgradeTimeoutTimer;
    private Action<object[]>? _upgradeOnPacket;

    private readonly Server _server;

    public Socket(string id, object server, Transport transport, int protocol)
    {
        Id = id;
        Protocol = protocol;
        Transport = transport;
        _server = (Server)server;
        _pingInterval = _server.Options.PingInterval;
        _pingTimeout = _server.Options.PingTimeout;
    }

    public void OnOpen()
    {
        WireTransportEvents();
        lock (_gate)
        {
            ReadyState = ReadyState.Open;
            Transport.Send(new[] { new Packet(EioPacketType.Open, MakeHandshakeData()) });
            SchedulePing();
            Emit("open");
        }
    }

    /// <summary>Async variant that awaits the transport's flush (needed for WS where Send is fire-and-forget).</summary>
    public async System.Threading.Tasks.Task OnOpenAsync()
    {
        WireTransportEvents();
        lock (_gate)
        {
            ReadyState = ReadyState.Open;
            SchedulePing();
            Emit("open");
        }
        if (Transport is Transports.WebSocketTransport wst)
        {
            await wst.SendAsync(new[] { new Packet(EioPacketType.Open, MakeHandshakeData()) });
        }
        else
        {
            Transport.Send(new[] { new Packet(EioPacketType.Open, MakeHandshakeData()) });
        }
    }

    /// <summary>Subscribes the socket's packet/error/close handlers to the current transport's events.</summary>
    private void WireTransportEvents()
    {
        Transport.On("packet", args => OnPacket((Packet)args[0]));
        Transport.On("error", args => Emit("error", args));
        Transport.On("close", _ => OnClose());
    }

    private RawData MakeHandshakeData()
    {
        var json = _server.BuildHandshakeData(Id, Transport.Name);
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

    /// <summary>
    /// Port of socket._maybeUpgrade. Drives the probe handshake: server listens on the
    /// new transport for a ping/"probe" → replies pong/"probe", then waits for an
    /// "upgrade" packet to swap transports.
    /// </summary>
    public void MaybeUpgrade(Transport newTransport, int upgradeTimeoutMs)
    {
        lock (_gate)
        {
            if (_upgrading || Upgraded)
            {
                newTransport.Close();
                return;
            }
            _upgrading = true;
        }

        void Cleanup()
        {
            lock (_gate) _upgrading = false;
            _upgradeTimeoutTimer?.Dispose();
            _upgradeTimeoutTimer = null;
            if (_upgradeOnPacket != null) newTransport.Off("packet", _upgradeOnPacket);
        }

        _upgradeOnPacket = args =>
        {
            var packet = (Packet)args[0];
            if (packet.Type == EioPacketType.Ping && packet.Data?.AsString() == "probe")
            {
                newTransport.Send(new[] { new Packet(EioPacketType.Pong, new RawData("probe")) });
            }
            else if (packet.Type == EioPacketType.Upgrade)
            {
                Cleanup();
                lock (_gate)
                {
                    Transport.Discard();
                    SetTransport(newTransport);
                    Upgraded = true;
                }
                Emit("upgrade", newTransport);
                Flush();
            }
            else
            {
                Cleanup();
                newTransport.Close();
            }
        };

        newTransport.On("packet", _upgradeOnPacket);

        _upgradeTimeoutTimer = new Timer(_ =>
        {
            Cleanup();
            if (newTransport.ReadyState == ReadyState.Open) newTransport.Close();
        }, null, upgradeTimeoutMs, Timeout.Infinite);
    }

    /// <summary>Swaps the active transport and re-wires its packet/error/close handlers.</summary>
    private void SetTransport(Transport transport)
    {
        Transport = transport;
        transport.On("packet", args => OnPacket((Packet)args[0]));
        transport.On("error", args => Emit("error", args));
        transport.On("close", _ => OnClose());
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
