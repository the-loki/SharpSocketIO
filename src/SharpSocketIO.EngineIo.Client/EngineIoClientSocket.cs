using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using SharpSocketIO.ComponentEmitter;
using SharpSocketIO.EngineIo.Client.Contrib;
using SharpSocketIO.EngineIo.Client.Transports;
using SharpSocketIO.EngineIo.Parser;
using SharpSocketIO.EngineIo.Parser.Commons;
using EioPacketType = SharpSocketIO.EngineIo.Parser.Commons.PacketType;

namespace SharpSocketIO.EngineIo.Client;

public enum SocketReadyState { Opening, Open, Closing, Closed }

/// <summary>
/// Port of lib/socket.ts (client) lifecycle subset for CC-1: open/handshake via polling,
/// send/receive string, ping/pong, close. Upgrade is CC-2; reconnect is CC-3.
/// </summary>
public sealed class EngineIoClientSocket : Emitter<UnitEvents>
{
    public const int Protocol = 4;

    public SocketOptions Opts { get; }
    public Transport? Transport { get; private set; }
    public string? Id { get; private set; }
    public SocketReadyState ReadyState { get; private set; } = SocketReadyState.Opening;

    private readonly object _gate = new();
    private int _pingInterval;
    private int _pingTimeout;
    private Timer? _pingIntervalTimer;
    private Timer? _pingTimeoutTimer;
    private List<string>? _upgrades;

    public EngineIoClientSocket(string uri = "", SocketOptions? opts = null)
    {
        Opts = opts ?? new SocketOptions();
        if (!string.IsNullOrEmpty(uri))
        {
            var parsed = EngineIoUri.Parse(uri);
            Opts.Secure = parsed.Secure;
            Opts.Hostname = parsed.Host;
            Opts.Port = string.IsNullOrEmpty(parsed.Port) ? null : parsed.Port;
            Opts.Host = parsed.Host + (string.IsNullOrEmpty(parsed.Port) ? "" : ":" + parsed.Port);
            if (!string.IsNullOrEmpty(parsed.Query))
            {
                foreach (var kv in Parseqs.Decode(parsed.Query)) Opts.Query[kv.Key] = kv.Value;
            }
        }
    }

    public Task OpenAsync()
    {
        lock (_gate) ReadyState = SocketReadyState.Opening;
        // Pick the initial transport: the first transport in Opts.Transports that the
        // server will accept (mirrors socket.createTransport(name) with the first name).
        var initialName = Opts.Transports.Count > 0 ? Opts.Transports[0] : "polling";
        Transport = CreateTransport(initialName);
        // The initial transport must NOT carry a sid (handshake); CreateTransport copies
        // Opts.Query which may be empty — that's fine.
        Transport.On("open", _ => { /* handshake driven by the open *packet* */ });
        Transport.On("packet", args => OnPacket((Packet)args[0]));
        Transport.On("error", args => Emit("error", args));
        Transport.On("close", args =>
        {
            OnCloseInternal("transport closed");
        });
        Transport.Open();
        return Task.CompletedTask;
    }

    private void OnPacket(Packet packet)
    {
        switch (packet.Type)
        {
            case EioPacketType.Open:
                ParseOpenPacket(packet.Data?.AsString() ?? "");
                lock (_gate) ReadyState = SocketReadyState.Open;
                SchedulePingTimeout();
                Emit("open");
                // Trigger upgrade probes (mirrors socket.maybeUpgrade): for each advertised
                // upgrade not equal to the current transport, probe it.
                if (Opts.Upgrade && _upgrades != null)
                {
                    foreach (var u in _upgrades)
                    {
                        if (Opts.Transports.Contains(u) && u != Transport?.Name)
                        {
                            Probe(u);
                        }
                    }
                }
                break;
            case EioPacketType.Ping:
                Transport!.Send(new[] { new Packet(EioPacketType.Pong, packet.Data ?? default) });
                SchedulePingTimeout();
                break;
            case EioPacketType.Pong:
                SchedulePingTimeout();
                break;
            case EioPacketType.Message:
                Emit("message", packet.Data ?? default);
                break;
            case EioPacketType.Close:
                OnCloseInternal("transport closed");
                break;
        }
    }

    private void ParseOpenPacket(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        Id = root.GetProperty("sid").GetString();
        _pingInterval = root.GetProperty("pingInterval").GetInt32();
        _pingTimeout = root.GetProperty("pingTimeout").GetInt32();
        _upgrades = new List<string>();
        if (root.TryGetProperty("upgrades", out var up))
        {
            foreach (var u in up.EnumerateArray()) _upgrades.Add(u.GetString()!);
        }
        if (Transport != null && Id != null) Transport.Query["sid"] = Id!;
    }

    public void Send(string data)
    {
        Transport?.Send(new[] { new Packet(EioPacketType.Message, new RawData(data)) });
    }

    public void Send(byte[] data)
    {
        Transport?.Send(new[] { new Packet(EioPacketType.Message, new RawData(data)) });
    }

    private void SchedulePing()
    {
        _pingIntervalTimer?.Dispose();
        _pingIntervalTimer = new Timer(_ =>
        {
            lock (_gate)
            {
                if (ReadyState != SocketReadyState.Open) return;
                Transport?.Send(new[] { new Packet(EioPacketType.Ping, default) });
                SchedulePingTimeout();
            }
        }, null, _pingInterval > 0 ? _pingInterval : 25000, Timeout.Infinite);
    }

    private void SchedulePingTimeout()
    {
        _pingTimeoutTimer?.Dispose();
        _pingTimeoutTimer = new Timer(_ =>
        {
            // server-initiated ping not received in time → close
            OnCloseInternal("ping timeout");
        }, null, (_pingInterval > 0 ? _pingInterval : 25000) + (_pingTimeout > 0 ? _pingTimeout : 20000),
        Timeout.Infinite);
    }

    public void Close()
    {
        lock (_gate)
        {
            if (ReadyState == SocketReadyState.Closed) return;
            ReadyState = SocketReadyState.Closing;
        }
        Transport?.Send(new[] { new Packet(EioPacketType.Close) });
        Transport?.Close();
        OnCloseInternal("forced close");
    }

    public bool Upgraded { get; private set; }

    /// <summary>Creates a transport instance by name, seeded with the current sid query.</summary>
    public Transport CreateTransport(string name)
    {
        var query = new Dictionary<string, string>(Opts.Query);
        if (Id != null) query["sid"] = Id;
        var probeOpts = new SocketOptions
        {
            Host = Opts.Host,
            Hostname = Opts.Hostname,
            Secure = Opts.Secure,
            Port = Opts.Port,
            Path = Opts.Path,
            Query = query,
            Upgrade = false,
            TimestampRequests = Opts.TimestampRequests,
            TimestampParam = Opts.TimestampParam,
            ForceBase64 = Opts.ForceBase64,
            Transports = Opts.Transports,
        };
        return name switch
        {
            "websocket" => new WebSocketTransport(probeOpts),
            "polling" => new PollingTransport(probeOpts),
            _ => throw new ArgumentException($"unknown transport: {name}"),
        };
    }

    /// <summary>
    /// Port of socket._probe. Client-side upgrade handshake: open the new transport,
    /// send ping/"probe", await a single pong/"probe", pause polling, swap transport,
    /// send the "upgrade" packet, emit "upgrade", flush.
    /// </summary>
    public void Probe(string name)
    {
        var transport = CreateTransport(name);
        bool failed = false;
        Action<object[]>? onPacket = null;
        Action<object[]>? onError = null;
        Action<object[]>? onClose = null;
        Action<object[]>? onTransportOpen = null;

        void Cleanup()
        {
            if (onTransportOpen != null) transport.Off("open", onTransportOpen);
            if (onPacket != null) transport.Off("packet", onPacket);
            if (onError != null) transport.Off("error", onError);
            if (onClose != null) transport.Off("close", onClose);
        }

        void Freeze()
        {
            if (failed) return;
            failed = true;
            Cleanup();
            transport.Close();
        }

        onTransportOpen = _ =>
        {
            if (failed) return;
            transport.Send(new[] { new Packet(EioPacketType.Ping, new RawData("probe")) });
            onPacket = args =>
            {
                if (failed) return;
                var msg = (Packet)args[0];
                if (msg.Type == EioPacketType.Pong && msg.Data?.AsString() == "probe")
                {
                    Transport?.Pause(() =>
                    {
                        if (failed || ReadyState == SocketReadyState.Closed) return;
                        Cleanup();
                        SetTransport(transport);
                        transport.Send(new[] { new Packet(EioPacketType.Upgrade) });
                        Emit("upgrade", transport);
                    });
                }
                else
                {
                    Emit("upgradeError", new InvalidOperationException("probe error"));
                }
            };
            transport.Once("packet", onPacket);
        };

        onError = args =>
        {
            Freeze();
            Emit("upgradeError", args.Length > 0 ? args[0] : new InvalidOperationException("probe error"));
        };
        onClose = _ =>
        {
            if (!failed) onError!(new object[] { new InvalidOperationException("transport closed") });
        };

        transport.On("open", onTransportOpen);
        transport.On("error", onError);
        transport.On("close", onClose);
        transport.Open();
    }

    /// <summary>Swaps the active transport, re-wiring packet/error/close handlers.</summary>
    private void SetTransport(Transport transport)
    {
        Transport?.Close();
        Transport = transport;
        transport.On("packet", args => OnPacket((Packet)args[0]));
        transport.On("error", args => Emit("error", args));
        transport.On("close", args => OnCloseInternal("transport closed"));
        Upgraded = transport.Name == "websocket";
    }

    private void OnCloseInternal(string reason)
    {
        lock (_gate)
        {
            if (ReadyState == SocketReadyState.Closed) return;
            ReadyState = SocketReadyState.Closed;
            _pingIntervalTimer?.Dispose();
            _pingTimeoutTimer?.Dispose();
        }
        Emit("close", reason);
    }
}
