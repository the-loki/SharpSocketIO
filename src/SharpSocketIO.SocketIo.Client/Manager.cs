using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using SharpSocketIO.ComponentEmitter;
using SharpSocketIO.EngineIo.Client;
using SharpSocketIO.SocketIoClient.Contrib;
using SharpSocketIO.SocketIo.Parser;
using SharpSocketIO.SocketIo.Parser.Commons;
using SiPacketType = SharpSocketIO.SocketIo.Parser.Commons.PacketType;

namespace SharpSocketIO.SocketIoClient;

/// <summary>
/// Port of lib/manager.ts. Wraps an engine.io-client engine, owns a socket.io Decoder/Encoder,
/// manages namespace Sockets, and handles reconnect with backoff.
/// </summary>
public sealed class Manager : Emitter<UnitEvents>
{
    public ManagerOptions Options { get; }
    public Encoder Encoder { get; } = new();
    private readonly Decoder _decoder;
    private readonly ConcurrentDictionary<string, SocketIoClientSocket> _nsps = new();
    private readonly Backoff _backoff;
    private EngineIoClientSocket? _engine;
    private int _reconnectionAttempts;
    private bool _reconnecting;
    private bool _skipReconnect;
    private readonly string _uri;

    public bool Reconnecting => _reconnecting;
    public EngineIoClientSocket? Engine => _engine;

    public Manager(string uri, ManagerOptions? opts = null)
    {
        _uri = uri;
        Options = opts ?? new ManagerOptions();
        _decoder = new Decoder();
        _backoff = new Backoff(new BackoffOptions
        {
            Min = Options.ReconnectionDelay,
            Max = Options.ReconnectionDelayMax,
            Jitter = Options.RandomizationFactor,
        });

        _decoder.On("decoded", args => OnDecoded((Packet)args[0]));

        if (Options.AutoConnect) _ = OpenAsync();
    }

    public async Task OpenAsync()
    {
        _skipReconnect = false;
        _engine = new EngineIoClientSocket(_uri, new EngineIo.Client.SocketOptions
        {
            Path = Options.Path,
            Transports = Options.Transports,
            Reconnection = false, // Manager handles reconnect, not the engine
            Upgrade = false, // Polling-only for stability; upgrade path works but has timing sensitivity under load
        });
        _engine.On("open", _ => OnEngineOpen());
        _engine.On("message", args => OnEngineMessage(args));
        _engine.On("close", args => OnEngineClose(args));
        _engine.On("error", args => EmitReserved("error", args));
        await _engine.OpenAsync();
    }

    private void OnEngineOpen()
    {
        _reconnectionAttempts = 0;
        if (_reconnecting)
        {
            _reconnecting = false;
            _backoff.Reset();
            EmitReserved("reconnect", _reconnectionAttempts);
        }
        // (re)connect all namespace sockets
        foreach (var nsp in _nsps.Values) _ = nsp.ConnectAsync();
        EmitReserved("open");
    }

    private void OnEngineMessage(object[] args)
    {
        if (args.Length == 0) return;
        if (args[0] is SharpSocketIO.EngineIo.Parser.Commons.RawData rd)
        {
            var text = rd.Kind == SharpSocketIO.EngineIo.Parser.Commons.RawDataKind.String ? rd.AsString() : null;
            if (text != null) _decoder.Add(text);
        }
        else if (args[0] is string s)
        {
            _decoder.Add(s);
        }
    }

    private void OnDecoded(Packet packet)
    {
        var nsp = string.IsNullOrEmpty(packet.Nsp) ? "/" : packet.Nsp;
        if (_nsps.TryGetValue(nsp, out var socket))
        {
            socket.OnPacket(packet);
        }
    }

    private void OnEngineClose(object[] args)
    {
        EmitReserved("close", args);
        if (Options.Reconnection && !_skipReconnect)
        {
            Reconnect();
        }
    }

    private void Reconnect()
    {
        if (_reconnectionAttempts >= Options.ReconnectionAttempts) return;
        _reconnecting = true;
        var delay = _backoff.Duration();
        _reconnectionAttempts++;
        EmitReserved("reconnect_attempt", _reconnectionAttempts);
        new Timer(_ =>
        {
            if (_skipReconnect) return;
            _ = OpenAsync();
        }, null, delay, Timeout.Infinite);
    }

    /// <summary>Gets or creates a namespace Socket.</summary>
    public SocketIoClientSocket Socket(string nsp = "/")
    {
        if (!nsp.StartsWith("/")) nsp = "/" + nsp;
        return _nsps.GetOrAdd(nsp, name => new SocketIoClientSocket(this, name));
    }

    /// <summary>Sends a socket.io packet via the engine as an engine.io message.</summary>
    internal void SendPacket(Packet packet)
    {
        if (_engine == null) return;
        foreach (var part in Encoder.Encode(packet))
        {
            if (part is string s) _engine.Send(s);
            else if (part is byte[] b) _engine.Send(b);
        }
    }

    public void DisconnectAll()
    {
        _skipReconnect = true;
        _engine?.Close();
    }
}
