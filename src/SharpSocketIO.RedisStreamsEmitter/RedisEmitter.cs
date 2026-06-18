using System.Collections.Generic;
using System.Linq;
using SharpSocketIO.SocketIo.Parser;
using SharpSocketIO.SocketIo.Parser.Commons;
using SiPacketType = SharpSocketIO.SocketIo.Parser.Commons.PacketType;

namespace SharpSocketIO.RedisStreamsEmitter;

/// <summary>
/// Port of lib/index.ts Emitter. A standalone emitter that publishes socket.io broadcast
/// documents to a Redis Stream (via IRedisPublisher / XADD).
/// </summary>
public sealed class RedisEmitter
{
    public string StreamKey { get; }
    public string Nsp { get; }
    private readonly IRedisPublisher _publisher;
    private readonly Encoder _encoder = new();

    public RedisEmitter(IRedisPublisher publisher, string nsp = "/", RedisStreamsEmitterOptions? opts = null)
    {
        _publisher = publisher;
        Nsp = nsp;
        opts ??= new RedisStreamsEmitterOptions();
        StreamKey = opts.StreamKey;
    }

    public RedisEmitter Of(string nsp)
    {
        if (!nsp.StartsWith("/")) nsp = "/" + nsp;
        return new RedisEmitter(_publisher, nsp);
    }

    public void Emit(string eventName, params object[] args) =>
        new RedisBroadcastOperator(this).Emit(eventName, args);

    public RedisBroadcastOperator To(string room) => new RedisBroadcastOperator(this).To(room);
    public RedisBroadcastOperator To(IEnumerable<string> rooms) => new RedisBroadcastOperator(this).To(rooms);
    public RedisBroadcastOperator In(string room) => To(room);
    public RedisBroadcastOperator Except(string room) => new RedisBroadcastOperator(this).Except(room);
    public RedisBroadcastOperator Compress(bool compress) => new RedisBroadcastOperator(this).Compress(compress);
    public RedisBroadcastOperator Volatile() => new RedisBroadcastOperator(this).Volatile();

    internal void PublishBroadcast(HashSet<string> rooms, HashSet<string> except, RedisEmitterFlags? flags, string eventName, object[] args)
    {
        var packet = new Packet
        {
            Type = SiPacketType.Event,
            Nsp = Nsp,
            Data = new[] { eventName }.Concat(args).ToArray(),
        };
        string encoded = string.Empty;
        foreach (var part in _encoder.Encode(packet))
        {
            if (part is string s) { encoded = s; break; }
        }

        var doc = new RedisBroadcastDocument
        {
            Type = RedisMessageType.Broadcast,
            Nsp = Nsp,
            Data = new RedisBroadcastData
            {
                Rooms = rooms.ToList(),
                Except = except.ToList(),
                Flags = flags,
                Packet = encoded,
            },
        };
        var payload = System.Text.Json.JsonSerializer.Serialize(doc);
        _publisher.Publish(StreamKey, payload);
    }
}

public sealed class RedisBroadcastOperator
{
    private readonly RedisEmitter _emitter;
    private readonly HashSet<string> _rooms;
    private readonly HashSet<string> _except;
    private readonly RedisEmitterFlags _flags;

    public RedisBroadcastOperator(RedisEmitter emitter)
        : this(emitter, new HashSet<string>(), new HashSet<string>(), new RedisEmitterFlags()) { }

    private RedisBroadcastOperator(RedisEmitter emitter, HashSet<string> rooms, HashSet<string> except, RedisEmitterFlags flags)
    {
        _emitter = emitter; _rooms = rooms; _except = except; _flags = flags;
    }

    public RedisBroadcastOperator To(string room) => To(new[] { room });
    public RedisBroadcastOperator To(IEnumerable<string> rooms)
    {
        var next = new HashSet<string>(_rooms);
        foreach (var r in rooms) next.Add(r);
        return new RedisBroadcastOperator(_emitter, next, _except, CloneFlags());
    }
    public RedisBroadcastOperator In(string room) => To(room);

    public RedisBroadcastOperator Except(string room) => Except(new[] { room });
    public RedisBroadcastOperator Except(IEnumerable<string> rooms)
    {
        var next = new HashSet<string>(_except);
        foreach (var r in rooms) next.Add(r);
        return new RedisBroadcastOperator(_emitter, _rooms, next, CloneFlags());
    }

    public RedisBroadcastOperator Compress(bool compress)
    {
        var flags = CloneFlags(); flags.Compress = compress;
        return new RedisBroadcastOperator(_emitter, _rooms, _except, flags);
    }

    public RedisBroadcastOperator Volatile()
    {
        var flags = CloneFlags(); flags.Volatile = true;
        return new RedisBroadcastOperator(_emitter, _rooms, _except, flags);
    }

    public void Emit(string eventName, params object[] args) =>
        _emitter.PublishBroadcast(_rooms, _except, _flags, eventName, args);

    private RedisEmitterFlags CloneFlags() => new()
    {
        Volatile = _flags.Volatile,
        Compress = _flags.Compress,
    };
}
