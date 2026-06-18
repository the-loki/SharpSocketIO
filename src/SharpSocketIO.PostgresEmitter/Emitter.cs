using System.Collections.Generic;
using System.Linq;
using SharpSocketIO.SocketIo.Parser;
using SharpSocketIO.SocketIo.Parser.Commons;
using SiPacketType = SharpSocketIO.SocketIo.Parser.Commons.PacketType;

namespace SharpSocketIO.PostgresEmitter;

/// <summary>
/// Port of lib/index.ts Emitter. A standalone emitter that publishes socket.io broadcast
/// documents to a Postgres NOTIFY channel (via IPostgresPublisher). Targets rooms/namespaces.
/// </summary>
public sealed class Emitter
{
    public string Channel { get; }
    public string TableName { get; }
    public int PayloadThreshold { get; }
    public string Nsp { get; }
    private readonly IPostgresPublisher _publisher;
    private readonly Encoder _encoder = new();

    public Emitter(IPostgresPublisher publisher, string nsp = "/", PostgresEmitterOptions? opts = null)
    {
        _publisher = publisher;
        Nsp = nsp;
        opts ??= new PostgresEmitterOptions();
        Channel = $"{opts.ChannelPrefix}#{nsp}";
        TableName = opts.TableName;
        PayloadThreshold = opts.PayloadThreshold;
    }

    /// <summary>Returns a new emitter for the given namespace.</summary>
    public Emitter Of(string nsp)
    {
        if (!nsp.StartsWith("/")) nsp = "/" + nsp;
        return new Emitter(_publisher, nsp);
    }

    /// <summary>Emits to all clients.</summary>
    public void Emit(string eventName, params object[] args)
    {
        new BroadcastOperator(this).Emit(eventName, args);
    }

    /// <summary>Targets a room.</summary>
    public BroadcastOperator To(string room) => new BroadcastOperator(this).To(room);
    public BroadcastOperator To(IEnumerable<string> rooms) => new BroadcastOperator(this).To(rooms);
    public BroadcastOperator In(string room) => To(room);
    public BroadcastOperator Except(string room) => new BroadcastOperator(this).Except(room);
    public BroadcastOperator Compress(bool compress) => new BroadcastOperator(this).Compress(compress);
    public BroadcastOperator Volatile() => new BroadcastOperator(this).Volatile();

    /// <summary>Called by BroadcastOperator.Emit — builds and publishes the document.</summary>
    internal void PublishBroadcast(HashSet<string> rooms, HashSet<string> except, EmitterFlags? flags, string eventName, object[] args)
    {
        // encode the socket.io EVENT packet
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

        var doc = new BroadcastDocument
        {
            Type = EventType.Broadcast,
            Nsp = Nsp,
            Data = new BroadcastDocumentData
            {
                Rooms = rooms.ToList(),
                Except = except.ToList(),
                Flags = flags,
                Packet = encoded,
            },
        };
        var payload = System.Text.Json.JsonSerializer.Serialize(doc);
        _publisher.Publish(Channel, payload);
    }
}

/// <summary>
/// Port of lib/index.ts BroadcastOperator. Immutable builder: To/In/Except/Compress/Volatile
/// return new instances; Emit publishes a broadcast document.
/// </summary>
public sealed class BroadcastOperator
{
    private readonly Emitter _emitter;
    private readonly HashSet<string> _rooms;
    private readonly HashSet<string> _except;
    private readonly EmitterFlags _flags;

    public BroadcastOperator(Emitter emitter)
        : this(emitter, new HashSet<string>(), new HashSet<string>(), new EmitterFlags()) { }

    private BroadcastOperator(Emitter emitter, HashSet<string> rooms, HashSet<string> except, EmitterFlags flags)
    {
        _emitter = emitter; _rooms = rooms; _except = except; _flags = flags;
    }

    public BroadcastOperator To(string room) => To(new[] { room });
    public BroadcastOperator To(IEnumerable<string> rooms)
    {
        var next = new HashSet<string>(_rooms);
        foreach (var r in rooms) next.Add(r);
        return new BroadcastOperator(_emitter, next, _except, CloneFlags());
    }
    public BroadcastOperator In(string room) => To(room);

    public BroadcastOperator Except(string room) => Except(new[] { room });
    public BroadcastOperator Except(IEnumerable<string> rooms)
    {
        var next = new HashSet<string>(_except);
        foreach (var r in rooms) next.Add(r);
        return new BroadcastOperator(_emitter, _rooms, next, CloneFlags());
    }

    public BroadcastOperator Compress(bool compress)
    {
        var flags = CloneFlags(); flags.Compress = compress;
        return new BroadcastOperator(_emitter, _rooms, _except, flags);
    }

    public BroadcastOperator Volatile()
    {
        var flags = CloneFlags(); flags.Volatile = true;
        return new BroadcastOperator(_emitter, _rooms, _except, flags);
    }

    public void Emit(string eventName, params object[] args) =>
        _emitter.PublishBroadcast(_rooms, _except, _flags, eventName, args);

    private EmitterFlags CloneFlags() => new()
    {
        Volatile = _flags.Volatile,
        Compress = _flags.Compress,
    };
}
