using System.Collections.Generic;

namespace SharpSocketIO.RedisStreamsEmitter;

/// <summary>Message types for cluster communication (port of adapter-types MessageType).</summary>
public enum RedisMessageType
{
    InitialHeartbeat = 1,
    Heartbeat,
    Broadcast,
    SocketsJoin,
    SocketsLeave,
    DisconnectSockets,
    FetchSockets,
    FetchSocketsResponse,
    ServerSideEmit,
    ServerSideEmitResponse,
}

public sealed class RedisEmitterFlags
{
    public bool Volatile { get; set; }
    public bool Compress { get; set; }
}

public sealed class RedisBroadcastDocument
{
    public string Uid { get; set; } = "emitter";
    public RedisMessageType Type { get; set; }
    public string Nsp { get; set; } = "/";
    public RedisBroadcastData Data { get; set; } = new();
}

public sealed class RedisBroadcastData
{
    public List<string> Rooms { get; set; } = new();
    public List<string> Except { get; set; } = new();
    public RedisEmitterFlags? Flags { get; set; }
    public object? Packet { get; set; }
}

public sealed class RedisStreamsEmitterOptions
{
    public string StreamKey { get; set; } = "socket.io";
    public int PayloadThreshold { get; set; } = 8000;
}

/// <summary>
/// Transport seam for publishing to a Redis Stream. The StackExchange.Redis implementation
/// would do XADD with the document as a field.
/// </summary>
public interface IRedisPublisher
{
    void Publish(string streamKey, string payload);
}
