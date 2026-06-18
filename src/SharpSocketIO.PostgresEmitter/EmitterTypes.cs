using System.Collections.Generic;

namespace SharpSocketIO.PostgresEmitter;

/// <summary>Event types for messages between the emitter and server nodes.</summary>
public enum EventType
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

/// <summary>Broadcast flags.</summary>
public sealed class EmitterFlags
{
    public bool Volatile { get; set; }
    public bool Compress { get; set; }
}

/// <summary>A document published to the server cluster.</summary>
public sealed class BroadcastDocument
{
    public string Uid { get; set; } = "emitter";
    public EventType Type { get; set; }
    public string Nsp { get; set; } = "/";
    public BroadcastDocumentData Data { get; set; } = new();
}

public sealed class BroadcastDocumentData
{
    public List<string> Rooms { get; set; } = new();
    public List<string> Except { get; set; } = new();
    public EmitterFlags? Flags { get; set; }
    public object? Packet { get; set; }
}

/// <summary>Options for the Postgres emitter.</summary>
public sealed class PostgresEmitterOptions
{
    public string ChannelPrefix { get; set; } = "socket.io";
    public string TableName { get; set; } = "socket_io_attachments";
    public int PayloadThreshold { get; set; } = 8000;
}

/// <summary>
/// Transport seam for publishing a document. The Npgsql implementation would do
/// `NOTIFY channel, payload` (and a table insert for large/binary payloads).
/// </summary>
public interface IPostgresPublisher
{
    void Publish(string channel, string payload);
}
