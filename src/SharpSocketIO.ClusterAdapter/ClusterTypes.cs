using System.Collections.Generic;
using SharpSocketIO.SocketIo.Adapter;

namespace SharpSocketIO.ClusterAdapter;

public enum MessageType
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
    BroadcastClientCount,
    BroadcastAck,
    AdapterClose,
}

public sealed class ClusterMessageOpts
{
    public string[] Rooms { get; set; } = System.Array.Empty<string>();
    public string[] Except { get; set; } = System.Array.Empty<string>();
    public BroadcastFlags? Flags { get; set; }
}

public abstract class ClusterMessage
{
    public string Uid { get; set; } = "";
    public string Nsp { get; set; } = "";
    public MessageType Type { get; protected set; }
}

public sealed class BroadcastMessage : ClusterMessage
{
    public string Packet { get; set; } = "";
    public ClusterMessageOpts Opts { get; set; } = new();
    public string? RequestId { get; set; }
    public BroadcastMessage() { Type = MessageType.Broadcast; }
}

public sealed class SocketsJoinMessage : ClusterMessage
{
    public ClusterMessageOpts Opts { get; set; } = new();
    public string[] Rooms { get; set; } = System.Array.Empty<string>();
    public SocketsJoinMessage() { Type = MessageType.SocketsJoin; }
}

public sealed class SocketsLeaveMessage : ClusterMessage
{
    public ClusterMessageOpts Opts { get; set; } = new();
    public string[] Rooms { get; set; } = System.Array.Empty<string>();
    public SocketsLeaveMessage() { Type = MessageType.SocketsLeave; }
}

public sealed class FetchSocketsMessage : ClusterMessage
{
    public ClusterMessageOpts Opts { get; set; } = new();
    public string RequestId { get; set; } = "";
    public FetchSocketsMessage() { Type = MessageType.FetchSockets; }
}

public sealed class ServerSideEmitMessage : ClusterMessage
{
    public string? RequestId { get; set; }
    public object[] Packet { get; set; } = System.Array.Empty<object>();
    public ServerSideEmitMessage() { Type = MessageType.ServerSideEmit; }
}

public sealed class HeartbeatMessage : ClusterMessage
{
    public HeartbeatMessage() { Type = MessageType.Heartbeat; }
}

public sealed class AdapterCloseMessage : ClusterMessage
{
    public AdapterCloseMessage() { Type = MessageType.AdapterClose; }
}

public abstract class ClusterResponse
{
    public string Uid { get; set; } = "";
    public string Nsp { get; set; } = "";
    public MessageType Type { get; protected set; }
    public string RequestId { get; set; } = "";
}

public sealed class FetchSocketsResponse : ClusterResponse
{
    public List<RemoteSocketInfo> Sockets { get; set; } = new();
    public FetchSocketsResponse() { Type = MessageType.FetchSocketsResponse; }
}

public sealed class ServerSideEmitResponse : ClusterResponse
{
    public object[] Packet { get; set; } = System.Array.Empty<object>();
    public ServerSideEmitResponse() { Type = MessageType.ServerSideEmitResponse; }
}

public sealed class RemoteSocketInfo
{
    public string Id { get; set; } = "";
    public string[] Rooms { get; set; } = System.Array.Empty<string>();
    public object? Data { get; set; }
}

public sealed class ClusterAdapterOptions
{
    public int HeartbeatInterval { get; set; } = 5000;
    public int HeartbeatTimeout { get; set; } = 10000;
}
