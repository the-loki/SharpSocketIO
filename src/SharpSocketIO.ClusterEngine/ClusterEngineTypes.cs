using System.Collections.Generic;

namespace SharpSocketIO.ClusterEngine;

/// <summary>Cluster-engine message types (port of MessageType enum).</summary>
public enum ClusterMessageType
{
    AcquireLock = 0,
    AcquireLockResponse,
    Drain,
    Packet,
    Upgrade,
    UpgradeResponse,
    Close,
}

/// <summary>Base for cluster-engine messages.</summary>
public abstract class ClusterEngineMessage
{
    public string SenderId { get; set; } = "";
    public string? RecipientId { get; set; }
    public ClusterMessageType Type { get; protected set; }
}

public sealed class AcquireLockMessage : ClusterEngineMessage
{
    public int RequestId { get; set; }
    public string SessionId { get; set; } = "";
    public string TransportName { get; set; } = "";
    public string RequestType { get; set; } = "read"; // "read" or "write"
    public AcquireLockMessage() { Type = ClusterMessageType.AcquireLock; }
}

public sealed class AcquireLockResponseMessage : ClusterEngineMessage
{
    public int RequestId { get; set; }
    public bool Success { get; set; }
    public AcquireLockResponseMessage() { Type = ClusterMessageType.AcquireLockResponse; }
}

public sealed class DrainMessage : ClusterEngineMessage
{
    public string SessionId { get; set; } = "";
    public List<string> Packets { get; set; } = new();
    public DrainMessage() { Type = ClusterMessageType.Drain; }
}

public sealed class PacketMessage : ClusterEngineMessage
{
    public string SessionId { get; set; } = "";
    public string Packet { get; set; } = "";
    public PacketMessage() { Type = ClusterMessageType.Packet; }
}

public sealed class CloseMessage : ClusterEngineMessage
{
    public string SessionId { get; set; } = "";
    public CloseMessage() { Type = ClusterMessageType.Close; }
}
