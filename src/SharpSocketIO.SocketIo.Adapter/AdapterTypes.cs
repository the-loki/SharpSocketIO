using System.Collections.Generic;

namespace SharpSocketIO.SocketIo.Adapter;

/// <summary>Flags modifying a broadcast.</summary>
public sealed class BroadcastFlags
{
    public bool Volatile { get; set; }
    public bool Compress { get; set; }
    public bool Local { get; set; }
    public bool Broadcast { get; set; }
    public bool Binary { get; set; }
    public int? Timeout { get; set; }
}

/// <summary>Options for a broadcast.</summary>
public sealed class BroadcastOptions
{
    public ISet<string> Rooms { get; set; } = new HashSet<string>();
    public ISet<string>? Except { get; set; }
    public BroadcastFlags? Flags { get; set; }
}

/// <summary>The contract a Namespace must satisfy so the Adapter can push packets.</summary>
public interface IAdapterNamespace
{
    void Send(string socketId, string packet);
}
