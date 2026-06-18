using System.Collections.Concurrent;
using System.Collections.Generic;

namespace SharpSocketIO.ClusterAdapter;

/// <summary>
/// In-process multiplexer — the .NET analogue of Node's cluster IPC. Multiple
/// InProcessClusterAdapter instances register with a shared InProcessCluster;
/// published messages are delivered to all OTHER registered adapters (excluding
/// the publisher, matching namespace).
/// </summary>
public sealed class InProcessCluster
{
    private readonly ConcurrentDictionary<string, InProcessClusterAdapter> _adapters = new();

    public void Register(InProcessClusterAdapter adapter) => _adapters[adapter.Uid] = adapter;

    public void Unregister(string uid) => _adapters.TryRemove(uid, out _);

    /// <summary>Delivers a message to all registered adapters except the publisher (by uid).</summary>
    public void Publish(ClusterMessage message)
    {
        foreach (var kv in _adapters)
        {
            if (kv.Key == message.Uid) continue; // exclude publisher
            kv.Value.OnMessage(message);
        }
    }

    /// <summary>Delivers a response to the specific requesting adapter.</summary>
    public void PublishResponse(string requesterUid, ClusterResponse response)
    {
        if (_adapters.TryGetValue(requesterUid, out var adapter)) adapter.OnResponse(response);
    }

    public int PeerCount => _adapters.Count;
}
