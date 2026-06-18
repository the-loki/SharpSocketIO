using System.Collections.Concurrent;

namespace SharpSocketIO.ClusterEngine;

/// <summary>
/// In-process multiplexer for cluster engines — the .NET analogue of Node's cluster IPC.
/// Multiple InProcessClusterEngine instances register; published messages are delivered
/// to other instances (excluding sender, or to a specific recipient).
/// </summary>
public sealed class InProcessClusterEngineHub
{
    private readonly ConcurrentDictionary<string, InProcessClusterEngine> _engines = new();

    public void Register(InProcessClusterEngine engine) => _engines[engine.NodeId] = engine;
    public void Unregister(string nodeId) => _engines.TryRemove(nodeId, out _);
    public int NodeCount => _engines.Count;

    public void Publish(ClusterEngineMessage message)
    {
        foreach (var kv in _engines)
        {
            if (kv.Key == message.SenderId) continue;
            if (message.RecipientId != null && kv.Key != message.RecipientId) continue;
            kv.Value.OnMessage(message);
        }
    }
}

/// <summary>
/// In-process cluster engine. Registers with a shared hub; publishes to peers in-process.
/// </summary>
public sealed class InProcessClusterEngine : ClusterEngine
{
    private readonly InProcessClusterEngineHub _hub;

    public InProcessClusterEngine(InProcessClusterEngineHub hub)
    {
        _hub = hub;
        _hub.Register(this);
    }

    protected override void PublishMessage(ClusterEngineMessage message) => _hub.Publish(message);
}
