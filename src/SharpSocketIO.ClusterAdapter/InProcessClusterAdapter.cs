using System.Threading.Tasks;
using SharpSocketIO.SocketIo.Adapter;

namespace SharpSocketIO.ClusterAdapter;

/// <summary>
/// In-process cluster adapter. Registers with an InProcessCluster; DoPublish fans
/// the message out to peer adapters in the same process. This is the .NET analogue
/// of the upstream NodeClusterAdapter (which uses Node's cluster IPC).
/// </summary>
public sealed class InProcessClusterAdapter : ClusterAdapter
{
    private readonly InProcessCluster _cluster;

    public InProcessClusterAdapter(IAdapterNamespace nsp, InProcessCluster cluster, ClusterAdapterOptions? opts = null)
        : base(nsp, opts)
    {
        _cluster = cluster;
        _cluster.Register(this);
    }

    public void SetNspName(string name) => SetNamespace(name);

    protected override Task DoPublish(ClusterMessage message)
    {
        _cluster.Publish(message);
        return Task.CompletedTask;
    }

    protected override Task DoPublishResponse(string requesterUid, ClusterResponse response)
    {
        _cluster.PublishResponse(requesterUid, response);
        return Task.CompletedTask;
    }
}
