using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using SharpSocketIO.SocketIo.Adapter;
using Xunit;

namespace SharpSocketIO.ClusterAdapter.Tests;

internal sealed class FakeNsp : IAdapterNamespace
{
    public List<(string sid, string packet)> Sent { get; } = new();
    public void Send(string socketId, string packet) => Sent.Add((socketId, packet));
    public void SendParts(string socketId, System.Collections.Generic.IReadOnlyList<object> parts) { }
}

public class ClusterAdapterTests
{
    private static InProcessClusterAdapter MakeAdapter(InProcessCluster cluster, FakeNsp nsp, string nspName = "/")
    {
        var a = new InProcessClusterAdapter(nsp, cluster);
        a.SetNspName(nspName);
        return a;
    }

    [Fact]
    public async Task Broadcast_reaches_other_instances_local_sockets()
    {
        var cluster = new InProcessCluster();
        var nspA = new FakeNsp();
        var nspB = new FakeNsp();
        var a = MakeAdapter(cluster, nspA);
        var b = MakeAdapter(cluster, nspB);

        // socket "s1" on instance B in room "alpha"
        b.Add("s1", "alpha");

        // instance A broadcasts to room "alpha"
        await a.BroadcastAsync("hello-packet", new BroadcastOptions { Rooms = new HashSet<string> { "alpha" } });

        // the packet should reach s1 (on B)
        Assert.Contains(("s1", "hello-packet"), nspB.Sent);
        // and NOT reach any socket on A (none joined anyway)
        Assert.Empty(nspA.Sent);
    }

    [Fact]
    public async Task Broadcast_to_all_when_no_rooms()
    {
        var cluster = new InProcessCluster();
        var nspA = new FakeNsp();
        var nspB = new FakeNsp();
        var a = MakeAdapter(cluster, nspA);
        var b = MakeAdapter(cluster, nspB);
        b.AddAll("s1", new HashSet<string>()); b.AddAll("s2", new HashSet<string>());

        await a.BroadcastAsync("broadcast-all", new BroadcastOptions());

        Assert.Equal(2, nspB.Sent.Count);
    }

    [Fact]
    public async Task Sockets_join_propagates_across_instances()
    {
        var cluster = new InProcessCluster();
        var nspA = new FakeNsp();
        var nspB = new FakeNsp();
        var a = MakeAdapter(cluster, nspA);
        var b = MakeAdapter(cluster, nspB);

        // socket "s1" exists on instance B
        b.AddAll("s1", new HashSet<string>());

        // instance A tells all sockets matching (all) to join "new-room"
        await a.DoPublishForTestImpl(new SocketsJoinMessage
        {
            Uid = a.Uid,
            Nsp = "/",
            Opts = new ClusterMessageOpts(),
            Rooms = new[] { "new-room" },
        });

        Assert.True(b.HasRoom("new-room"));
        Assert.True(b.HasSocket("s1"));
    }

    [Fact]
    public async Task Sockets_leave_propagates_across_instances()
    {
        var cluster = new InProcessCluster();
        var nspA = new FakeNsp();
        var nspB = new FakeNsp();
        var a = MakeAdapter(cluster, nspA);
        var b = MakeAdapter(cluster, nspB);
        b.Add("s1", "room1");

        await a.DoPublishForTestImpl(new SocketsLeaveMessage
        {
            Uid = a.Uid,
            Nsp = "/",
            Opts = new ClusterMessageOpts(),
            Rooms = new[] { "room1" },
        });

        Assert.False(b.HasRoom("room1"));
    }

    [Fact]
    public async Task FetchSockets_aggregates_from_all_instances()
    {
        var cluster = new InProcessCluster();
        var nspA = new FakeNsp();
        var nspB = new FakeNsp();
        var a = MakeAdapter(cluster, nspA);
        var b = MakeAdapter(cluster, nspB);
        a.AddAll("local-s1", new HashSet<string>());
        b.AddAll("remote-s1", new HashSet<string>());

        // fetch from A; expect 1 peer response (from B), plus A's own local socket
        var sockets = await a.FetchSocketsAsync(new BroadcastOptions(), expectedResponses: 1, timeoutMs: 2000);
        var ids = sockets.Select(s => s.Id).ToHashSet();
        Assert.Contains("local-s1", ids);
        Assert.Contains("remote-s1", ids);
    }

    [Fact]
    public void Cluster_peer_count_tracks_registrations()
    {
        var cluster = new InProcessCluster();
        Assert.Equal(0, cluster.PeerCount);
        var a = MakeAdapter(cluster, new FakeNsp());
        Assert.Equal(1, cluster.PeerCount);
        var b = MakeAdapter(cluster, new FakeNsp());
        Assert.Equal(2, cluster.PeerCount);
        cluster.Unregister(a.Uid);
        Assert.Equal(1, cluster.PeerCount);
    }
}

