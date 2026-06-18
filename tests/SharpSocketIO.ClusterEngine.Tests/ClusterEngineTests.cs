using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace SharpSocketIO.ClusterEngine.Tests;

public class ClusterEngineTests
{
    [Fact]
    public async Task AcquireLock_succeeds_when_no_peer_owns_session()
    {
        var hub = new InProcessClusterEngineHub();
        var engine = new InProcessClusterEngine(hub);
        var acquired = await engine.AcquireLockAsync("session-1", timeoutMs: 500);
        Assert.True(acquired);
        Assert.True(engine.OwnsSession("session-1"));
    }

    [Fact]
    public async Task AcquireLock_fails_when_another_instance_owns_session()
    {
        var hub = new InProcessClusterEngineHub();
        var engineA = new InProcessClusterEngine(hub);
        var engineB = new InProcessClusterEngine(hub);
        await engineA.AcquireLockAsync("session-1", timeoutMs: 500);
        var bAcquired = await engineB.AcquireLockAsync("session-1", timeoutMs: 2000);
        Assert.False(bAcquired);
        Assert.False(engineB.OwnsSession("session-1"));
    }

    [Fact]
    public async Task ReleaseAndDrain_transfers_ownership_and_buffers()
    {
        var hub = new InProcessClusterEngineHub();
        var engineA = new InProcessClusterEngine(hub);
        var engineB = new InProcessClusterEngine(hub);
        await engineA.AcquireLockAsync("session-1", timeoutMs: 500);
        engineA.BufferPacket("session-1", "packet-1");
        engineA.BufferPacket("session-1", "packet-2");

        var drainedPackets = new List<string>();
        engineB.On("drained-packet", args =>
        {
            if (args.Length >= 2 && (string)args[0] == "session-1")
                drainedPackets.Add((string)args[1]);
        });

        engineA.ReleaseAndDrain("session-1", engineB.NodeId);
        // engineB should receive the drained packets (after it would re-acquire the lock)
        await engineB.AcquireLockAsync("session-1", timeoutMs: 2000);
        await Task.Delay(200);
        Assert.Equal(new[] { "packet-1", "packet-2" }, drainedPackets);
    }

    [Fact]
    public async Task Packet_routes_to_owning_instance()
    {
        var hub = new InProcessClusterEngineHub();
        var engineA = new InProcessClusterEngine(hub);
        var engineB = new InProcessClusterEngine(hub);
        await engineA.AcquireLockAsync("session-1", timeoutMs: 500);

        var receivedPackets = new List<string>();
        engineA.On("packet", args =>
        {
            if (args[0] is PacketMessage pm && pm.SessionId == "session-1")
                receivedPackets.Add(pm.Packet);
        });

        // engineB routes a packet for session-1 (owned by A)
        engineB.RoutePacket("session-1", "routed-packet");
        await Task.Delay(200);
        Assert.Contains("routed-packet", receivedPackets);
    }

    [Fact]
    public async Task Close_propagates_to_peers()
    {
        var hub = new InProcessClusterEngineHub();
        var engineA = new InProcessClusterEngine(hub);
        var engineB = new InProcessClusterEngine(hub);
        await engineA.AcquireLockAsync("session-1", timeoutMs: 500);

        var closed = new List<string>();
        engineB.On("remote-close", args => closed.Add((string)args[0]));

        engineA.CloseSession("session-1");
        await Task.Delay(200);
        Assert.Contains("session-1", closed);
        Assert.False(engineA.OwnsSession("session-1"));
    }

    [Fact]
    public void Hub_tracks_registered_nodes()
    {
        var hub = new InProcessClusterEngineHub();
        Assert.Equal(0, hub.NodeCount);
        var a = new InProcessClusterEngine(hub);
        Assert.Equal(1, hub.NodeCount);
        var b = new InProcessClusterEngine(hub);
        Assert.Equal(2, hub.NodeCount);
        hub.Unregister(a.NodeId);
        Assert.Equal(1, hub.NodeCount);
    }
}
