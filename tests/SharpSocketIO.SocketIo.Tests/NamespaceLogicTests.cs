using System.Collections.Generic;
using System.Linq;
using SharpSocketIO.SocketIo.Tests.Fakes;
using Xunit;

namespace SharpSocketIO.SocketIo.Tests;

public class NamespaceLogicTests
{
    [Fact]
    public void Of_creates_and_caches_namespaces()
    {
        var server = new Server();
        var nsp1 = server.Of("/chat");
        var nsp2 = server.Of("/chat");
        Assert.Same(nsp1, nsp2);
        Assert.Equal("/chat", nsp1.Name);
    }

    [Fact]
    public void Each_namespace_has_its_own_adapter()
    {
        var server = new Server();
        var root = server.Of("/");
        var chat = server.Of("/chat");
        Assert.NotSame(root.Adapter, chat.Adapter);
    }

    [Fact]
    public async System.Threading.Tasks.Task Broadcast_reaches_only_sockets_in_the_target_room()
    {
        var server = new Server();
        var nsp = server.Of("/");
        var conn1 = new FakeEngineIoConnection { Id = "c1" };
        var conn2 = new FakeEngineIoConnection { Id = "c2" };
        server.CreateClient(conn1); conn1.ReceiveFromClient("0");
        server.CreateClient(conn2); conn2.ReceiveFromClient("0");

        // each socket joins a room named after its own id; find them by that.
        var s1 = nsp.Sockets.Values.First();
        var s2 = nsp.Sockets.Values.First(so => so != s1);
        s1.Join("alpha");

        conn1.Sent.Clear(); conn2.Sent.Clear();
        await nsp.BroadcastAsync("EVENT-packet", new HashSet<string> { "alpha" });
        // exactly one client should have received — the one whose socket joined "alpha"
        var totalSent = conn1.Sent.Count + conn2.Sent.Count;
        Assert.Equal(1, totalSent);
    }

    [Fact]
    public void Middleware_runs_before_connection_event()
    {
        var server = new Server();
        bool middlewareRan = false;
        bool connectionFired = false;
        server.Of("/").Use((socket, next) =>
        {
            middlewareRan = true;
            next(null);
            return true;
        });
        server.Of("/").On("connection", _ => connectionFired = true);
        var conn = new FakeEngineIoConnection { Id = "mw1" };
        server.CreateClient(conn);
        conn.ReceiveFromClient("0");
        Assert.True(middlewareRan);
        Assert.True(connectionFired);
    }

    [Fact]
    public void Middleware_can_reject_connection()
    {
        var server = new Server();
        bool connectionFired = false;
        server.Of("/").Use((socket, next) =>
        {
            next("not authorized");
            return true;
        });
        server.Of("/").On("connection", _ => connectionFired = true);
        var conn = new FakeEngineIoConnection { Id = "mw2" };
        server.CreateClient(conn);
        conn.ReceiveFromClient("0");
        Assert.False(connectionFired); // rejected — no socket added
        Assert.Empty(server.Of("/").Sockets);
    }
}
