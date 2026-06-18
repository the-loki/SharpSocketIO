using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using SharpSocketIO.SocketIo.Tests.Fakes;
using Xunit;

namespace SharpSocketIO.SocketIo.Tests;

public class BroadcastOperatorTests
{
    // Connects N sockets, returns the namespace + each socket's underlying connection
    // paired by socket (stable, not by enumeration order).
    private static (Server server, Namespace nsp, Dictionary<string, FakeEngineIoConnection> bySocketId, List<Socket> sockets) Setup(int socketCount)
    {
        var server = new Server();
        var nsp = server.Of("/");
        var bySocketId = new Dictionary<string, FakeEngineIoConnection>();
        for (int i = 0; i < socketCount; i++)
        {
            var conn = new FakeEngineIoConnection { Id = "c" + i };
            server.CreateClient(conn);
            conn.ReceiveFromClient("0"); // CONNECT
        }
        // map each socket to its connection
        foreach (var s in nsp.Sockets.Values)
        {
            // find which conn this socket's client owns by matching sent identity
            // simpler: a socket's Client.Conn.Id matches a connection Id
            bySocketId[s.Id] = null!;
        }
        return (server, nsp, bySocketId, nsp.Sockets.Values.ToList());
    }

    private static FakeEngineIoConnection? ConnFor(Socket s, List<FakeEngineIoConnection> conns)
    {
        // match by checking which connection the socket's client uses (Client.Conn.Id)
        return conns.FirstOrDefault(c => c.Id == s.Client.Conn.Id);
    }

    private static (Server server, Namespace nsp, List<FakeEngineIoConnection> conns) SetupConns(int n)
    {
        var server = new Server();
        var nsp = server.Of("/");
        var conns = new List<FakeEngineIoConnection>();
        for (int i = 0; i < n; i++)
        {
            var conn = new FakeEngineIoConnection { Id = "c" + i };
            server.CreateClient(conn);
            conn.ReceiveFromClient("0");
            conns.Add(conn);
        }
        return (server, nsp, conns);
    }

    [Fact]
    public async Task Namespace_To_room_emit_reaches_only_that_room()
    {
        var (server, nsp, conns) = SetupConns(3);
        var sockets = nsp.Sockets.Values.ToList();
        sockets[0].Join("alpha");
        sockets[1].Join("alpha");
        sockets[2].Join("beta");
        foreach (var c in conns) c.Sent.Clear();

        await nsp.To("alpha").EmitAsync("msg", "hi");

        // total: exactly 2 (the two in alpha)
        Assert.Equal(2, conns.Count(c => c.Sent.Count > 0));
    }

    [Fact]
    public async Task Namespace_emit_reaches_all_sockets()
    {
        var (server, nsp, conns) = SetupConns(3);
        foreach (var c in conns) c.Sent.Clear();
        await nsp.EmitAsync("broadcast", "all");
        Assert.True(conns.All(c => c.Sent.Count > 0));
    }

    [Fact]
    public async Task Except_excludes_targeted_room_members()
    {
        var (server, nsp, conns) = SetupConns(3);
        var sockets = nsp.Sockets.Values.ToList();
        sockets[0].Join("alpha");
        sockets[1].Join("alpha");
        sockets[1].Join("excluded");
        foreach (var c in conns) c.Sent.Clear();

        await nsp.To("alpha").Except("excluded").EmitAsync("msg");

        // exactly 1 received (alpha minus excluded)
        Assert.Equal(1, conns.Count(c => c.Sent.Count > 0));
    }

    [Fact]
    public async Task Chained_To_targets_union_of_rooms()
    {
        var (server, nsp, conns) = SetupConns(3);
        var sockets = nsp.Sockets.Values.ToList();
        sockets[0].Join("a");
        sockets[1].Join("b");
        sockets[2].Join("c");
        foreach (var c in conns) c.Sent.Clear();

        await nsp.To("a").To("b").EmitAsync("msg");

        // exactly 2 (a + b)
        Assert.Equal(2, conns.Count(c => c.Sent.Count > 0));
    }

    [Fact]
    public void ParentNamespace_fans_out_emit_to_children()
    {
        var server = new Server();
        var parent = new ParentNamespace(server);
        var childA = server.Of("/dyn-a");
        var childB = server.Of("/dyn-b");
        parent.AddChild(childA);
        parent.AddChild(childB);

        var connA = new FakeEngineIoConnection { Id = "pa" };
        server.CreateClient(connA);
        connA.ReceiveFromClient("0/dyn-a");
        var connB = new FakeEngineIoConnection { Id = "pb" };
        server.CreateClient(connB);
        connB.ReceiveFromClient("0/dyn-b");

        connA.Sent.Clear(); connB.Sent.Clear();
        parent.Emit("parent-msg", "hi");

        Assert.NotEmpty(connA.Sent);
        Assert.NotEmpty(connB.Sent);
    }
}
