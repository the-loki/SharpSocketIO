using System.Linq;
using SharpSocketIO.SocketIo.Tests.Fakes;
using Xunit;

namespace SharpSocketIO.SocketIo.Tests;

public class SocketLogicTests
{
    private static (Server server, FakeEngineIoConnection conn, Client client) Connect(string id = "c1")
    {
        var server = new Server();
        var conn = new FakeEngineIoConnection { Id = id };
        var client = server.CreateClient(conn);
        conn.ReceiveFromClient("0"); // CONNECT packet (empty auth) to "/"
        return (server, conn, client);
    }

    [Fact]
    public void Connect_creates_a_socket_in_the_default_namespace()
    {
        var (server, conn, _) = Connect();
        Assert.Single(server.Of("/").Sockets);
        Assert.NotEmpty(conn.Sent); // server replied with a CONNECT ack
    }

    [Fact]
    public void Server_emits_connection_event_with_socket()
    {
        var server = new Server();
        var conn = new FakeEngineIoConnection { Id = "c2" };
        Socket? got = null;
        server.Of("/").On("connection", args => got = (Socket)args[0]);
        server.CreateClient(conn);
        conn.ReceiveFromClient("0");
        Assert.NotNull(got);
        Assert.True(got!.Connected);
    }

    [Fact]
    public void Socket_can_join_and_leave_rooms()
    {
        var (server, _, _) = Connect();
        var socket = server.Of("/").Sockets.Values.First();
        socket.Join("room1");
        Assert.True(server.Of("/").Adapter.HasRoom("room1"));
        socket.Leave("room1");
        Assert.False(server.Of("/").Adapter.HasRoom("room1"));
    }

    [Fact]
    public void Server_emit_sends_a_packet_to_the_client()
    {
        var (server, conn, _) = Connect();
        conn.Sent.Clear();
        var socket = server.Of("/").Sockets.Values.First();
        socket.Emit("greeting", "hello");
        Assert.NotEmpty(conn.Sent); // an EVENT packet was sent down the engine.io conn
    }

    [Fact]
    public void Socket_receives_event_from_client_and_invokes_handler()
    {
        var (server, conn, _) = Connect();
        var socket = server.Of("/").Sockets.Values.First();
        string? gotEvent = null;
        object[]? gotArgs = null;
        socket.On("chat", args =>
        {
            gotEvent = "chat";
            gotArgs = args;
        });
        // client emits an event: encode an EVENT packet "2[\"chat\",\"hi\"]"
        conn.ReceiveFromClient("2[\"chat\",\"hi\"]");
        Assert.Equal("chat", gotEvent);
        Assert.NotNull(gotArgs);
        Assert.Equal("hi", gotArgs![0]);
    }

    [Fact]
    public void Socket_ack_round_trips()
    {
        var (server, conn, _) = Connect();
        var socket = server.Of("/").Sockets.Values.First();
        bool? gotAck = null;
        // server emits with ack
        socket.EmitWithAck("ping", data => gotAck = true, "are-you-there");
        conn.Sent.Clear();
        // client replies with ACK — the id was 0 (first ack)
        conn.ReceiveFromClient("30[]"); // ACK packet id=0, empty data
        Assert.True(gotAck ?? false);
    }

    [Fact]
    public void Disconnect_removes_socket_from_namespace()
    {
        var (server, _, _) = Connect();
        var socket = server.Of("/").Sockets.Values.First();
        Assert.True(socket.Connected);
        socket.Disconnect();
        Assert.False(socket.Connected);
        Assert.DoesNotContain(socket.Id, server.Of("/").Sockets.Keys);
    }
}
