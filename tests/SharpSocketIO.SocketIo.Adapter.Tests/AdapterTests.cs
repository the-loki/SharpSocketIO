using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using SharpSocketIO.SocketIo.Adapter;
using Xunit;

namespace SharpSocketIO.SocketIo.Adapter.Tests;

internal sealed class FakeNamespace : IAdapterNamespace
{
    public List<(string socketId, string packet)> Sent { get; } = new();
    public void Send(string socketId, string packet) => Sent.Add((socketId, packet));
}

public class AdapterTests
{
    private static Adapter NewAdapter()
    {
        var nsp = new FakeNamespace();
        return new Adapter(nsp);
    }

    [Fact]
    public void AddAll_adds_socket_to_rooms_and_emits_create_join()
    {
        var adapter = NewAdapter();
        var created = new List<string>();
        var joined = new List<(string room, string sid)>();
        adapter.On("create-room", args => created.Add((string)args[0]));
        adapter.On("join-room", args => joined.Add(((string)args[0], (string)args[1])));

        adapter.AddAll("s1", new HashSet<string> { "room1", "room2" });

        Assert.Equal(new[] { "room1", "room2" }, created);
        Assert.Equal(new[] { ("room1", "s1"), ("room2", "s1") }, joined.ToArray());
        Assert.True(adapter.HasRoom("room1"));
        Assert.True(adapter.HasSocket("s1"));
    }

    [Fact]
    public void Del_removes_socket_from_room_and_emits_leave_and_delete_when_empty()
    {
        var adapter = NewAdapter();
        adapter.AddAll("s1", new HashSet<string> { "room1" });
        var left = new List<(string room, string sid)>();
        var deleted = new List<string>();
        adapter.On("leave-room", args => left.Add(((string)args[0], (string)args[1])));
        adapter.On("delete-room", args => deleted.Add((string)args[0]));

        adapter.Del("s1", "room1");

        Assert.Equal(new[] { ("room1", "s1") }, left.ToArray());
        Assert.Equal(new[] { "room1" }, deleted);
        Assert.False(adapter.HasRoom("room1"));
    }

    [Fact]
    public void DelAll_removes_socket_from_all_rooms()
    {
        var adapter = NewAdapter();
        adapter.AddAll("s1", new HashSet<string> { "room1", "room2" });
        adapter.AddAll("s2", new HashSet<string> { "room1" });

        adapter.DelAll("s1");

        Assert.False(adapter.HasSocket("s1"));
        Assert.True(adapter.HasRoom("room1")); // still has s2
        Assert.False(adapter.HasRoom("room2")); // empty → deleted
    }

    [Fact]
    public async Task SocketsAsync_returns_sockets_in_given_rooms()
    {
        var adapter = NewAdapter();
        adapter.AddAll("s1", new HashSet<string> { "room1" });
        adapter.AddAll("s2", new HashSet<string> { "room1" });
        adapter.AddAll("s3", new HashSet<string> { "room2" });

        var inRoom1 = await adapter.SocketsAsync(new HashSet<string> { "room1" });
        Assert.Equal(new HashSet<string> { "s1", "s2" }, inRoom1);
    }

    [Fact]
    public async Task RoomsAsync_returns_rooms_for_a_socket()
    {
        var adapter = NewAdapter();
        adapter.AddAll("s1", new HashSet<string> { "room1", "room2" });
        var rooms = await adapter.RoomsAsync("s1");
        Assert.Equal(new HashSet<string> { "room1", "room2" }, rooms);
    }

    [Fact]
    public async Task BroadcastAsync_sends_to_matching_sockets_except_excluded()
    {
        var nsp = new FakeNamespace();
        var adapter = new Adapter(nsp);
        adapter.AddAll("s1", new HashSet<string> { "room1" });
        adapter.AddAll("s2", new HashSet<string> { "room1" });
        adapter.AddAll("s3", new HashSet<string> { "room2" });

        await adapter.BroadcastAsync(
            packet: "hello-packet",
            new BroadcastOptions
            {
                Rooms = new HashSet<string> { "room1" },
                Except = new HashSet<string> { "s2" },
            });

        Assert.Single(nsp.Sent);
        Assert.Equal(("s1", "hello-packet"), nsp.Sent[0]);
    }

    [Fact]
    public void ServerCount_is_one_for_in_memory()
    {
        Assert.Equal(1, NewAdapter().ServerCount());
    }

    [Fact]
    public async Task BroadcastAsync_to_all_when_no_rooms()
    {
        var nsp = new FakeNamespace();
        var adapter = new Adapter(nsp);
        adapter.AddAll("s1", new HashSet<string>());
        adapter.AddAll("s2", new HashSet<string>());
        // AddAll with empty set still registers the sid
        Assert.True(adapter.HasSocket("s1"));

        await adapter.BroadcastAsync("p", new BroadcastOptions());
        Assert.Equal(2, nsp.Sent.Count);
    }
}
