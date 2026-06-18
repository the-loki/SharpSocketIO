using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using Xunit;

namespace SharpSocketIO.PostgresEmitter.Tests;

internal sealed class InMemoryPublisher : IPostgresPublisher
{
    public List<(string channel, string payload)> Published { get; } = new();
    public void Publish(string channel, string payload) => Published.Add((channel, payload));
}

public class EmitterTests
{
    private static BroadcastDocument? ParseDoc(string payload) =>
        JsonSerializer.Deserialize<BroadcastDocument>(payload, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

    [Fact]
    public void Emit_publishes_a_broadcast_document_to_default_channel()
    {
        var pub = new InMemoryPublisher();
        var emitter = new Emitter(pub);
        emitter.Emit("hello", "world");
        Assert.Single(pub.Published);
        Assert.Equal("socket.io#/", pub.Published[0].channel);
        var doc = ParseDoc(pub.Published[0].payload);
        Assert.NotNull(doc);
        Assert.Equal(EventType.Broadcast, doc!.Type);
        Assert.Equal("/", doc.Nsp);
    }

    [Fact]
    public void Of_targets_a_different_namespace()
    {
        var pub = new InMemoryPublisher();
        var emitter = new Emitter(pub).Of("/chat");
        emitter.Emit("msg");
        Assert.Equal("socket.io#/chat", pub.Published[0].channel);
        var doc = ParseDoc(pub.Published[0].payload);
        Assert.Equal("/chat", doc!.Nsp);
    }

    [Fact]
    public void To_room_includes_room_in_document()
    {
        var pub = new InMemoryPublisher();
        var emitter = new Emitter(pub);
        emitter.To("room-101").Emit("event");
        var doc = ParseDoc(pub.Published[0].payload);
        Assert.Contains("room-101", doc!.Data.Rooms);
    }

    [Fact]
    public void Except_room_includes_except_in_document()
    {
        var pub = new InMemoryPublisher();
        var emitter = new Emitter(pub);
        emitter.Except("banned").Emit("event");
        var doc = ParseDoc(pub.Published[0].payload);
        Assert.Contains("banned", doc!.Data.Except);
    }

    [Fact]
    public void Chained_To_targets_union()
    {
        var pub = new InMemoryPublisher();
        var emitter = new Emitter(pub);
        emitter.To("a").To("b").Emit("event");
        var doc = ParseDoc(pub.Published[0].payload);
        Assert.Contains("a", doc!.Data.Rooms);
        Assert.Contains("b", doc.Data.Rooms);
    }

    [Fact]
    public void Volatile_sets_flag()
    {
        var pub = new InMemoryPublisher();
        var emitter = new Emitter(pub);
        emitter.Volatile().Emit("event");
        var doc = ParseDoc(pub.Published[0].payload);
        Assert.True(doc!.Data.Flags?.Volatile);
    }

    [Fact]
    public void Compress_sets_flag()
    {
        var pub = new InMemoryPublisher();
        var emitter = new Emitter(pub);
        emitter.Compress(false).Emit("event");
        var doc = ParseDoc(pub.Published[0].payload);
        Assert.False(doc!.Data.Flags?.Compress);
    }

    [Fact]
    public void Document_uid_is_emitter()
    {
        var pub = new InMemoryPublisher();
        var emitter = new Emitter(pub);
        emitter.Emit("event");
        var doc = ParseDoc(pub.Published[0].payload);
        Assert.Equal("emitter", doc!.Uid);
    }

    [Fact]
    public void Custom_channel_prefix()
    {
        var pub = new InMemoryPublisher();
        var emitter = new Emitter(pub, "/", new PostgresEmitterOptions { ChannelPrefix = "myio" });
        emitter.Emit("event");
        Assert.Equal("myio#/", pub.Published[0].channel);
    }
}
