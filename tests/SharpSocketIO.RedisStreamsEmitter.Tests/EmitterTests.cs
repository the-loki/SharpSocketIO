using System.Collections.Generic;
using System.Text.Json;
using Xunit;

namespace SharpSocketIO.RedisStreamsEmitter.Tests;

internal sealed class InMemoryPublisher : IRedisPublisher
{
    public List<(string streamKey, string payload)> Published { get; } = new();
    public void Publish(string streamKey, string payload) => Published.Add((streamKey, payload));
}

public class EmitterTests
{
    private static RedisBroadcastDocument? ParseDoc(string payload) =>
        JsonSerializer.Deserialize<RedisBroadcastDocument>(payload, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

    [Fact]
    public void Emit_publishes_a_broadcast_document()
    {
        var pub = new InMemoryPublisher();
        var emitter = new RedisEmitter(pub);
        emitter.Emit("hello", "world");
        Assert.Single(pub.Published);
        Assert.Equal("socket.io", pub.Published[0].streamKey);
        var doc = ParseDoc(pub.Published[0].payload);
        Assert.Equal(RedisMessageType.Broadcast, doc!.Type);
        Assert.Equal("/", doc.Nsp);
    }

    [Fact]
    public void Of_targets_a_different_namespace()
    {
        var pub = new InMemoryPublisher();
        var emitter = new RedisEmitter(pub).Of("/chat");
        emitter.Emit("msg");
        var doc = ParseDoc(pub.Published[0].payload);
        Assert.Equal("/chat", doc!.Nsp);
    }

    [Fact]
    public void To_room_includes_room_in_document()
    {
        var pub = new InMemoryPublisher();
        new RedisEmitter(pub).To("room-101").Emit("event");
        var doc = ParseDoc(pub.Published[0].payload);
        Assert.Contains("room-101", doc!.Data.Rooms);
    }

    [Fact]
    public void Except_room_includes_except_in_document()
    {
        var pub = new InMemoryPublisher();
        new RedisEmitter(pub).Except("banned").Emit("event");
        var doc = ParseDoc(pub.Published[0].payload);
        Assert.Contains("banned", doc!.Data.Except);
    }

    [Fact]
    public void Chained_To_targets_union()
    {
        var pub = new InMemoryPublisher();
        new RedisEmitter(pub).To("a").To("b").Emit("event");
        var doc = ParseDoc(pub.Published[0].payload);
        Assert.Contains("a", doc!.Data.Rooms);
        Assert.Contains("b", doc.Data.Rooms);
    }

    [Fact]
    public void Volatile_sets_flag()
    {
        var pub = new InMemoryPublisher();
        new RedisEmitter(pub).Volatile().Emit("event");
        Assert.True(ParseDoc(pub.Published[0].payload)!.Data.Flags?.Volatile);
    }

    [Fact]
    public void Compress_sets_flag()
    {
        var pub = new InMemoryPublisher();
        new RedisEmitter(pub).Compress(false).Emit("event");
        Assert.False(ParseDoc(pub.Published[0].payload)!.Data.Flags?.Compress);
    }

    [Fact]
    public void Document_uid_is_emitter()
    {
        var pub = new InMemoryPublisher();
        new RedisEmitter(pub).Emit("event");
        Assert.Equal("emitter", ParseDoc(pub.Published[0].payload)!.Uid);
    }

    [Fact]
    public void Custom_stream_key()
    {
        var pub = new InMemoryPublisher();
        new RedisEmitter(pub, "/", new RedisStreamsEmitterOptions { StreamKey = "myio" }).Emit("event");
        Assert.Equal("myio", pub.Published[0].streamKey);
    }
}
