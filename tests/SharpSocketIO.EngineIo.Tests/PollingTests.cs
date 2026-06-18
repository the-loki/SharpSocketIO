using System.Collections.Generic;
using System.Text;
using SharpSocketIO.EngineIo.Commons;
using SharpSocketIO.EngineIo.Parser.Commons;
using SharpSocketIO.EngineIo.Transports;
using Xunit;
using PacketType = SharpSocketIO.EngineIo.Parser.Commons.PacketType;

namespace SharpSocketIO.EngineIo.Tests;

public class PollingTests
{
    private sealed class Req : IEngineRequest
    {
        public string Method { get; set; } = "GET";
        public string Path => "/engine.io/";
        public IReadOnlyDictionary<string, string> Query { get; set; } = new Dictionary<string, string> { ["EIO"] = "4" };
        public IReadOnlyDictionary<string, string> Headers { get; } = new Dictionary<string, string>();
        public string? RemoteAddress => "127.0.0.1";
        public byte[]? Body { get; set; }
    }

    [Fact]
    public void Ingests_a_posted_payload_and_emits_packets()
    {
        var t = new Polling(new Req());
        var received = new List<Packet>();
        t.On("packet", args => received.Add((Packet)args[0]));

        // payload: two message packets joined by \x1e
        var payload = "4hello" + (char)30 + "4world";
        t.OnDataRequest(new Req { Method = "POST", Body = Encoding.UTF8.GetBytes(payload) });

        Assert.Equal(2, received.Count);
        Assert.Equal(PacketType.Message, received[0].Type);
        Assert.Equal("hello", received[0].Data!.Value.Value!.ToString());
        Assert.Equal("world", received[1].Data!.Value.Value!.ToString());
    }

    [Fact]
    public void Flush_joins_queued_packets_with_separator()
    {
        var t = new Polling(new Req());
        t.Enqueue(new Packet(PacketType.Message, new RawData("a")));
        t.Enqueue(new Packet(PacketType.Message, new RawData("b")));
        var flushed = t.FlushToString();
        Assert.Equal("4a" + (char)30 + "4b", flushed);
    }

    [Fact]
    public void Flush_empty_returns_empty_string()
    {
        var t = new Polling(new Req());
        Assert.Equal(string.Empty, t.FlushToString());
    }

    [Fact]
    public void Name_is_polling()
    {
        var t = new Polling(new Req());
        Assert.Equal("polling", t.Name);
    }
}
