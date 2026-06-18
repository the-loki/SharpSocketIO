using System.Collections.Generic;
using SharpSocketIO.EngineIo.Commons;
using SharpSocketIO.EngineIo.Parser.Commons;
using SharpSocketIO.EngineIo.Transports;
using Xunit;

namespace SharpSocketIO.EngineIo.Tests;

public class TransportTests
{
    private sealed class FakeRequest : IEngineRequest
    {
        public string Method => "GET";
        public string Path => "/engine.io/";
        public IReadOnlyDictionary<string, string> Query { get; set; } = new Dictionary<string, string>();
        public IReadOnlyDictionary<string, string> Headers { get; } = new Dictionary<string, string>();
        public string? RemoteAddress => "127.0.0.1";
        public byte[]? Body => null;
    }

    private sealed class FakeTransport : Transport
    {
        public FakeTransport(IEngineRequest req) : base(req) { }
        public override string Name => "fake";
        public override void Send(IReadOnlyList<Packet> packets) { }
        public override void DoClose(System.Action? fn = null) => fn?.Invoke();
    }

    [Fact]
    public void Selects_v4_parser_when_EIO_is_4()
    {
        var req = new FakeRequest { Query = new Dictionary<string, string> { ["EIO"] = "4" } };
        var t = new FakeTransport(req);
        Assert.Equal(4, t.Protocol);
        Assert.True(t.SupportsBinary); // default true (no b64)
    }

    [Fact]
    public void Selects_v3_parser_when_EIO_is_not_4()
    {
        var req = new FakeRequest { Query = new Dictionary<string, string> { ["EIO"] = "3" } };
        var t = new FakeTransport(req);
        Assert.Equal(3, t.Protocol);
    }

    [Fact]
    public void SupportsBinary_false_when_b64_query_set()
    {
        var req = new FakeRequest { Query = new Dictionary<string, string> { ["EIO"] = "4", ["b64"] = "1" } };
        var t = new FakeTransport(req);
        Assert.False(t.SupportsBinary);
    }

    [Fact]
    public void Close_transitions_readyState_and_invokes_doClose()
    {
        var t = new FakeTransport(new FakeRequest());
        Assert.Equal(ReadyState.Open, t.ReadyState);
        bool closed = false;
        t.Close(() => closed = true);
        Assert.Equal(ReadyState.Closing, t.ReadyState);
        Assert.True(closed);
    }
}
