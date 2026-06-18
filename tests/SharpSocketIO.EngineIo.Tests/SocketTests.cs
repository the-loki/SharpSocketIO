using System.Collections.Generic;
using System.Linq;
using SharpSocketIO.EngineIo.Commons;
using SharpSocketIO.EngineIo.Parser.Commons;
using SharpSocketIO.EngineIo.Transports;
using Xunit;
using PacketType = SharpSocketIO.EngineIo.Parser.Commons.PacketType;

namespace SharpSocketIO.EngineIo.Tests;

public class SocketTests
{
    private sealed class FakeReq : IEngineRequest
    {
        public string Method => "GET";
        public string Path => "/engine.io/";
        public IReadOnlyDictionary<string, string> Query => new Dictionary<string, string> { ["EIO"] = "4" };
        public IReadOnlyDictionary<string, string> Headers => new Dictionary<string, string>();
        public string? RemoteAddress => "127.0.0.1";
        public byte[]? Body => null;
    }

    private sealed class FakeTransport : Transport
    {
        public List<Packet> Sent { get; } = new();
        public bool Closed { get; private set; }
        public FakeTransport() : base(new FakeReq()) { }
        public override string Name => "polling";
        public override void Send(IReadOnlyList<Packet> packets) { Sent.AddRange(packets); Writable = true; }
        public override void DoClose(System.Action? callback = null) { Closed = true; callback?.Invoke(); }
    }

    [Fact]
    public void Open_sends_open_handshake_packet()
    {
        var transport = new FakeTransport();
        var socket = new Socket("sid-1", server: null!, transport, protocol: 4);
        socket.OnOpen();
        Assert.Equal(ReadyState.Open, socket.ReadyState);
        Assert.Equal(PacketType.Open, transport.Sent[0].Type);
    }

    [Fact]
    public void Buffers_and_flushes_a_message_packet()
    {
        var transport = new FakeTransport();
        var socket = new Socket("sid-2", null!, transport, 4);
        socket.OnOpen();
        transport.Sent.Clear();

        var msg = new Packet(PacketType.Message, new RawData("hello"));
        socket.Send(new[] { msg });
        Assert.Contains(msg, transport.Sent);
    }

    [Fact]
    public void OnPacket_ping_replies_pong()
    {
        var transport = new FakeTransport();
        var socket = new Socket("sid-3", null!, transport, 4);
        socket.OnOpen();
        transport.Sent.Clear();

        socket.OnPacket(new Packet(PacketType.Ping, new RawData("probe")));
        Assert.Equal(PacketType.Pong, transport.Sent[0].Type);
    }

    [Fact]
    public void Close_emits_close_after_flushing()
    {
        var transport = new FakeTransport();
        var socket = new Socket("sid-4", null!, transport, 4);
        bool closed = false;
        socket.On("close", _ => closed = true);
        socket.OnOpen();
        socket.Close();
        Assert.True(closed);
        Assert.Equal(ReadyState.Closed, socket.ReadyState);
    }
}
