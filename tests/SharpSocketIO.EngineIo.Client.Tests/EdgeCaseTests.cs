using SharpSocketIO.EngineIo.Client.Transports;
using Xunit;

namespace SharpSocketIO.EngineIo.Client.Tests;

public class EdgeCaseTests
{
    [Fact]
    public void Socket_defaults_are_sensible()
    {
        var s = new EngineIoClientSocket();
        Assert.Equal(SocketReadyState.Opening, s.ReadyState);
        Assert.Null(s.Id);
        Assert.Null(s.Transport);
        Assert.False(s.Upgraded);
        Assert.Equal(4, EngineIoClientSocket.Protocol);
    }

    [Fact]
    public void SocketOptions_defaults()
    {
        var o = new SocketOptions();
        Assert.True(o.Upgrade);
        Assert.False(o.ForceBase64);
        Assert.Equal("/engine.io", o.Path);
        Assert.Equal("t", o.TimestampParam);
        Assert.Equal(new[] { "polling", "websocket" }, o.Transports);
    }

    [Fact]
    public void Transport_SupportsBinary_defaults_true_and_false_with_ForceBase64()
    {
        var t1 = new PollingTransport(new SocketOptions { Hostname = "localhost" });
        Assert.True(t1.SupportsBinary);
        var t2 = new PollingTransport(new SocketOptions { Hostname = "localhost", ForceBase64 = true });
        Assert.False(t2.SupportsBinary);
    }

    [Fact]
    public void Close_before_open_is_noop()
    {
        var s = new EngineIoClientSocket();
        s.Close();
        Assert.Equal(SocketReadyState.Closed, s.ReadyState);
    }
}
