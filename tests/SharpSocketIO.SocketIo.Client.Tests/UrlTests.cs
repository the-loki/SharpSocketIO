using global::SharpSocketIO.SocketIoClient;
using Xunit;

namespace SharpSocketIO.Tests.SocketIoClient;

public class UrlTests
{
    [Fact]
    public void Parses_http_url()
    {
        var u = Url.Parse("http://localhost:3000");
        Assert.Equal("localhost:3000", u.Host);
        Assert.False(u.Secure);
    }

    [Fact]
    public void Parses_https_url_as_secure()
    {
        var u = Url.Parse("https://example.com");
        Assert.Equal("example.com", u.Host);
        Assert.True(u.Secure);
    }

    [Fact]
    public void Parses_ws_and_wss_schemes()
    {
        Assert.False(Url.Parse("ws://localhost").Secure);
        Assert.True(Url.Parse("wss://localhost").Secure);
    }

    [Fact]
    public void Parses_query_string()
    {
        var u = Url.Parse("http://localhost?token=abc&foo=bar");
        Assert.Equal("abc", u.Query["token"]);
        Assert.Equal("bar", u.Query["foo"]);
    }

    [Fact]
    public void Defaults_path_to_socket_io()
    {
        var u = Url.Parse("http://localhost");
        Assert.Equal("/socket.io", u.Path);
    }

    [Fact]
    public void Parses_custom_path()
    {
        var u = Url.Parse("http://localhost/custom/path");
        Assert.Equal("/custom/path", u.Path);
    }
}
