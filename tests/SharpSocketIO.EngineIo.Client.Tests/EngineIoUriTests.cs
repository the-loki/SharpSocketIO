using SharpSocketIO.EngineIo.Client.Contrib;
using Xunit;

namespace SharpSocketIO.EngineIo.Client.Tests;

public class EngineIoUriTests
{
    private static string Repeat(char c, int n)
    {
        var arr = new char[n];
        for (int i = 0; i < n; i++) arr[i] = c;
        return new string(arr);
    }

    [Fact]
    public void Parses_various_uris()
    {
        var http = EngineIoUri.Parse("http://google.com");
        Assert.Equal("http", http.Protocol);
        Assert.Equal("", http.Port);
        Assert.Equal("google.com", http.Host);

        var https = EngineIoUri.Parse("https://www.google.com:80");
        Assert.Equal("https", https.Protocol);
        Assert.Equal("80", https.Port);
        Assert.Equal("www.google.com", https.Host);

        var query = EngineIoUri.Parse("google.com:8080/foo/bar?foo=bar");
        Assert.Equal("8080", query.Port);
        Assert.Equal("foo=bar", query.Query);
        Assert.Equal("/foo/bar", query.Path);
        Assert.Equal("/foo/bar?foo=bar", query.Relative);
        Assert.Equal("bar", query.QueryKey["foo"]);
        Assert.Equal("foo", query.PathNames[0]);
        Assert.Equal("bar", query.PathNames[1]);

        var localhost = EngineIoUri.Parse("localhost:8080");
        Assert.Equal("", localhost.Protocol);
        Assert.Equal("localhost", localhost.Host);
        Assert.Equal("8080", localhost.Port);

        var ipv6 = EngineIoUri.Parse("2001:0db8:85a3:0042:1000:8a2e:0370:7334");
        Assert.Equal("", ipv6.Protocol);
        Assert.Equal("2001:0db8:85a3:0042:1000:8a2e:0370:7334", ipv6.Host);
        Assert.Equal("", ipv6.Port);

        var ipv6port = EngineIoUri.Parse("2001:db8:85a3:42:1000:8a2e:370:7334:80");
        Assert.Equal("2001:db8:85a3:42:1000:8a2e:370:7334", ipv6port.Host);
        Assert.Equal("80", ipv6port.Port);

        var ipv6abbrev = EngineIoUri.Parse("2001::7334:a:80");
        Assert.Equal("", ipv6abbrev.Protocol);
        Assert.Equal("2001::7334:a:80", ipv6abbrev.Host);
        Assert.Equal("", ipv6abbrev.Port);

        var ipv6http = EngineIoUri.Parse("http://[2001::7334:a]:80");
        Assert.Equal("http", ipv6http.Protocol);
        Assert.Equal("80", ipv6http.Port);
        Assert.Equal("2001::7334:a", ipv6http.Host);

        var ipv6query = EngineIoUri.Parse("http://[2001::7334:a]:80/foo/bar?foo=bar");
        Assert.Equal("http", ipv6query.Protocol);
        Assert.Equal("80", ipv6query.Port);
        Assert.Equal("2001::7334:a", ipv6query.Host);
        Assert.Equal("/foo/bar?foo=bar", ipv6query.Relative);

        var withUserInfo = EngineIoUri.Parse("ws://foo:bar@google.com");
        Assert.Equal("ws", withUserInfo.Protocol);
        Assert.Equal("foo:bar", withUserInfo.UserInfo);
        Assert.Equal("foo", withUserInfo.User);
        Assert.Equal("bar", withUserInfo.Password);
        Assert.Equal("google.com", withUserInfo.Host);

        var relWithQuery = EngineIoUri.Parse("/foo?bar=@example.com");
        Assert.Equal("", relWithQuery.Host);
        Assert.Equal("/foo", relWithQuery.Path);
        Assert.Equal("bar=@example.com", relWithQuery.Query);
    }

    [Fact]
    public void Throws_when_uri_too_long()
    {
        Assert.ThrowsAny<System.Exception>(() => EngineIoUri.Parse(Repeat('a', 8001)));
    }

    [Fact]
    public void Secure_flag_reflects_protocol()
    {
        Assert.True(EngineIoUri.Parse("https://x").Secure);
        Assert.True(EngineIoUri.Parse("wss://x").Secure);
        Assert.False(EngineIoUri.Parse("http://x").Secure);
        Assert.False(EngineIoUri.Parse("ws://x").Secure);
    }
}
