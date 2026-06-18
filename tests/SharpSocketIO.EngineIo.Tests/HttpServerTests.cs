using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using SharpSocketIO.EngineIo.Commons;
using SharpSocketIO.EngineIo.Tests.Commons;
using Xunit;

namespace SharpSocketIO.EngineIo.Tests;

/// <summary>
/// Ported subset of test/server.js that exercises the live HTTP surface end-to-end
/// via TestDriver (HttpClient + TestServer). WebSocket-only tests are deferred to 3C.
/// </summary>
public class HttpServerTests
{
    private static string SidFromBody(string body)
    {
        var m = Regex.Match(body, "\"sid\":\"([^\"]+)\"");
        return m.Success ? m.Groups[1].Value : string.Empty;
    }

    // server.js: "should disallow non-existent transports"
    [Fact]
    public async Task Disallows_nonexistent_transport()
    {
        await using var driver = await TestDriver.StartAsync();
        var (status, body, _) = await driver.GetAsync("/engine.io/?EIO=4&transport=tobi");
        Assert.Equal(400, status);
        Assert.Contains("\"code\":0", body);
        Assert.Contains("Transport unknown", body);
    }

    // server.js: "should disallow `constructor` as transports"
    [Fact]
    public async Task Disallows_constructor_transport()
    {
        await using var driver = await TestDriver.StartAsync();
        var (status, body, _) = await driver.GetAsync("/engine.io/?EIO=4&transport=constructor");
        Assert.Equal(400, status);
        Assert.Contains("\"code\":0", body);
    }

    // server.js: "should disallow invalid handshake method"
    [Fact]
    public async Task Disallows_invalid_method_via_post_without_sid()
    {
        await using var driver = await TestDriver.StartAsync();
        var (status, _) = await driver.PostAsync("/engine.io/?EIO=4&transport=polling", "");
        Assert.Equal(400, status);
    }

    // server.js: "should disallow unsupported protocol versions"
    [Fact]
    public async Task Disallows_unsupported_protocol_version()
    {
        await using var driver = await TestDriver.StartAsync();
        var (status, body, _) = await driver.GetAsync("/engine.io/?EIO=2&transport=polling");
        Assert.Equal(400, status);
        Assert.Contains("Unsupported protocol version", body);
    }

    // server.js: "should send the io cookie" (default)
    [Fact]
    public async Task Sends_io_cookie_default()
    {
        await using var driver = await TestDriver.StartAsync(o => o.CookieConfig = new CookieConfig());
        var (status, body, headers) = await driver.GetAsync("/engine.io/?EIO=4&transport=polling");
        Assert.Equal(200, status);
        var sid = SidFromBody(body);
        Assert.True(headers.TryGetValue("Set-Cookie", out var cookie));
        Assert.Equal($"io={sid}; Path=/; HttpOnly", cookie);
    }

    // server.js: "should send the io cookie custom name"
    [Fact]
    public async Task Sends_cookie_with_custom_name()
    {
        await using var driver = await TestDriver.StartAsync(o => o.CookieConfig = new CookieConfig { Name = "woot" });
        var (_, body, headers) = await driver.GetAsync("/engine.io/?EIO=4&transport=polling");
        var sid = SidFromBody(body);
        Assert.True(headers.TryGetValue("Set-Cookie", out var cookie));
        Assert.Equal($"woot={sid}; Path=/; HttpOnly", cookie);
    }

    // server.js: "should send the io cookie with sameSite=strict"
    [Fact]
    public async Task Sends_cookie_with_sameSite_strict()
    {
        await using var driver = await TestDriver.StartAsync(o => o.CookieConfig = new CookieConfig { SameSite = "strict" });
        var (_, body, headers) = await driver.GetAsync("/engine.io/?EIO=4&transport=polling");
        Assert.True(headers.TryGetValue("Set-Cookie", out var cookie));
        Assert.Contains("SameSite=Strict", cookie);
    }

    // server.js: "should not send the io cookie"
    [Fact]
    public async Task Does_not_send_cookie_when_disabled()
    {
        await using var driver = await TestDriver.StartAsync();
        var (_, _, headers) = await driver.GetAsync("/engine.io/?EIO=4&transport=polling");
        Assert.False(headers.ContainsKey("Set-Cookie"));
    }

    // server.js: "should exchange handshake data"
    [Fact]
    public async Task Exchanges_handshake_data()
    {
        await using var driver = await TestDriver.StartAsync();
        var (status, body, _) = await driver.GetAsync("/engine.io/?EIO=4&transport=polling");
        Assert.Equal(200, status);
        Assert.StartsWith("0", body); // open packet type
        Assert.Contains("\"sid\":", body);
        Assert.Contains("\"upgrades\":[\"websocket\"]", body);
        Assert.Contains("\"pingInterval\":25000", body);
        Assert.Contains("\"pingTimeout\":20000", body);
        Assert.Contains("\"maxPayload\":1000000", body);
    }

    // server.js: "should support requests without trailing slash"
    [Fact]
    public async Task Supports_no_trailing_slash()
    {
        await using var driver = await TestDriver.StartAsync();
        var (status, _, _) = await driver.GetAsync("/engine.io?EIO=4&transport=polling");
        Assert.Equal(200, status);
    }

    // server.js: "should allow arbitrary data through query string"
    [Fact]
    public async Task Accepts_arbitrary_query()
    {
        await using var driver = await TestDriver.StartAsync();
        var (status, _, _) = await driver.GetAsync("/engine.io/?EIO=4&transport=polling&foo=bar");
        Assert.Equal(200, status);
    }

    // server.js: cors
    [Fact]
    public async Task Sends_cors_header()
    {
        await using var driver = await TestDriver.StartAsync();
        var (status, _, headers) = await driver.GetAsync("/engine.io/?EIO=4&transport=polling");
        Assert.Equal(200, status);
        Assert.True(headers.TryGetValue("Access-Control-Allow-Origin", out var origin));
        Assert.Equal("*", origin);
    }

    // server.js: POST ingest replies "ok"
    [Fact]
    public async Task Post_replies_ok()
    {
        await using var driver = await TestDriver.StartAsync();
        var (_, body, _) = await driver.GetAsync("/engine.io/?EIO=4&transport=polling");
        var sid = SidFromBody(body);
        var (status, respBody) = await driver.PostAsync($"/engine.io/?EIO=4&transport=polling&sid={sid}", "4hello");
        Assert.Equal(200, status);
        Assert.Equal("ok", respBody);
    }

    // server.js: "should disallow non-existent sids"
    [Fact]
    public async Task Disallows_unknown_sid_on_poll()
    {
        await using var driver = await TestDriver.StartAsync();
        var (status, body, _) = await driver.GetAsync("/engine.io/?EIO=4&transport=polling&sid=nope");
        Assert.Equal(400, status);
        Assert.Contains("\"code\":0", body);
    }
}
