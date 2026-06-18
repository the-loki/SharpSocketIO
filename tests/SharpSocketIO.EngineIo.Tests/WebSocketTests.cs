using System.Text.RegularExpressions;
using System.Threading.Tasks;
using SharpSocketIO.EngineIo.Tests.Commons;
using Xunit;

namespace SharpSocketIO.EngineIo.Tests;

/// <summary>
/// Ported subset of test/server.js that exercises the live WebSocket surface end-to-end
/// via WsDriver (ClientWebSocket against a real Kestrel server).
/// </summary>
public class WebSocketTests
{
    private static string SidFromBody(string body)
    {
        var m = Regex.Match(body, "\"sid\":\"([^\"]+)\"");
        return m.Success ? m.Groups[1].Value : string.Empty;
    }

    // server.js: "should be able to open with ws directly"
    [Fact]
    public async Task Opens_ws_only_handshake()
    {
        await using var driver = await TestDriver.StartRealAsync(o => o.Transports = new[] { "websocket" });
        var ws = new WsDriver(driver.RealBaseAddress);
        var (openText, sid) = await ws.ConnectWsOnlyAsync();
        Assert.NotEmpty(sid);
        Assert.StartsWith("0", openText);
        Assert.Contains("\"sid\":", openText);
        Assert.Contains("\"upgrades\":[]", openText);
        await ws.CloseAsync();
    }

    // server.js: full upgrade flow polling → websocket
    [Fact]
    public async Task Upgrades_from_polling_to_websocket()
    {
        await using var driver = await TestDriver.StartRealAsync();
        // 1. open polling
        var (_, body, _) = await driver.GetAsync("/engine.io/?EIO=4&transport=polling");
        var sid = SidFromBody(body);
        Assert.Contains("\"upgrades\":[\"websocket\"]", body);
        // 2. upgrade via WS probe sequence
        var ws = new WsDriver(driver.RealBaseAddress);
        await ws.UpgradeAsync(sid);
        await Task.Delay(150);
        Assert.True(driver.Engine.Clients.TryGetValue(sid, out var socket));
        Assert.Equal("websocket", socket.Transport.Name);
        Assert.True(socket.Upgraded);
        await ws.CloseAsync();
    }

    // server.js: "should not suggest any upgrades for websocket" (ws-only)
    [Fact]
    public async Task Ws_only_advertises_no_upgrades()
    {
        await using var driver = await TestDriver.StartRealAsync(o => o.Transports = new[] { "websocket" });
        var ws = new WsDriver(driver.RealBaseAddress);
        var (openText, _) = await ws.ConnectWsOnlyAsync();
        Assert.Contains("\"upgrades\":[]", openText);
        await ws.CloseAsync();
    }

    // message round-trip over ws (after ws-only open)
    [Fact]
    public async Task Sends_and_receives_message_over_ws()
    {
        await using var driver = await TestDriver.StartRealAsync(o => o.Transports = new[] { "websocket" });
        var ws = new WsDriver(driver.RealBaseAddress);
        var (_, sid) = await ws.ConnectWsOnlyAsync();
        string? received = null;
        driver.Engine.Clients[sid].On("message", args =>
        {
            if (args[0] is SharpSocketIO.EngineIo.Parser.Commons.RawData rd)
                received = rd.AsString();
        });
        await ws.SendTextAsync("4hello");
        await Task.Delay(150);
        Assert.Equal("hello", received);
        await ws.CloseAsync();
    }
}
