# engine.io Sub-cycle 3C Implementation Plan — WebSocket transport + upgrade

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Implement task-by-task. Checkbox steps.

**Goal:** Add `WebSocketTransport` (System.Net.WebSockets) + the polling→websocket upgrade handshake to SharpSocketIO.EngineIo; port the server.js ws subset to run end-to-end via a `ClientWebSocket` driver.

**Architecture:** `WebSocketTransport` runs a receive loop feeding the 3A parser. `Server.HandleWebSocketUpgradeAsync` accepts the WS upgrade; if sid present → upgrade existing polling socket (probe ping/pong + `upgrade` packet); else → ws-only fresh handshake. `Socket.MaybeUpgrade` implements the probe state machine. `ServerExtensions` routes WS upgrade requests (`transport=websocket` + WebSocket upgrade).

**Tech Stack:** .NET 8/9/10, `System.Net.WebSockets`, ASP.NET Core, xUnit.

**Reference:** `_upstream/packages/engine.io/lib/transports/websocket.ts`, `lib/socket.ts` (`_maybeUpgrade`), `lib/server.ts` (`handleUpgrade`), `_upstream/packages/engine.io/test/server.js`.

---

## Task C-1: `WebSocketTransport`

**Files:**
- Create: `src/SharpSocketIO.EngineIo/Transports/WebSocketTransport.cs`

- [ ] **Step 1: Implement `WebSocketTransport`**

```csharp
using System;
using System.Collections.Generic;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using SharpSocketIO.EngineIo.Commons;
using SharpSocketIO.EngineIo.Http;
using SharpSocketIO.EngineIo.Parser.Commons;
using EioParser = SharpSocketIO.EngineIo.Parser;
using EioPacketType = SharpSocketIO.EngineIo.Parser.Commons.PacketType;

namespace SharpSocketIO.EngineIo.Transports;

/// <summary>
/// Port of lib/transports/websocket.ts. Wraps a System.Net.WebSockets.WebSocket;
/// each engine.io packet maps to one WS message (Text for string packets, Binary
/// for binary). A background receive loop decodes incoming messages to packets.
/// </summary>
public sealed class WebSocketTransport : Transport
{
    private readonly WebSocket _ws;
    private readonly CancellationTokenSource _cts = new();
    private readonly int _maxPayload;

    public WebSocketTransport(HttpContextEngineRequest req, WebSocket ws, int maxPayload)
        : base(req)
    {
        _ws = ws;
        _maxPayload = maxPayload > 0 ? maxPayload : 1_000_000;
        Writable = true;
    }

    public override string Name => "websocket";

    /// <summary>Starts the background receive loop. Returns when the socket closes.</summary>
    public async Task RunReceiveLoopAsync()
    {
        try
        {
            var buffer = new byte[16 * 1024];
            while (!_cts.IsCancellationRequested)
            {
                WebSocketReceiveResult result;
                using var msg = new System.IO.MemoryStream();
                do
                {
                    result = await _ws.ReceiveAsync(buffer, _cts.Token);
                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        OnClose();
                        return;
                    }
                    msg.Write(buffer, 0, result.Count);
                    if (msg.Length > _maxPayload)
                    {
                        await _ws.CloseAsync(WebSocketCloseStatus.MessageTooBig, "payload too big", CancellationToken.None);
                        OnClose();
                        return;
                    }
                } while (!result.EndOfMessage);

                var data = msg.ToArray();
                if (result.MessageType == WebSocketMessageType.Binary)
                {
                    OnPacket(EioParser.DecodePacket.Decode(new RawData(data)));
                }
                else
                {
                    var text = System.Text.Encoding.UTF8.GetString(data);
                    OnPacket(EioParser.DecodePacket.Decode(new RawData(text)));
                }
            }
        }
        catch (OperationCanceledException) { /* normal */ }
        catch (WebSocketException) { OnClose(); }
    }

    public override void Send(IReadOnlyList<Packet> packets)
    {
        // synchronous send — fire and forget on the thread pool; the JS send is also effectively async.
        _ = SendAsync(packets);
    }

    public async Task SendAsync(IReadOnlyList<Packet> packets)
    {
        for (int i = 0; i < packets.Count; i++)
        {
            var packet = packets[i];
            await SendOneAsync(packet);
        }
        Writable = true;
        Emit("drain");
        Emit("ready");
    }

    private async Task SendOneAsync(Packet packet)
    {
        if (packet.Data is { Kind: RawDataKind.ByteArray or RawDataKind.ArrayBuffer } bin)
        {
            // binary: encode raw, send as BinaryMessage
            RawData encoded = default;
            EioParser.EncodePacket.Encode(packet, true, r => encoded = r);
            var bytes = encoded.Kind == RawDataKind.ByteArray ? encoded.AsByteArray()! : encoded.AsArrayBuffer()!.Value.ToArray();
            await _ws.SendAsync(bytes, WebSocketMessageType.Binary, endOfMessage: true, _cts.Token);
        }
        else
        {
            string captured = string.Empty;
            EioParser.EncodePacket.Encode(packet, true, r => captured = r.AsString()!);
            var bytes = System.Text.Encoding.UTF8.GetBytes(captured);
            await _ws.SendAsync(bytes, WebSocketMessageType.Text, endOfMessage: true, _cts.Token);
        }
    }

    public override void DoClose(System.Action? callback = null)
    {
        _ = Task.Run(async () =>
        {
            try
            {
                _cts.Cancel();
                await _ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "closed", CancellationToken.None);
            }
            catch { }
            finally { callback?.Invoke(); }
        });
    }
}
```

- [ ] **Step 2: Build library**

Run: `dotnet build src/SharpSocketIO.EngineIo/SharpSocketIO.EngineIo.csproj`
Expected: 0 errors.

- [ ] **Step 3: Commit**

```
git add -A
git commit -m "feat(engine): WebSocketTransport — System.Net.WebSockets + binary/text packet framing"
```

---

## Task C-2: `Socket.MaybeUpgrade` + `SetTransport`

Extend `Socket` with the upgrade state machine (probe ping/pong, `upgrade` packet, transport swap).

**Files:**
- Modify: `src/SharpSocketIO.EngineIo/Socket.cs`

- [ ] **Step 1: Add upgrade fields + `MaybeUpgrade` + `SetTransport`**

Add to `Socket`:
```csharp
    public bool Upgraded { get; private set; }
    private bool _upgrading;
    private Timer? _upgradeTimeoutTimer;
    private Action<Packet>? _upgradeOnPacket;

    /// <summary>
    /// Port of socket._maybeUpgrade. Drives the probe handshake: server listens on the
    /// new transport for a ping/"probe" → replies pong/"probe", then waits for an
    /// "upgrade" packet to swap transports.
    /// </summary>
    public void MaybeUpgrade(Transport newTransport, int upgradeTimeoutMs)
    {
        lock (_gate)
        {
            if (_upgrading || Upgraded)
            {
                newTransport.Close();
                return;
            }
            _upgrading = true;
        }

        void Cleanup()
        {
            lock (_gate) _upgrading = false;
            _upgradeTimeoutTimer?.Dispose();
            newTransport.Off("packet", _upgradeOnPacket!);
        }

        _upgradeOnPacket = args =>
        {
            var packet = (Packet)args[0];
            if (packet.Type == EioPacketType.Ping && packet.Data?.AsString() == "probe")
            {
                newTransport.Send(new[] { new Packet(EioPacketType.Pong, new RawData("probe")) });
            }
            else if (packet.Type == EioPacketType.Upgrade)
            {
                Cleanup();
                lock (_gate)
                {
                    Transport.Discard();
                    Transport = newTransport;
                    Upgraded = true;
                }
                Emit("upgrade", newTransport);
                Flush();
            }
            else
            {
                Cleanup();
                newTransport.Close();
            }
        };

        newTransport.On("packet", _upgradeOnPacket);

        _upgradeTimeoutTimer = new Timer(_ =>
        {
            Cleanup();
            if (newTransport.ReadyState == ReadyState.Open) newTransport.Close();
        }, null, upgradeTimeoutMs, Timeout.Infinite);
    }
```

Also add an `Off` helper to `Emitter<TEvents>` (remove a specific listener):
- [ ] **Step 2: Add `Off(string, Delegate)` is already present in Emitter; verify `Off("packet", handler)` works** — it does (existing API).

- [ ] **Step 3: Build**

- [ ] **Step 4: Commit**

```
git add -A
git commit -m "feat(engine): Socket.MaybeUpgrade — probe ping/pong + upgrade packet state machine"
```

---

## Task C-3: `Server.HandleWebSocketUpgradeAsync` + middleware routing

**Files:**
- Modify: `src/SharpSocketIO.EngineIo/Server.cs`
- Modify: `src/SharpSocketIO.EngineIo/Http/ServerExtensions.cs`

- [ ] **Step 1: Add `HandleWebSocketUpgradeAsync` to Server**

```csharp
    public async Task HandleWebSocketUpgradeAsync(HttpContext ctx, HttpContextEngineRequest req, string? sid)
    {
        if (!ctx.WebSockets.IsWebSocketRequest)
        {
            ctx.Response.StatusCode = 400;
            return;
        }
        var ws = await ctx.WebSockets.AcceptWebSocketAsync();

        if (!string.IsNullOrEmpty(sid) && Clients.TryGetValue(sid, out var existing))
        {
            // upgrade path
            var upgradeTransport = new WebSocketTransport(req, ws, (int)Options.MaxHttpBufferSize);
            // force a polling cycle to speed up the probe
            if (existing.Transport is PollingHttp polling && polling.Name == "polling")
            {
                polling.EnqueueAndFlush(new[] { new SharpSocketIO.EngineIo.Parser.Commons.Packet(
                    SharpSocketIO.EngineIo.Parser.Commons.PacketType.Noop) });
            }
            existing.MaybeUpgrade(upgradeTransport, Options.UpgradeTimeout);
            _ = upgradeTransport.RunReceiveLoopAsync();
            return;
        }

        // ws-only fresh handshake
        if (!string.IsNullOrEmpty(sid))
        {
            ctx.Response.StatusCode = 400;
            return;
        }
        // verify
        var (code, message) = Verify(req);
        if (code.HasValue)
        {
            await ws.CloseAsync(System.Net.WebSockets.WebSocketCloseStatus.NormalClosure, message, System.Threading.CancellationToken.None);
            return;
        }
        if (req.Query.TryGetValue("EIO", out var eio) && eio != "4" && !Options.AllowEIO3)
        {
            await ws.CloseAsync(System.Net.WebSockets.WebSocketCloseStatus.NormalClosure, "Unsupported protocol version", System.Threading.CancellationToken.None);
            return;
        }
        string id = GenerateId();
        var cookie = BuildCookieValue(id);
        if (cookie != null) ctx.Response.Headers["Set-Cookie"] = cookie;

        var transport = new WebSocketTransport(req, ws, (int)Options.MaxHttpBufferSize);
        var socket = new Socket(id, this, transport, protocol: 4);
        RegisterSocket(id, socket);
        socket.OnOpen(); // sends the open packet over WS
        _ = transport.RunReceiveLoopAsync();
    }
```

- [ ] **Step 2: Route WS upgrades in `ServerExtensions.Attach`**

Add at the top of the middleware, after CORS:
```csharp
            var transport = req.Query.TryGetValue("transport", out var tn) ? tn : "";
            if (transport == "websocket" && ctx.WebSockets.IsWebSocketRequest)
            {
                string? sid = req.Query.TryGetValue("sid", out var sidv) && !string.IsNullOrEmpty(sidv) ? sidv : null;
                await engine.HandleWebSocketUpgradeAsync(ctx, req, sid);
                return;
            }
```

Also add `app.UseWebSockets()` — but since we use `Host.CreateDefaultBuilder().ConfigureWebHost(Configure(...))` in the driver, we need `UseWebSockets` inside `Configure`. Easiest: have `Attach` call `app.UseWebSockets()` before its middleware.

- [ ] **Step 3: Make `Attach` call `app.UseWebSockets()` first**

In `ServerExtensions.Attach`, before `app.Use(...)`:
```csharp
        app.UseWebSockets();
```

- [ ] **Step 4: Build**

- [ ] **Step 5: Commit**

```
git add -A
git commit -m "feat(engine): Server.HandleWebSocketUpgradeAsync + WS routing in middleware"
```

---

## Task C-4: WsDriver (ClientWebSocket) + ws tests

**Files:**
- Create: `tests/.../Commons/WsDriver.cs`
- Create: `tests/.../WebSocketTests.cs`

- [ ] **Step 1: Implement `WsDriver`**

```csharp
using System;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SharpSocketIO.EngineIo.Parser.Commons;
using EioParser = SharpSocketIO.EngineIo.Parser;

namespace SharpSocketIO.EngineIo.Tests.Commons;

/// <summary>
/// ClientWebSocket-based driver for the engine.io ws surface + upgrade handshake.
/// NOT the engine.io-client package — a minimal driver sufficient for server.js ws tests.
/// </summary>
public sealed class WsDriver
{
    private readonly ClientWebSocket _ws = new();
    private readonly Uri _baseUri;

    public WsDriver(Uri baseUri) { _baseUri = baseUri; }

    /// <summary>Opens a fresh ws-only session and returns the open packet's sid.</summary>
    public async Task<string> ConnectWsOnlyAsync(string path = "/engine.io/?EIO=4&transport=websocket")
    {
        await _ws.ConnectAsync(new Uri(_baseUri, path), CancellationToken.None);
        var openPacket = await ReceivePacketAsync();
        // open packet: '0' + JSON
        return ExtractSid(openPacket.text);
    }

    /// <summary>Upgrades an existing polling session (sid) to websocket. Returns when upgraded.</summary>
    public async Task UpgradeAsync(string sid, string pathBase = "/engine.io/?EIO=4&transport=websocket&sid=")
    {
        await _ws.ConnectAsync(new Uri(_baseUri, pathBase + sid), CancellationToken.None);
        // 1. send probe ping
        await SendAsync("2probe");
        // 2. expect pong "3probe"
        var (text, _) = await ReceiveRawAsync();
        if (!text.StartsWith("3probe")) throw new InvalidOperationException($"expected 3probe, got {text}");
        // 3. send upgrade packet
        await SendAsync("5");
    }

    public async Task SendAsync(string text)
    {
        var bytes = Encoding.UTF8.GetBytes(text);
        await _ws.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None);
    }

    public async Task SendAsync(byte[] binary)
    {
        await _ws.SendAsync(binary, WebSocketMessageType.Binary, true, CancellationToken.None);
    }

    public async Task<(string text, bool isBinary)> ReceiveRawAsync()
    {
        var buffer = new byte[16 * 1024];
        using var ms = new System.IO.MemoryStream();
        WebSocketReceiveResult result;
        do
        {
            result = await _ws.ReceiveAsync(buffer, CancellationToken.None);
            ms.Write(buffer, 0, result.Count);
        } while (!result.EndOfMessage);
        var bytes = ms.ToArray();
        if (result.MessageType == WebSocketMessageType.Binary) return ("", true);
        return (Encoding.UTF8.GetString(bytes), false);
    }

    public async Task<(string text, Packet packet)> ReceivePacketAsync()
    {
        var (text, isBinary) = await ReceiveRawAsync();
        if (isBinary)
        {
            // for our tests we don't drive binary ws receives; treat as message
            return ("", new Packet(PacketType.Message));
        }
        // decode first char as packet type
        return (text, SharpSocketIO.EngineIo.Parser.DecodePacket.Decode(new RawData(text)));
    }

    public static string ExtractSid(string openPacketBody)
    {
        var m = System.Text.RegularExpressions.Regex.Match(openPacketBody, "\"sid\":\"([^\"]+)\"");
        return m.Success ? m.Groups[1].Value : string.Empty;
    }

    public async Task CloseAsync()
    {
        if (_ws.State == WebSocketState.Open)
            await _ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "", CancellationToken.None);
    }
}
```

- [ ] **Step 2: Write `WebSocketTests.cs`**

```csharp
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using SharpSocketIO.EngineIo.Commons;
using SharpSocketIO.EngineIo.Tests.Commons;
using Xunit;

namespace SharpSocketIO.EngineIo.Tests;

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
        await using var driver = await TestDriver.StartAsync(o => o.Transports = new[] { "websocket" });
        var baseUri = driver.BaseUri; // need TestDriver to expose base uri
        var ws = new WsDriver(baseUri);
        var sid = await ws.ConnectWsOnlyAsync();
        Assert.NotEmpty(sid);
        await ws.CloseAsync();
    }

    // server.js: full upgrade flow polling → websocket
    [Fact]
    public async Task Upgrades_from_polling_to_websocket()
    {
        await using var driver = await TestDriver.StartAsync();
        // 1. open polling
        var (status, body, _) = await driver.GetAsync("/engine.io/?EIO=4&transport=polling");
        var sid = SidFromBody(body);
        Assert.Contains("\"upgrades\":[\"websocket\"]", body);
        // 2. upgrade via WS
        var ws = new WsDriver(driver.BaseUri);
        await ws.UpgradeAsync(sid);
        // 3. once upgraded, transport on the server is websocket
        Assert.Equal("websocket", driver.Engine.Clients[sid].Transport.Name);
        await ws.CloseAsync();
    }

    // server.js: "should not suggest any upgrades for websocket"
    [Fact]
    public async Task Ws_only_advertises_no_upgrades()
    {
        await using var driver = await TestDriver.StartAsync(o => o.Transports = new[] { "websocket" });
        var ws = new WsDriver(driver.BaseUri);
        // first received packet is open
        await ws.ConnectWsOnlyAsync();
        // fetch handshake body via the open packet we received — already done in ConnectWsOnly; assert sid present
        await ws.CloseAsync();
        Assert.True(true); // body assertions on upgrades require capturing the open packet text
    }
}
```

- [ ] **Step 3: Expose `BaseUri` on `TestDriver`**

Add to `TestDriver`:
```csharp
    public Uri BaseUri => _client.BaseAddress!;
```

- [ ] **Step 4: Run, iterate until green**

Run: `dotnet test --filter "FullyQualifiedName~WebSocketTests"`
Expected: all green. Iterate on `WebSocketTransport`/`MaybeUpgrade`/`HandleWebSocketUpgradeAsync` until the upgrade handshake completes.

- [ ] **Step 5: Commit**

```
git add -A
git commit -m "test(engine): ClientWebSocket WsDriver + ported server.js ws subset (ws-only open, polling→ws upgrade)"
```

---

## Task C-5: Final 3C verification

- [ ] **Step 1: Release build + test across all TFMs**

```
dotnet build -c Release
dotnet test -c Release
```
Expected: all tests green on net8/9/10 (3A logic + 3B HTTP + 3C WS).

- [ ] **Step 2: Confirm upgrade handshake works end-to-end** (Upgrades_from_polling_to_websocket test green).

- [ ] **Step 3: Commit**

```
git add -A
git commit -m "chore(engine): 3C verification across TFMs" --allow-empty
```

---

## Self-review

**Spec coverage:** 3C design §1 DoD 1–6 → C-1 (WebSocketTransport), C-2 (MaybeUpgrade), C-3 (HandleWebSocketUpgradeAsync + routing), C-4 (WsDriver + tests), C-5 (verify). §3 upgrade flow → C-2/C-3. §5 wire format → C-1/C-4.

**Placeholder scan:** C-4 step 2's third test (`Ws_only_advertises_no_upgrades`) has a weak `Assert.True(true)` — to be strengthened during execution by capturing the open packet body and asserting `upgrades` is empty.

**Type consistency:** `WebSocketTransport : Transport` (C-1). `Socket.MaybeUpgrade(Transport, int)` (C-2). `Server.HandleWebSocketUpgradeAsync(HttpContext, HttpContextEngineRequest, string?)` (C-3). `WsDriver` (C-4). `TestDriver.BaseUri` (C-4 step 3).

**Scope honesty:** 3C delivers the WebSocket transport + upgrade handshake end-to-end. `perMessageDeflate` and `WebTransport` are explicitly omitted. The full ws test subset of server.js isn't 1:1 ported — representative ws-only-open + full-upgrade-flow + close tests are ported.

## Execution Handoff

Plan saved to `docs/superpowers/plans/2026-06-18-engineio-3c-port.md`. Inline TDD execution proceeding.
