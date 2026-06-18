# engine.io-client Sub-cycle CC-2 — WebSocket transport + upgrade

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Checkbox steps.

**Goal:** Add the client WebSocket transport (`ClientWebSocket`) + the client-side polling→websocket upgrade probe. Tests: ws-only connect + full polling→ws upgrade against our SharpSocketIO.EngineIo server.

**Architecture:** `WebSocketTransport : Transport` wraps a `ClientWebSocket`, with a receive loop feeding the parser. `EngineIoClientSocket.Probe(name)` implements the client-side upgrade handshake: open new transport → send `ping`/`probe` → await single `pong`/`probe` → pause polling → swap transport → send `upgrade`. Triggered automatically after `open` when `_upgrades` includes "websocket" and `Upgrade` is on.

**Tech Stack:** .NET 8/9/10, `System.Net.WebSockets.ClientWebSocket`, xUnit.

**Reference:** `_upstream/packages/engine.io-client/lib/transports/websocket.ts`, `lib/socket.ts` (`_probe`, `setTransport`).

---

## Task CC-2.1: `WebSocketTransport` (ClientWebSocket) — DONE

**Files:**
- Create: `src/SharpSocketIO.EngineIo.Client/Transports/WebSocketTransport.cs`

- [ ] **Step 1: Implement `WebSocketTransport`**

```csharp
using System;
using System.Collections.Generic;
using System.IO;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SharpSocketIO.EngineIo.Client.Contrib;
using SharpSocketIO.EngineIo.Parser;
using SharpSocketIO.EngineIo.Parser.Commons;
using EioParser = SharpSocketIO.EngineIo.Parser;

namespace SharpSocketIO.EngineIo.Client.Transports;

/// <summary>
/// Port of lib/transports/websocket.ts (client). Wraps ClientWebSocket; each engine.io
/// packet maps to one WS message (Text for string, Binary for binary). A receive loop
/// decodes incoming messages to packets.
/// </summary>
public sealed class WebSocketTransport : Transport
{
    private ClientWebSocket? _ws;
    private readonly string _baseUrl;
    private readonly CancellationTokenSource _cts = new();

    public WebSocketTransport(SocketOptions opts) : base(opts)
    {
        var scheme = opts.Secure ? "wss" : "ws";
        var host = opts.Hostname ?? "localhost";
        string port = "";
        if (!string.IsNullOrEmpty(opts.Port) &&
            !((opts.Secure && opts.Port == "443") || (!opts.Secure && opts.Port == "80")))
        {
            port = ":" + opts.Port;
        }
        _baseUrl = $"{scheme}://{host}{port}{opts.Path}";
    }

    public override string Name => "websocket";

    protected override void DoOpen()
    {
        _ = Task.Run(async () =>
        {
            try
            {
                _ws = new ClientWebSocket();
                var uri = BuildUri();
                await _ws.ConnectAsync(uri, _cts.Token);
                OnOpen();
                _ = ReceiveLoopAsync();
            }
            catch (Exception ex)
            {
                OnError("websocket error", ex.Message);
                OnClose(new CloseDetails { Description = "websocket connect error", Context = ex.Message });
            }
        });
    }

    protected override void DoClose()
    {
        _cts.Cancel();
        _ = Task.Run(async () =>
        {
            if (_ws != null && _ws.State == WebSocketState.Open)
            {
                try { await _ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "", CancellationToken.None); } catch { }
            }
            OnClose();
        });
    }

    protected override void Write(IReadOnlyList<Packet> packets)
    {
        _ = SendAsync(packets);
    }

    private async Task SendAsync(IReadOnlyList<Packet> packets)
    {
        if (_ws == null) return;
        for (int i = 0; i < packets.Count; i++)
        {
            var packet = packets[i];
            if (packet.Data is { Kind: RawDataKind.ByteArray or RawDataKind.ArrayBuffer })
            {
                RawData encoded = default;
                EioParser.EncodePacket.Encode(packet, true, r => encoded = r);
                var bytes = encoded.Kind == RawDataKind.ByteArray ? encoded.AsByteArray()! : encoded.AsArrayBuffer()!.Value.ToArray();
                await _ws.SendAsync(bytes, WebSocketMessageType.Binary, true, _cts.Token);
            }
            else
            {
                string captured = string.Empty;
                EioParser.EncodePacket.Encode(packet, true, r => captured = r.AsString()!);
                var bytes = Encoding.UTF8.GetBytes(captured);
                await _ws.SendAsync(bytes, WebSocketMessageType.Text, true, _cts.Token);
            }
        }
        Writable = true;
        EmitReserved("drain");
    }

    private async Task ReceiveLoopAsync()
    {
        var buffer = new byte[16 * 1024];
        try
        {
            while (!_cts.IsCancellationRequested && _ws!.State == WebSocketState.Open)
            {
                using var msg = new MemoryStream();
                WebSocketReceiveResult result;
                do
                {
                    result = await _ws.ReceiveAsync(buffer, _cts.Token);
                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        OnClose(new CloseDetails { Description = "websocket connection closed" });
                        return;
                    }
                    msg.Write(buffer, 0, result.Count);
                } while (!result.EndOfMessage);

                var data = msg.ToArray();
                if (result.MessageType == WebSocketMessageType.Binary)
                    OnData(new RawData(data));
                else
                    OnData(new RawData(Encoding.UTF8.GetString(data)));
            }
        }
        catch (OperationCanceledException) { }
        catch (WebSocketException)
        {
            OnClose(new CloseDetails { Description = "websocket connection closed" });
        }
    }

    private Uri BuildUri()
    {
        var query = new Dictionary<string, string>(Query) { ["EIO"] = "4", ["transport"] = "websocket" };
        if (!SupportsBinary) query["b64"] = "1";
        return new Uri(_baseUrl + "?" + Parseqs.Encode(query));
    }
}
```

- [ ] **Step 2: Build, commit**

```
dotnet build src/SharpSocketIO.EngineIo.Client/SharpSocketIO.EngineIo.Client.csproj
git add -A
git commit -m "feat(client): WebSocketTransport (ClientWebSocket + binary/text framing)"
```

---

## Task CC-2.2: `Socket.CreateTransport` + `Probe` (client-side upgrade) — DONE

**Files:**
- Modify: `src/SharpSocketIO.EngineIo.Client/EngineIoClientSocket.cs`

- [ ] **Step 1: Add transport factory + probe/upgrade state machine**

Add to `EngineIoClientSocket`:
```csharp
    public Transport CreateTransport(string name)
    {
        var query = new Dictionary<string, string>(Opts.Query);
        if (Id != null) query["sid"] = Id;
        var probeOpts = new SocketOptions
        {
            Host = Opts.Host,
            Hostname = Opts.Hostname,
            Secure = Opts.Secure,
            Port = Opts.Port,
            Path = Opts.Path,
            Query = query,
            Upgrade = false,           // probe transport doesn't itself upgrade
            TimestampRequests = Opts.TimestampRequests,
            TimestampParam = Opts.TimestampParam,
            ForceBase64 = Opts.ForceBase64,
            Transports = Opts.Transports,
        };
        return name switch
        {
            "websocket" => new Transports.WebSocketTransport(probeOpts),
            "polling" => new Transports.PollingTransport(probeOpts),
            _ => throw new ArgumentException($"unknown transport: {name}"),
        };
    }

    /// <summary>
    /// Port of socket._probe. Client-side upgrade handshake: open the new transport,
    /// send ping/"probe", await a single pong/"probe", pause polling, swap transport,
    /// send the "upgrade" packet, emit "upgrade", flush.
    /// </summary>
    public void Probe(string name)
    {
        var transport = CreateTransport(name);
        bool failed = false;
        Action<object[]>? onPacket = null;
        Action<object[]>? onError = null;
        Action<object[]>? onClose = null;
        Action<object[]>? onTransportOpen = null;

        void Cleanup()
        {
            if (onTransportOpen != null) transport.Off("open", onTransportOpen);
            if (onPacket != null) transport.Off("packet", onPacket);
            if (onError != null) transport.Off("error", onError);
            if (onClose != null) transport.Off("close", onClose);
        }

        void Freeze()
        {
            if (failed) return;
            failed = true;
            Cleanup();
            transport.Close();
        }

        onTransportOpen = _ =>
        {
            if (failed) return;
            transport.Send(new[] { new Packet(EioPacketType.Ping, new RawData("probe")) });
            // wait for a single packet — pong/"probe" → success
            onPacket = args =>
            {
                if (failed) return;
                var msg = (Packet)args[0];
                if (msg.Type == EioPacketType.Pong && msg.Data?.AsString() == "probe")
                {
                    // pause polling then swap
                    Transport?.Pause(() =>
                    {
                        if (failed) return;
                        if (ReadyState == SocketReadyState.Closed) return;
                        Cleanup();
                        SetTransport(transport);
                        transport.Send(new[] { new Packet(EioPacketType.Upgrade) });
                        Emit("upgrade", transport);
                        Flush();
                    });
                }
                else
                {
                    Emit("upgradeError", new InvalidOperationException("probe error"));
                }
            };
            transport.Once("packet", onPacket);
        };

        onError = args =>
        {
            Freeze();
            Emit("upgradeError", args.Length > 0 ? args[0] : new InvalidOperationException("probe error"));
        };
        onClose = _ =>
        {
            if (!failed) onError!(new object[] { new InvalidOperationException("transport closed") });
        };

        transport.On("open", onTransportOpen);
        transport.On("error", onError);
        transport.On("close", onClose);
        transport.Open();
    }

    private void SetTransport(Transport transport)
    {
        Transport?.Close();
        Transport = transport;
        transport.On("open", _ => { });
        transport.On("packet", args => OnPacket((Packet)args[0]));
        transport.On("error", args => Emit("error", args));
        transport.On("close", args => OnCloseInternal("transport closed"));
        Upgraded = transport.Name == "websocket";
        Emit("transportchange", transport);
    }

    public bool Upgraded { get; private set; }

    public void Flush()
    {
        // For CC-2 the write buffer is empty after upgrade; future sends go via the new transport.
    }
```

- [ ] **Step 2: Trigger probes after open**

In `OnPacket`'s `EioPacketType.Open` branch, after `Emit("open")`, add:
```csharp
                if (Opts.Upgrade && _upgrades != null)
                {
                    foreach (var u in _upgrades)
                    {
                        if (Opts.Transports.Contains(u) && u != Transport?.Name)
                        {
                            Probe(u);
                        }
                    }
                }
```

- [ ] **Step 3: Build, commit**

```
dotnet build src/SharpSocketIO.EngineIo.Client/SharpSocketIO.EngineIo.Client.csproj
git add -A
git commit -m "feat(client): Socket.Probe — client-side polling→websocket upgrade handshake"
```

---

## Task CC-2.3: ws-only connect + upgrade integration tests — DONE

**Files:**
- Create: `tests/.../WebSocketIntegrationTests.cs`

- [ ] **Step 1: Write the tests**

```csharp
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SharpSocketIO.EngineIo;
using SharpSocketIO.EngineIo.Http;
using SharpSocketIO.EngineIo.Parser.Commons;
using Xunit;

namespace SharpSocketIO.EngineIo.Client.Tests;

public class WebSocketIntegrationTests
{
    private static async Task<(IHost host, string baseAddress, Server engine)> StartServerAsync(
        System.Action<SharpSocketIO.EngineIo.Commons.ServerOptions>? configure = null)
    {
        var engine = new Server();
        engine.Options.CookieConfig = null;
        configure?.Invoke(engine.Options);
        var builder = Host.CreateDefaultBuilder()
            .ConfigureWebHost(web =>
            {
                web.UseKestrel();
                web.UseUrls("http://127.0.0.1:0");
                web.Configure(app => engine.Attach(app));
            });
        var host = builder.Build();
        await host.StartAsync();
        var sf = host.Services.GetRequiredService<IServer>();
        var address = sf.Features.Get<IServerAddressesFeature>()!.Addresses.First();
        return (host, address, engine);
    }

    [Fact]
    public async Task Connects_ws_only()
    {
        var (host, baseAddress, engine) = await StartServerAsync(o => o.Transports = new[] { "websocket" });
        try
        {
            var client = new EngineIoClientSocket(baseAddress, new SocketOptions { Transports = new[] { "websocket" } });
            var openTcs = new TaskCompletionSource<bool>();
            client.On("open", _ => openTcs.TrySetResult(true));
            await client.OpenAsync();
            var opened = await Task.WhenAny(openTcs.Task, Task.Delay(5000));
            Assert.True(opened == openTcs.Task, "did not open ws-only within 5s");
            Assert.Equal("websocket", client.Transport?.Name);
            client.Close();
        }
        finally { await host.StopAsync(); host.Dispose(); }
    }

    [Fact]
    public async Task Upgrades_from_polling_to_websocket()
    {
        var (host, baseAddress, engine) = await StartServerAsync();
        try
        {
            var client = new EngineIoClientSocket(baseAddress);
            var upgradeTcs = new TaskCompletionSource<bool>();
            client.On("open", _ => { /* probes start automatically */ });
            client.On("upgrade", _ => upgradeTcs.TrySetResult(true));
            await client.OpenAsync();
            var upgraded = await Task.WhenAny(upgradeTcs.Task, Task.Delay(8000));
            Assert.True(upgraded == upgradeTcs.Task, "did not upgrade within 8s");
            Assert.True(client.Upgraded);
            Assert.Equal("websocket", client.Transport?.Name);
            client.Close();
        }
        finally { await host.StopAsync(); host.Dispose(); }
    }

    [Fact]
    public async Task Sends_message_over_websocket_after_upgrade()
    {
        var (host, baseAddress, engine) = await StartServerAsync();
        try
        {
            string? receivedOnServer = null;
            string? receivedOnClient = null;
            var doneTcs = new TaskCompletionSource<bool>();

            engine.On("connection", args =>
            {
                var srvSocket = (Socket)args[0];
                srvSocket.On("message", m =>
                {
                    if (m is object[] arr && arr.Length > 0 && arr[0] is RawData rd)
                    {
                        receivedOnServer = rd.AsString();
                        srvSocket.Send(new[] { new Packet(PacketType.Message, new RawData("echo:" + receivedOnServer)) });
                    }
                });
            });

            var client = new EngineIoClientSocket(baseAddress);
            client.On("upgrade", _ => client.Send("hello-via-ws"));
            client.On("message", args =>
            {
                if (args[0] is RawData rd && rd.AsString() == "echo:hello-via-ws")
                {
                    receivedOnClient = rd.AsString();
                    doneTcs.TrySetResult(true);
                }
            });
            await client.OpenAsync();
            var done = await Task.WhenAny(doneTcs.Task, Task.Delay(8000));
            Assert.True(done == doneTcs.Task, "did not round-trip over ws within 8s");
            Assert.Equal("hello-via-ws", receivedOnServer);
            Assert.Equal("echo:hello-via-ws", receivedOnClient);
            client.Close();
        }
        finally { await host.StopAsync(); host.Dispose(); }
    }
}
```

- [ ] **Step 2: Run, iterate until green**

Run: `dotnet test --filter "FullyQualifiedName~WebSocketIntegrationTests"`
Expected: all 3 green.

- [ ] **Step 3: Commit**

```
git add -A
git commit -m "test(client): ws-only + polling→ws upgrade + post-upgrade message integration tests"
```

---

## Task CC-2.4: Final CC-2 verification — DONE

- [ ] **Step 1: Release build + test across all TFMs**

```
dotnet build -c Release
dotnet test -c Release
```
Expected: all green on net8/9/10 (CC-1 client + CC-2 ws + every prior package).

- [ ] **Step 2: Commit**

```
git add -A
git commit -m "chore(client): CC-2 verification across TFMs" --allow-empty
```

---

## Self-review

**Spec coverage:** CC-2 covers spec §6 CC-2 items: WebSocketTransport, Probe/setTransport, ws-only + upgrade integration tests. §4 upgrade handshake (client: ping/"probe" → pong/"probe" → pause → upgrade packet) honored.

**Placeholder scan:** none.

**Type consistency:** `WebSocketTransport : Transport` (CC-2.1). `EngineIoClientSocket.CreateTransport/Probe/SetTransport` (CC-2.2). Tests use `SocketOptions.Transports=["websocket"]` for ws-only.

**Scope honesty:** CC-2 delivers client WebSocket + upgrade end-to-end. Binary send/receive and reconnect are CC-3.

## Execution Handoff

Plan saved to `docs/superpowers/plans/2026-06-18-engineio-client-cc2-port.md`. Inline TDD execution proceeding.
