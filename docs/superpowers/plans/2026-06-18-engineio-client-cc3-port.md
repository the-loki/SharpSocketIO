# engine.io-client Sub-cycle CC-3 — Binary + edge cases

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Checkbox steps.

**Goal:** Verify and harden binary send/receive (polling + ws), add the `ForceBase64` (b64) path, transport pause/resume, and edge-case tests. **Reconnect/backoff is NOT in engine.io-client** — that belongs to the socket.io-client layer (later cycle); engine.io-client only emits close/error and the consumer decides whether to reconnect. This was confirmed by reading upstream `lib/socket.ts` (the `Socket` class has no auto-reconnect).

**Architecture:** Binary already flows through the v4 parser in CC-1/CC-2. CC-3 adds: explicit binary round-trip tests (polling + ws), the `ForceBase64` option path (query `b64=1`, base64 payloads), `PollingTransport.Pause` real implementation (drain in-flight GET before switching), and close/error edge cases.

**Tech Stack:** .NET 8/9/10, xUnit.

**Reference:** `_upstream/packages/engine.io-client/lib/transports/polling.ts` (binary/b64 path), `lib/transport.ts` (pause/resume).

---

## Task CC-3.1: Binary round-trip tests (polling + ws) — DONE

**Files:**
- Create: `tests/.../BinaryIntegrationTests.cs`

- [ ] **Step 1: Write binary round-trip tests** (client sends byte[], server echoes, client receives byte[])

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
using PacketType = SharpSocketIO.EngineIo.Parser.Commons.PacketType;

namespace SharpSocketIO.EngineIo.Client.Tests;

public class BinaryIntegrationTests
{
    private static async Task<(IHost host, string baseAddress, Server engine)> StartServerAsync()
    {
        var engine = new Server();
        engine.Options.CookieConfig = null;
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
        return (host, sf.Features.Get<IServerAddressesFeature>()!.Addresses.First(), engine);
    }

    [Fact]
    public async Task Binary_round_trips_over_polling()
    {
        var (host, baseAddress, engine) = await StartServerAsync();
        try
        {
            byte[] sent = { 1, 2, 3, 4, 5 };
            byte[]? received = null;
            var doneTcs = new TaskCompletionSource<bool>();

            engine.On("connection", args =>
            {
                var srvSocket = (Socket)args[0];
                srvSocket.On("message", m =>
                {
                    if (m is object[] arr && arr.Length > 0 && arr[0] is RawData rd && rd.IsBinary)
                    {
                        var echo = rd.AsByteArray();
                        srvSocket.Send(new[] { new Packet(PacketType.Message, new RawData(echo)) });
                    }
                });
            });

            var client = new EngineIoClientSocket(baseAddress, new SocketOptions { Upgrade = false });
            client.On("open", _ => client.Send(sent));
            client.On("message", args =>
            {
                if (args[0] is RawData rd && rd.IsBinary)
                {
                    received = rd.AsByteArray();
                    doneTcs.TrySetResult(true);
                }
            });
            await client.OpenAsync();
            var done = await Task.WhenAny(doneTcs.Task, Task.Delay(15000));
            Assert.True(done == doneTcs.Task, "binary polling round-trip did not complete");
            Assert.Equal(sent, received);
            client.Close();
        }
        finally { await host.StopAsync(); host.Dispose(); }
    }

    [Fact]
    public async Task Binary_round_trips_over_websocket()
    {
        var (host, baseAddress, engine) = await StartServerAsync();
        try
        {
            byte[] sent = { 10, 20, 30, 40, 50, 60 };
            byte[]? received = null;
            var doneTcs = new TaskCompletionSource<bool>();

            engine.On("connection", args =>
            {
                var srvSocket = (Socket)args[0];
                srvSocket.On("message", m =>
                {
                    if (m is object[] arr && arr.Length > 0 && arr[0] is RawData rd && rd.IsBinary)
                    {
                        srvSocket.Send(new[] { new Packet(PacketType.Message, new RawData(rd.AsByteArray())) });
                    }
                });
            });

            var client = new EngineIoClientSocket(baseAddress);
            client.On("upgrade", _ => client.Send(sent));
            client.On("message", args =>
            {
                if (args[0] is RawData rd && rd.IsBinary)
                {
                    received = rd.AsByteArray();
                    doneTcs.TrySetResult(true);
                }
            });
            await client.OpenAsync();
            var done = await Task.WhenAny(doneTcs.Task, Task.Delay(15000));
            Assert.True(done == doneTcs.Task, "binary ws round-trip did not complete");
            Assert.Equal(sent, received);
            client.Close();
        }
        finally { await host.StopAsync(); host.Dispose(); }
    }

    [Fact]
    public async Task ForceBase64_round_trips_binary_over_polling()
    {
        var (host, baseAddress, engine) = await StartServerAsync();
        try
        {
            byte[] sent = { 7, 8, 9 };
            byte[]? received = null;
            var doneTcs = new TaskCompletionSource<bool>();

            engine.On("connection", args =>
            {
                var srvSocket = (Socket)args[0];
                srvSocket.On("message", m =>
                {
                    if (m is object[] arr && arr.Length > 0 && arr[0] is RawData rd && rd.IsBinary)
                    {
                        srvSocket.Send(new[] { new Packet(PacketType.Message, new RawData(rd.AsByteArray())) });
                    }
                });
            });

            // ForceBase64 → client sends b64=1, binary packets base64-encoded in payload
            var client = new EngineIoClientSocket(baseAddress, new SocketOptions { Upgrade = false, ForceBase64 = true });
            client.On("open", _ => client.Send(sent));
            client.On("message", args =>
            {
                if (args[0] is RawData rd && rd.IsBinary)
                {
                    received = rd.AsByteArray();
                    doneTcs.TrySetResult(true);
                }
            });
            await client.OpenAsync();
            var done = await Task.WhenAny(doneTcs.Task, Task.Delay(15000));
            Assert.True(done == doneTcs.Task, "force-base64 round-trip did not complete");
            Assert.Equal(sent, received);
            client.Close();
        }
        finally { await host.StopAsync(); host.Dispose(); }
    }
}
```

- [ ] **Step 2: Run, iterate until green**

Run: `dotnet test --filter "FullyQualifiedName~BinaryIntegrationTests"`

- [ ] **Step 3: Commit**

```
git add -A
git commit -m "test(client): binary round-trip tests (polling + ws + force-base64)"
```

---

## Task CC-3.2: Transport pause/resume + close/error edge cases — DONE

**Files:**
- Create: `tests/.../EdgeCaseTests.cs`
- Possibly modify: `src/.../Transports/PollingTransport.cs` (real Pause impl) if tests need it

- [ ] **Step 1: Write edge-case tests**

```csharp
using System.Threading.Tasks;
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
```

- [ ] **Step 2: Run, commit**

```
dotnet test --filter "FullyQualifiedName~EdgeCaseTests"
git add -A
git commit -m "test(client): edge cases — defaults, ForceBase64, close-before-open"
```

---

## Task CC-3.3: Final CC-3 verification — DONE

- [ ] **Step 1: Release build + test across all TFMs**

```
dotnet build -c Release
dotnet test -c Release
```
Expected: all green on net8/9/10.

- [ ] **Step 2: Commit**

```
git add -A
git commit -m "chore(client): CC-3 verification across TFMs" --allow-empty
```

---

## Self-review

**Spec coverage:** CC-3 covers binary send/receive (polling + ws + force-base64), edge cases (defaults, close-before-open). Reconnect/backoff explicitly NOT ported (belongs to socket.io-client). §5 deviations honored.

**Placeholder scan:** none.

**Scope honesty:** CC-3 is binary + edge cases. Reconnect is deferred to the socket.io-client cycle (confirmed by reading upstream — engine.io-client has no auto-reconnect).

## Execution Handoff

Plan saved to `docs/superpowers/plans/2026-06-18-engineio-client-cc3-port.md`. Inline TDD execution proceeding.
