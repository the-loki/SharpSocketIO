# socket.io server Sub-cycle 5B — Live server integration

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Checkbox steps.

**Goal:** Wire the 5A socket.io logic to a live Kestrel server via `SharpSocketIO.EngineIo`. Add `EngineIoSocketConnection` (adapts `SharpSocketIO.EngineIo.Socket` → `IEngineIoConnection`), `Server.Attach(engineIoServer)` (creates a socket.io Client per engine.io connection), and end-to-end integration tests using `SharpSocketIO.EngineIo.Client` as the transport.

**Architecture:** socket.io `Server.Attach(engine)` subscribes to the engine.io server's `connection` event; for each `EngineIo.Socket`, wraps it in `EngineIoSocketConnection : IEngineIoConnection` and creates a socket.io `Client`. The Client's Decoder receives engine.io `message` packets (which carry socket.io packets) and routes them to namespaces. End-to-end: a `SharpSocketIO.EngineIo.Client` connects to the engine.io server, sends socket.io CONNECT/EVENT packets as engine.io messages, the socket.io server processes them and replies.

**Tech Stack:** .NET 8/9/10, ASP.NET Core (via engine.io), xUnit.

**Reference:** `_upstream/packages/socket.io/lib/index.ts` (`Server.attach`).

---

## Task 5B-1: Add engine.io dependency + `EngineIoSocketConnection` adapter — DONE

**Files:**
- Modify: `src/SharpSocketIO.SocketIo/SharpSocketIO.SocketIo.csproj` (add EngineIo ref)
- Create: `src/SharpSocketIO.SocketIo/EngineIoSocketConnection.cs`

- [ ] **Step 1: Add EngineIo project reference**

In the library csproj, add:
```xml
    <ProjectReference Include="..\SharpSocketIO.EngineIo\SharpSocketIO.EngineIo.csproj" />
```

- [ ] **Step 2: Implement `EngineIoSocketConnection`**

```csharp
using System.Net;
using SharpSocketIO.ComponentEmitter;
using SharpSocketIO.EngineIo;
using SharpSocketIO.EngineIo.Parser.Commons;
using EioPacketType = SharpSocketIO.EngineIo.Parser.Commons.PacketType;

namespace SharpSocketIO.SocketIo;

/// <summary>
/// Adapts a SharpSocketIO.EngineIo.Socket (the server-side engine.io session) to the
/// IEngineIoConnection abstraction the socket.io Client consumes. engine.io "message"
/// packets become "data" events (string payload); socket.io sends become engine.io
/// "message" packets.
/// </summary>
public sealed class EngineIoSocketConnection : IEngineIoConnection
{
    private readonly EngineIo.Socket _socket;
    private readonly Emitter _events = new();

    public EngineIoSocketConnection(EngineIo.Socket socket)
    {
        _socket = socket;
        Id = socket.Id;
        Protocol = socket.Protocol;
        Handshake = new Handshake
        {
            Id = socket.Id,
            // engine.io Socket carries remote address via Transport; best-effort
            RemoteAddress = null,
        };

        socket.On("message", args =>
        {
            if (args.Length > 0 && args[0] is RawData rd)
            {
                // engine.io message packet data is the socket.io packet string
                var text = rd.Kind == RawDataKind.String ? rd.AsString() : null;
                if (text != null) _events.Emit("data", text);
            }
        });
        socket.On("close", args => _events.Emit("close", args.Length > 0 ? args[0] : DisconnectReasons.TransportClose));
        socket.On("error", args => _events.Emit("error", args));
    }

    public string Id { get; }
    public int Protocol { get; }
    public Handshake Handshake { get; }
    public Emitter Events => _events;

    public void Send(string encodedPayload)
    {
        // send as an engine.io "message" packet (type 4) carrying the socket.io packet
        _socket.Send(new[] { new EngineIo.Parser.Commons.Packet(EioPacketType.Message, new RawData(encodedPayload)) });
    }

    public void Close(bool discard = false) => _socket.Close();
}
```

- [ ] **Step 3: Build library**

```
dotnet build src/SharpSocketIO.SocketIo/SharpSocketIO.SocketIo.csproj
```

- [ ] **Step 4: Commit**

```
git add -A
git commit -m "feat(socketio): EngineIoSocketConnection adapter — EngineIo.Socket → IEngineIoConnection"
```

---

## Task 5B-2: `Server.Attach(engine)` + handshake wiring — DONE

**Files:**
- Modify: `src/SharpSocketIO.SocketIo/Server.cs`

- [ ] **Step 1: Add `Attach` to Server**

```csharp
    /// <summary>Attaches the socket.io server to an engine.io server.</summary>
    public Server Attach(SharpSocketIO.EngineIo.Server engine, ServerOptions? opts = null)
    {
        if (opts != null) Options = opts;
        engine.On("connection", args =>
        {
            var engineSocket = (SharpSocketIO.EngineIo.Socket)args[0];
            var conn = new EngineIoSocketConnection(engineSocket);
            CreateClient(conn);
        });
        return this;
    }
```

- [ ] **Step 2: Build, commit**

```
dotnet build src/SharpSocketIO.SocketIo/SharpSocketIO.SocketIo.csproj
git add -A
git commit -m "feat(socketio): Server.Attach — subscribe to engine.io connection events"
```

---

## Task 5B-3: End-to-end integration tests — DONE

**Files:**
- Modify: `tests/SharpSocketIO.SocketIo.Tests/SharpSocketIO.SocketIo.Tests.csproj` (add EngineIo + EngineIo.Client refs)
- Create: `tests/.../SocketIoIntegrationTests.cs`

- [ ] **Step 1: Add project references to the test project**

```xml
  <ItemGroup>
    <ProjectReference Include="..\..\src\SharpSocketIO.SocketIo\SharpSocketIO.SocketIo.csproj" />
    <ProjectReference Include="..\..\src\SharpSocketIO.EngineIo\SharpSocketIO.EngineIo.csproj" />
    <ProjectReference Include="..\..\src\SharpSocketIO.EngineIo.Client\SharpSocketIO.EngineIo.Client.csproj" />
  </ItemGroup>
```

- [ ] **Step 2: Write the integration tests**

```csharp
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SharpSocketIO.EngineIo;
using SharpSocketIO.EngineIo.Client;
using SharpSocketIO.EngineIo.Http;
using SharpSocketIO.EngineIo.Parser.Commons;
using Xunit;

namespace SharpSocketIO.SocketIo.Tests;

public class SocketIoIntegrationTests
{
    private static async Task<(IHost host, string baseAddress, Server io, EngineIo.Server engine)> StartAsync()
    {
        var engine = new EngineIo.Server();
        engine.Options.CookieConfig = null;
        var io = new Server();
        io.Attach(engine);
        var builder = Host.CreateDefaultBuilder()
            .ConfigureWebHost(web =>
            {
                web.UseKestrel();
                web.UseUrls("http://127.0.0.1:0");
                web.Configure(app => ServerExtensions.Attach(engine, app));
            });
        var host = builder.Build();
        await host.StartAsync();
        var sf = host.Services.GetRequiredService<IServer>();
        return (host, sf.Features.Get<IServerAddressesFeature>()!.Addresses.First(), io, engine);
    }

    private static async Task SendSocketIoPacketAsync(EngineIoClientSocket client, string encoded)
    {
        // an engine.io "message" packet (type 4) carrying a socket.io packet
        client.Send(encoded);
    }

    [Fact]
    public async Task Socket_io_client_connects_to_default_namespace()
    {
        var (host, baseAddress, io, engine) = await StartAsync();
        try
        {
            bool connectionFired = false;
            io.Of("/").On("connection", _ => connectionFired = true);

            // use engine.io-client as the transport; open it, then send a socket.io CONNECT ("0")
            var transport = new EngineIoClientSocket(baseAddress, new EngineIo.Client.SocketOptions { Upgrade = false });
            var openTcs = new TaskCompletionSource<bool>();
            transport.On("open", _ => openTcs.TrySetResult(true));
            await transport.OpenAsync();
            var opened = await Task.WhenAny(openTcs.Task, Task.Delay(15000));
            Assert.True(opened == openTcs.Task, "engine.io transport did not open");

            // send socket.io CONNECT packet (type 0) for namespace "/"
            transport.Send("0");

            await Task.Delay(500);
            Assert.True(connectionFired, "socket.io connection event did not fire");
            transport.Close();
        }
        finally { await host.StopAsync(); host.Dispose(); }
    }

    [Fact]
    public async Task Server_emits_event_reaches_client()
    {
        var (host, baseAddress, io, engine) = await StartAsync();
        try
        {
            var transport = new EngineIoClientSocket(baseAddress, new EngineIo.Client.SocketOptions { Upgrade = false });
            await OpenAsync(transport);

            string? receivedEvent = null;
            // capture socket.io packets arriving as engine.io messages on the client
            transport.On("message", args =>
            {
                if (args[0] is RawData rd)
                {
                    var text = rd.AsString() ?? "";
                    if (text.StartsWith("2")) receivedEvent = text; // EVENT packet
                }
            });

            io.Of("/").On("connection", args =>
            {
                var s = (Socket)args[0];
                s.Emit("hello", "world");
            });
            transport.Send("0"); // CONNECT
            await Task.Delay(800);
            Assert.NotNull(receivedEvent);
            Assert.Contains("hello", receivedEvent);
            Assert.Contains("world", receivedEvent);
            transport.Close();
        }
        finally { await host.StopAsync(); host.Dispose(); }
    }

    private static async Task OpenAsync(EngineIoClientSocket transport)
    {
        var openTcs = new TaskCompletionSource<bool>();
        transport.On("open", _ => openTcs.TrySetResult(true));
        await transport.OpenAsync();
        await Task.WhenAny(openTcs.Task, Task.Delay(15000));
    }
}
```

- [ ] **Step 3: Run, iterate until green**

Run: `dotnet test --filter "FullyQualifiedName~SocketIoIntegrationTests"`

- [ ] **Step 4: Commit**

```
git add -A
git commit -m "test(socketio): 5B — end-to-end integration (engine.io-client transport → socket.io server)"
```

---

## Task 5B-4: Final 5B verification — DONE

- [ ] **Step 1: Release build + test across all TFMs**

```
dotnet build -c Release
dotnet test -c Release
```

- [ ] **Step 2: Commit**

```
git add -A
git commit -m "chore(socketio): 5B verification across TFMs" --allow-empty
```

---

## Self-review

**Spec coverage:** 5B covers §6 cycle-5B items: EngineIoSocketConnection adapter, Server.Attach, end-to-end integration. The bridge: engine.io message packet (RawData string) → socket.io Client "data" → Decoder → namespace routing.

**Placeholder scan:** none.

**Type consistency:** `EngineIoSocketConnection : IEngineIoConnection` wraps `SharpSocketIO.EngineIo.Socket`. `Server.Attach(EngineIo.Server)`. Integration tests use `SharpSocketIO.EngineIo.Client.EngineIoClientSocket` as the transport. `ServerExtensions.Attach(engine, app)` from engine.io 3B.

**Scope honesty:** 5B is live-server integration. BroadcastOperator/RemoteSocket/ParentNamespace/CSR are 5C.

## Execution Handoff

Plan saved to `docs/superpowers/plans/2026-06-18-socketio-5b-port.md`. Inline TDD execution proceeding.
