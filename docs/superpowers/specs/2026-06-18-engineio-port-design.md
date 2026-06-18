# Design: Port `engine.io` (server transport layer) to .NET (C#)

**Date:** 2026-06-18
**Scope:** Third major cycle of the "port socket.io v4 monorepo to .NET" objective.
**Upstream source:** `packages/engine.io` v6.6.9 (~4,481 LoC lib + ~4,631 LoC test).

This is the first I/O-bound package. It is also by far the largest single port so
far. Per the brainstorming skill's decomposition guidance, this design splits
`engine.io` into **three sub-cycles** delivered against this one spec, because
the whole package is too large for a single coherent spec+plan+implementation pass.

---

## 1. Goal & success criteria

Port `engine.io` v6.6.9 as-is to idiomatic C#, multi-targeted to recent .NET,
preserving every applicable upstream test, test-first.

**Definition of done (whole cycle):**

1. A library project `SharpSocketIO.EngineIo` exposing `Server`, `Socket`,
   `Transport`, `Polling` + `WebSocket` transports, `attach`/`listen` helpers,
   the v3-compat parser, cookie/CORS handling, and re-exporting
   `SharpSocketIO.EngineIo.Parser`.
2. A test project `SharpSocketIO.EngineIo.Tests` with ported equivalents of:
   `test/parser.js`, the unit-testable portions of `test/server.js`
   (handshake/verification/lifecycle/cookie/CORS/compression/polling round-trips),
   `test/middlewares.js`, `test/engine.io.js`.
3. Integration tests run a real HTTP server (ASP.NET Core / Kestrel) and exercise
   it with an `HttpClient`-based polling driver and `ClientWebSocket` for upgrade.
   (The full `engine.io-client` package is a *separate* later cycle; we ship a
   minimal in-test driver, not a public client.)
4. `dotnet test` passes with all ported tests green across net8.0/net9.0/net10.0.
5. Wire-format compatibility: handshake `0{"sid":"...","upgrades":["websocket"],"pingInterval":25000,"pingTimeout":20000,"maxPayload":1000000}`,
   polling payload framing (separator `\x1e`), and the v4 + v3 protocol branches
   all reproduced.
6. Work committed to git on the feature branch.

### Sub-cycle decomposition

- **3A — Core + polling (no live server):** `Transport` base, `Socket` lifecycle
  (open/ping/pong/close, write buffer, send callbacks, upgrade state machine
  *logic*), `Server` handshake/verification/cookie/CORS *logic*, polling transport
  *logic* against an HTTP-abstraction interface. Tests: parser.js + unit-level
  server/socket/polling tests that don't need a live socket.
- **3B — ASP.NET Core integration:** Kestrel-backed `Server.Attach`, polling over
  real HTTP, compression, JSONP. Tests: the HTTP-round-trip subset of server.js
  via an `HttpClient` driver.
- **3C — WebSocket transport + upgrade:** `WebSocket` transport over
  `System.Net.WebSockets`, the polling→websocket upgrade handshake, ws-specific
  server.js tests. (v3-compat parser + WebTransport are deferred or omitted —
  WebTransport has no first-class .NET server primitive; v3 parser is ported
  because Socket.IO v2 compat is in scope, but only if 3A/3B land cleanly first.)

This design covers 3A in executable detail and sketches 3B/3C; each sub-cycle gets
its own implementation plan when reached.

---

## 2. Target frameworks & integration model

**Server target frameworks:** `net10.0; net9.0; net8.0`. ASP.NET Core ships
in-box for these. `netstandard2.1` is **dropped for the server package**
(ASP.NET Core doesn't target it, and an I/O server on legacy runtimes isn't a
"recent .NET platform" the objective targets). The pure-logic parsers already
ship `netstandard2.1` separately.

**HTTP integration: ASP.NET Core (Kestrel).** Mapping from Node's
`http.Server` / `IncomingMessage` / `ServerResponse`:

| Node | .NET (ASP.NET Core) |
|---|---|
| `createServer(handler)` / `server.listen(port)` | `WebApplication.CreateBuilder().Build()` + `app.Run(handler)` + `app.RunAsync()` |
| `IncomingMessage` (req) | `HttpContext.Request` (`HttpRequest`) |
| `ServerResponse` (res) | `HttpContext.Response` (`HttpResponse`) |
| `req.url`, `req.method`, `req.headers` | `Request.Path`/`Query`, `Method`, `Headers` |
| `res.writeHead(status, headers)` | `Response.StatusCode = ...; Response.Headers[...] = ...` |
| `res.end(body)` | await `Response.WriteAsync(body)` / `Response.Body.WriteAsync` |
| `server.on("upgrade", (req, socket, head))` | `app.UseWebSockets()` + `context.WebSockets.AcceptWebSocketAsync()` |
| `req.connection.remoteAddress` | `context.Connection.RemoteIpAddress` |
| `req.on("data"/"end"/"close")` | `await Request.Body.ReadAsync(...)` / cancellation / `RequestAborted` |

**Attachment modes:** JS `attach(httpServer, opts)` mutates an existing Node
server. In .NET we provide both:
- `Server.Attach(WebApplication app, AttachOptions)` — middleware-style attach
  (the idiomatic .NET path).
- `Server.Listen(port, opts)` — convenience that creates+runs its own
  `WebApplication` (mirrors JS `listen`).

**Why not `HttpListener`?** `HttpListener` is obsolete-ish, lacks first-class
WebSocket upgrade ergonomics, and isn't the "recent .NET platform" the objective
calls for. Kestrel is the supported, performant, modern choice.

---

## 3. Architecture & file structure

```
src/SharpSocketIO.EngineIo/
  SharpSocketIO.EngineIo.csproj
  EngineIo.cs                    // public re-exports + listen/attach helpers + protocol const
  Commons/
    AttachOptions.cs             // path, destroyUpgrade, destroyUpgradeTimeout, addTrailingSlash
    ServerOptions.cs             // pingTimeout, pingInterval, upgradeTimeout, maxHttpBufferSize,
                                 // allowRequest, transports, allowUpgrades, perMessageDeflate,
                                 // httpCompression, wsEngine, cors, cookie, allowEIO3, initialPacket
    CookieOptions.cs             // CookieSerializeOptions port
    ReadyState.cs                // "opening"|"open"|"closing"|"closed"
    EngineRequest.cs             // HttpContext-wrapper view the transport sees
    ErrorCodes.cs                // Server.errors (TransportUnknown, Forbidden, BadRequest, ...)
    CorsOptions.cs               // cors options port
  Contrib/
    Base64Id.cs                  // random session-id generator (port of contrib/base64id)
    CookieSerializer.cs          // cookie serialize/parse (port of cookie pkg)
  ParserV3/
    Index.cs                     // port of parser-v3/index.ts (v3 compat codec)
    Utf8.cs                      // port of parser-v3/utf8.ts
  Transport.cs                   // abstract Transport : Emitter
  Socket.cs                      // Socket : Emitter (session lifecycle)
  Server.cs                      // Server : BaseServer : Emitter (handshake/verify/route)
  Transports/
    Index.cs                     // transport registry
    Polling.cs                   // HTTP long-polling transport
    PollingJsonp.cs              // JSONP variant
    WebSocket.cs                 // WebSocket transport
    WebTransport.cs              // stub/omit (no .NET primitive) — see §6
  Http/
    IHttpServerAdapter.cs        // abstraction over WebApplication/HttpListener
    KestrelServerAdapter.cs      // ASP.NET Core implementation
tests/SharpSocketIO.EngineIo.Tests/
  Commons/
    TestDriver.cs                // HttpClient + ClientWebSocket polling/ws driver (NOT a public client)
    PartialDone.cs               // createPartialDone port
  ParserV3Tests.cs               // ← test/parser.js (v3 compat)
  SocketTests.cs                 // unit tests for Socket lifecycle logic
  ServerTests.cs                 // ← unit-testable portions of test/server.js
  PollingTests.cs                // ← polling round-trips
  HandshakeTests.cs              // handshake/verification (no live server where possible)
  WebsocketTests.cs              // ← ws subset of test/server.js
  MiddlewaresTests.cs            // ← test/middlewares.js
```

---

## 4. Type & API mapping (key types)

### Transport (abstract, port of `lib/transport.ts`)
- `abstract class Transport : Emitter<UnitEvents>` with `string Sid`, `bool Writable`,
  `int Protocol` (3 or 4), `ReadyState ReadyState`, `bool Discarded`.
- Methods: `Discard()`, `OnRequest(EngineRequest)`, `Close(Action? callback)`,
  abstract `string Name`, abstract `Send(IReadOnlyList<Packet>)`, abstract `DoClose(Action?)`.
- Protected: `OnError`, `OnPacket`, `OnData`, `OnClose`.
- Protocol selection: `Protocol = req.Query["EIO"] == "4" ? 4 : 3`; parser swapped
  between `SharpSocketIO.EngineIo.Parser` (v4) and the local `ParserV3`.

### Socket (port of `lib/socket.ts`, 598 LoC)
The session state machine. Fields: `Id`, `Server`, `Transport`, `Protocol`,
`RemoteAddress`, `ReadyState`, write buffer, send-callback queues, ping/interval
timers. Key methods: `Send`/`Write` (buffer packets), `Close`, upgrade flow
(`SetTransport`, `MaybeUpgrade`), `OnError`, `OnPacket` (handles ping/pong/close/
message/upgrade), `OnOpen`. Mirrors the Node timer semantics (`pingInterval`/
`pingTimeout`) using `System.Threading.Timer`.

### Server (port of `lib/server.ts`, 1134 LoC)
The engine. Holds clients dictionary, generates SIDs (`Base64Id`), handles
`/engine.io/` route, runs verification (`AllowRequest`), sets cookies, manages
CORS, drives the handshake `open` packet, dispatches to transports, runs the
upgrade flow. `GenerateId` is a port of base64id. `Attach(WebApplication)` wires
the middleware; `Close()` shuts down.

### Polling transport (port of `lib/transports/polling.ts`, 396 LoC)
GET = long-poll (wait for data, flush write buffer as a `\x1e`-joined payload),
POST = client→server data (parse payload, emit packets). Compression via
`System.IO.Compression.GzipStream`/`DeflateStream`. Pause/resume semantics for
upgrade. HTTP-abstraction-backed so unit-testable without Kestrel in 3A.

### WebSocket transport (port of `lib/transports/websocket.ts`, 113 LoC)
Wraps `System.Net.WebSockets.WebSocket`. `Send` writes each packet via
`engine.io-parser`'s binary encoder stream framing. Receives via a read loop.
Close handshake. (Sub-cycle 3C.)

---

## 5. Behavior to preserve verbatim (correctness contract)

- **Handshake response** (`open` packet, JSON): `{ sid, upgrades, pingInterval,
  pingTimeout, maxPayload }` — exact keys and defaults (25000/20000/1000000).
- **Route:** `path` (default `/engine.io`), `addTrailingSlash` (default true →
  `/engine.io/`).
- **Verification errors** with exact codes/messages: `0` Transport unknown,
  `1` Forbidden (Origin), `2` Bad handshake method, `3` Bad request. All surface
  as HTTP 400 with `{ code, message, ...context }` JSON body and a
  `connection_error` server event.
- **Polling framing:** packets joined by `\x1e`; binary forced base64 in payload.
- **Cookies:** `io` cookie (name configurable) with `CookieSerializeOptions`,
  only set when `cookie` option present.
- **CORS:** `cors` options delegate; preflight handling.
- **Upgrade:** polling→websocket via `upgrade` packet echo + `noop` probe; only
  one upgrade at a time; `upgradeTimeout` default 10s.
- **Ping/pong:** server sends `ping` every `pingInterval`; client must `pong`
  within `pingTimeout` or socket closes.
- **maxHttpBufferSize:** reject bodies larger than 1MB (default) → close session.
- **v3 compat (`allowEIO3`):** `EIO=3` requests use `ParserV3`; v3 open packet
  format differs.

---

## 6. Adaptations & explicit deviations

1. **WebTransport omitted.** No first-class .NET WebTransport server primitive
   on the targeted frameworks (the API is experimental/partial). `WebTransport`
   transport and its tests are omitted; a stub type may exist for API parity.
2. **uWebSockets.js / eiows variants omitted.** These are alternate WS engines;
   .NET uses `System.Net.WebSockets` (Kestrel's built-in). `transports-uws/` and
   `userver.ts` are not ported.
3. **Full `engine.io-client` not ported in this cycle.** Test integration uses a
   minimal internal `TestDriver` (HttpClient + ClientWebSocket). The real client
   package is its own later cycle.
4. **No `netstandard2.1` for the server.** ASP.NET Core doesn't target it.
5. **`debug` module → `ILogger`.** Replace the `debug("engine:...")` calls with
   `Microsoft.Extensions.Logging` categories (`"EngineIo.Transport"`, etc.).
6. **EventEmitter → our `Emitter<TEvents>`.** Transport/Socket/Server extend the
   already-ported `SharpSocketIO.ComponentEmitter.Emitter<TEvents>` (string-keyed
   dispatch matches JS).
7. **Node streams/zlib → `System.IO.Compression`.** `createGzip`/`createDeflate`
   → `GzipStream`/`DeflateStream`; `accepts` content-encoding negotiation →
   manual `Accept-Encoding` parse.
8. **`process.nextTick`/`setImmediate` → `Task.Run`/`ThreadPool.QueueUserWorkItem`
   or just direct invoke.** Most are microtask deferrals; in .NET we run inline
   unless a test specifically needs deferral.
9. **Timer semantics:** `setTimeout`/`setInterval` → `System.Threading.Timer`
   with the same one-shot/periodic semantics and `change`/`dispose`.

---

## 7. Sub-cycle 3A scope (executable now)

**Delivered in 3A:**
- `SharpSocketIO.EngineIo` project (net8/9/10) referencing the parser + emitter.
- `Contrib/Base64Id`, `Contrib/CookieSerializer`, `Commons/*` option/cookie/error types.
- `ParserV3` (Index + Utf8) — v3 compat codec, with ported `test/parser.js`.
- `Transport` abstract base (pure logic, no I/O).
- `Socket` lifecycle logic (state machine, write buffer, ping/pong timers,
  upgrade logic) — testable by injecting a fake transport.
- `Server` handshake/verification/routing logic — testable by injecting a fake
  HTTP request representation and asserting response decisions.
- `Polling` transport logic against an `IHttpContext` abstraction (not yet wired
  to Kestrel — that's 3B).
- Tests: `ParserV3Tests` (full parser.js), `SocketTests` (lifecycle unit tests),
  `HandshakeTests`/`ServerTests` (verification logic), `PollingTests` (logic).

**Explicitly deferred to 3B/3C:** live Kestrel server, real HTTP round-trips,
WebSocket transport, upgrade, JSONP, compression over the wire, middlewares.js.

### 3A success criteria (the gate for this cycle's commit)
- Project builds clean on net8/9/10, 0 warnings (treat-as-error).
- All `test/parser.js` cases ported and green (v3 codec parity).
- Socket lifecycle: a fake-transport-driven test exercises open → ping/pong →
  message → close and matches JS behavior.
- Server verification: every error code (0/1/2/3) and the happy-path handshake
  `open` packet produced correctly for representative inputs.
- Polling: GET flush and POST ingest logic correct against the abstraction.

---

## 8. Risks

- **HTTP abstraction leakage.** The `IHttpContext`-style abstraction in 3A must be
  close enough to ASP.NET Core's `HttpContext` that 3B wiring is mechanical, not a
  redesign. Mitigation: model it on `HttpContext`'s shape from the start.
- **Timer/threading correctness.** JS is single-threaded; .NET timers fire on
  thread-pool threads. `Socket` state must be guarded (lock or
  `ConcurrencyMode`). Mitigation: per-socket lock around state transitions;
  tests for concurrent ping/close.
- **v3 parser complexity.** 485+210 LoC of legacy codec. Mitigation: port
  mechanically, test via the existing parser.js.
- **Test driver fidelity.** The internal `TestDriver` must replicate enough of
  `engine.io-client`'s polling/ws behavior for round-trip tests to be meaningful.
  Mitigation: keep it minimal and focused on the wire format we already proved in
  cycle 1.

---

## Next step

writing-plans → implementation plan for sub-cycle 3A (executable now). 3B/3C get
their own plans when 3A is green.
