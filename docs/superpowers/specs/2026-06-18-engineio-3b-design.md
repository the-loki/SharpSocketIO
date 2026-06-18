# Design Addendum: engine.io Sub-cycle 3B — ASP.NET Core / Kestrel HTTP integration

**Date:** 2026-06-18
**Scope:** Sub-cycle 3B of the engine.io port (see `2026-06-18-engineio-port-design.md`).
Wires the 3A pure-logic core to a real ASP.NET Core (Kestrel) HTTP server, ports
the HTTP round-trip subset of `test/server.js` to run against a live server via an
`HttpClient`-driven test driver.

---

## 1. Goal & success criteria (3B gate)

1. `Server.Attach(WebApplication app, AttachOptions?)` middleware wires engine.io
   onto an existing ASP.NET Core pipeline at `path` (default `/engine.io`).
2. `Server.Listen(int port, ...)` convenience creates+runs its own `WebApplication`.
3. The full handshake HTTP flow works against a real Kestrel server, exercised by
   an `HttpClient` test driver: GET handshake (returns `0{...sid...}` open packet +
   optional `Set-Cookie`), GET long-poll (returns queued packets or holds open),
   POST data ingest (accepts payload, replies `ok`), CORS preflight, gzip/deflate
   compression, JSONP wrap.
4. Ported `server.js` tests (handshake/verification/cookie/CORS/compression/
   register-client/exchange-handshake-data/sent-packet-with-handshake/polling-
   round-trips) pass against the live server. WebSocket upgrade is 3C.
5. `dotnet test` green on net8/9/10.
6. Work committed on the feature branch.

---

## 2. HttpContext mapping (the integration core)

Add an ASP.NET Core reference to the **test project only** (the library stays
framework-agnostic by mapping through the existing `IEngineRequest` abstraction —
no ASP.NET dependency in the library itself, keeping it reusable on any host).

Wait — that's wrong. The library *must* reference ASP.NET Core to provide
`Server.Attach(WebApplication)`. So:

- **`SharpSocketIO.EngineIo` library**: add `FrameworkReference Microsoft.AspNetCore.App`
  (net8/9/10 already include it). Provides `Server.Attach`, `HttpContextEngineRequest`.
- **Test project**: also references ASP.NET Core (via the lib) for `WebApplication`
  in tests + `HttpClient` driver.

| JS concept | C# (ASP.NET Core) |
|---|---|
| `req.url`, `req.method`, headers, query | `HttpContext.Request.Path`/`PathBase`/`Query`/`Method`/`Headers` |
| `req.connection.remoteAddress` | `HttpContext.Connection.RemoteIpAddress` |
| `res.writeHead(status, headers)` + `res.end(body)` | `Response.StatusCode`, `Response.Headers`, `await Response.WriteAsync(body)` |
| `req.on("data"/"end")` body | `await Request.BodyReader.ReadAsync` / `Request.Body.ReadAsync` |
| upgrade request (3C) | `context.WebSockets.IsWebSocketRequest` |

`HttpContextEngineRequest : IEngineRequest` adapts an `HttpContext` lazily (body
read on demand for POST). The polling transport holds the `HttpContext` for the
in-flight GET (long-poll) and completes the response when packets are flushed.

---

## 3. Request lifecycle (port of `server.ts` `handleRequest`)

For each request to `path`:
1. Build `IEngineRequest` from `HttpContext`.
2. Verify (3A `Server.Verify`); on error → HTTP 400 with `{code, message, context}` JSON,
   emit `connection_error`.
3. If `sid` query present → route to existing `Socket`'s transport (`transport.OnRequest`).
4. Else (handshake): `generateId`, create `Polling` transport + `Socket`, register
   client, write cookie if configured, send the `open` packet on the first poll.

### Polling GET (long-poll)
- If outbound buffer non-empty → flush immediately as `\x1e`-joined payload
  (force base64), `Content-Type: text/plain; charset=UTF-8`.
- Else hold the response open until packets arrive or `pingInterval`/close.
  Implement via `TaskCompletionSource<string>` per in-flight poll, completed on flush.

### Polling POST (data ingest)
- Read body (utf8), check `maxHttpBufferSize` (413 if exceeded), `decodePayload`,
  emit packets. Reply `ok` (200, `text/html`, body `ok`, Content-Length 2) — note
  the JS uses `text/html` to avoid download dialogs.

### Compression (port of `polling.ts` doWrite/compress)
- Only when `httpCompression` enabled AND any packet has `options.compress` AND
  length ≥ threshold (1024 default) AND `Accept-Encoding` includes gzip/deflate.
- `GzipStream`/`DeflateStream` over the payload bytes; set `Content-Encoding`.

### JSONP (port of `polling-jsonp.ts`)
- When `j` query present, wrap response as `___eio[<j>](<json>);`,
  `Content-Type: text/javascript; charset=UTF-8`. On POST ingest, parse the `d`
  form field (URL-decoded) instead of the raw body. Implement as a `JsonpPolling`
  subclass of `Polling`.

### CORS (port of `cors` option)
- Reflect `Access-Control-Allow-Origin` per `cors.origin` (default `*`),
  handle `OPTIONS` preflight with 204 + appropriate headers.

### Cookie (port of `cookie` option + handshake `Set-Cookie`)
- Default when `cookie: true`: name `io`, value `sid`, `Path=/; HttpOnly; SameSite=Lax`.
  Configurable name/path/httpOnly/sameSite. Use the 3A `CookieSerializer`.

---

## 4. TestDriver (HttpClient-based)

`tests/.../Commons/TestDriver.cs`:
- `StartServer(Action<ServerOptions> config, out int port)` — spins a `WebApplication`
  on an ephemeral port, attaches engine.io, returns a stop handle.
- `GetHandshake(port, query)` → `(sid, body, headers)` — does the initial polling GET.
- `Poll(port, sid, query)` → payload string (the long-poll).
- `Post(port, sid, payload)` → posts a payload, returns response.
- Uses `HttpClient` with `DefaultRequestHeaders`.

This is *not* the `engine.io-client` package (later cycle); it's a minimal driver
sufficient to exercise the server's polling surface against the same wire format
the real client uses.

---

## 5. File structure additions

```
src/SharpSocketIO.EngineIo/
  Http/
    HttpContextEngineRequest.cs   // IEngineRequest adapter over HttpContext
    ServerExtensions.cs           // Server.Attach(WebApplication) + Listen
  Transports/
    PollingHttp.cs                // HTTP-aware Polling (subclass; holds HttpContext, TCS long-poll)
    JsonpPolling.cs               // JSONP variant
  Commons/
    ServerOptions.cs              // extend: CorsOrigin → CorsOptions; add GenerateId hook
tests/SharpSocketIO.EngineIo.Tests/
  Commons/
    TestDriver.cs                 // HttpClient driver
    WebAppFactory.cs              // ephemeral-port Kestrel helper
  HttpServerTests.cs              // ← ported server.js round-trip subset
```

---

## 6. Behavior to preserve verbatim (3B subset)

- Handshake response body: `0{"sid":"...","upgrades":[...],"pingInterval":25000,"pingTimeout":20000,"maxPayload":1000000}`
  (open packet type `0` + JSON).
- POST reply: HTTP 200, `Content-Type: text/html`, body `ok`, `Content-Length: 2`.
- GET response: `Content-Type: text/plain; charset=UTF-8`, body = `\x1e`-joined
  payload (base64-forced binary).
- 400 error body: `{"code":<n>,"message":"<msg>","context":{...}}`.
- Default cookie: `io=<sid>; Path=/; HttpOnly; SameSite=Lax`.
- Compression: `Content-Encoding: gzip`/`deflate` only when negotiated.
- JSONP: `Content-Type: text/javascript; charset=UTF-8`, body `___eio[<j>](<json>);`.
- maxHttpBufferSize 413 on oversized POST.

---

## 7. Adaptations / deviations (3B)

1. **`WebApplication` not `http.Server`.** Idiomatic .NET; `Attach` is middleware.
2. **Long-poll hold via `TaskCompletionSource`.** No Node-style event loop; we
   `await` the TCS inside the request delegate.
3. **`accepts` → manual `Accept-Encoding` parse.** No npm dep.
4. **`cors` → simplified.** Full `cors` options delegate (function-form) deferred;
   3B supports origin-string + methods + credentials.
5. **`perMessageDeflate` not in 3B** (WebSocket-only → 3C).
6. **WebSocket upgrade is 3C** — requests with `transport=websocket` return 400
   in 3B (or 501). The `upgrades` field still advertises `websocket` so the wire
   format matches; the actual upgrade lands in 3C.

---

## 8. Test scope (ported from server.js)

The 3B test set covers (each is an HTTP round-trip via TestDriver):
- verification: unknown transport (400, code 0), `constructor` transport, unknown sid,
  `allowRequest` rejection, `__proto__` transport.
- handshake: send io cookie (default + custom name + custom path + path=false +
  httpOnly=true + sameSite=strict + httpOnly=false + not-boolean + no-cookie),
  register new client, exchange handshake data, custom ping timeouts, connection
  event, open with polling, no upgrades suggestion, arbitrary query data, bad
  request (handshake error), invalid origin, invalid method, unsupported protocol
  version, send packet along with handshake, support requests without trailing slash.
- close: server-initiated close over polling.
- messages: send/receive string + binary over polling round-trip.
- http compression: gzip + deflate negotiated.
- response headers, cors.

WebSocket-specific and ws-only tests are deferred to 3C.

---

## Next step

writing-plans → implementation plan for 3B, then TDD.
