# Design: Port `engine.io-client` v6.6.6 to .NET (C#)

**Date:** 2026-06-18
**Scope:** Fifth package ported. Client-side counterpart to `engine.io`.
**Upstream source:** `packages/engine.io-client` v6.6.6 (~2,739 LoC lib + ~2,014 LoC test).

This is the first *client* package. Built on the already-ported
`SharpSocketIO.EngineIo.Parser` (wire codec) and `SharpSocketIO.ComponentEmitter`.

---

## 1. Goal & success criteria

Port `engine.io-client` as-is to idiomatic C# (net8/9/10), preserving every
applicable upstream test (ported to xUnit), test-first.

**Definition of done:**

1. `SharpSocketIO.EngineIo.Client` library exposing `EngineIoClientSocket`
   (port of `lib/socket.ts`), `Transport` base, `Polling`/`XHR`/`WebSocket`/`WebTransport`
   transports (WebTransport stubbed — no .NET primitive), `parse(uri)`, `transports`
   registry, protocol const, `SocketOptions`.
2. Full client lifecycle: open (handshake), upgrade polling→websocket, send/receive
   string + binary, ping/pong, reconnect, close.
3. Tests: `parseuri.js` (full), `socket.js`/`transport.js`/`connection.js`/`node.js`
   unit-testable subsets ported; integration tests run the client against the
   already-ported `SharpSocketIO.EngineIo` server (real Kestrel).
4. `dotnet test` green on net8/9/10.
5. Wire format compatible with the engine.io server (proven end-to-end: client ↔
   our server round-trip).

### Sub-cycle decomposition (mirrors engine.io)

- **CC-1 — Core client + polling over HttpClient:** `parseuri`, `parseqs`, `util`,
  `Transport` base, `EngineIoClientSocket` lifecycle (open/probe/open-handshake/
  send/receive/ping/pong/close/reconnect *logic*), `Polling` transport using
  `HttpClient`. Tests: `parseuri.js` + `socket.js` logic subset + a real-server
  polling round-trip.
- **CC-2 — WebSocket transport + upgrade:** `WebSocket` transport via
  `ClientWebSocket`, the polling→websocket upgrade on the client side, ws-only
  connect. Tests: ws subset of connection.js/socket.js.
- **CC-3 — binary + edge cases:** ArrayBuffer/binary send/receive, reconnect,
  ping timeouts, `transport.js` pause/resume logic. (WebTransport stubbed/omitted.)

This design covers CC-1 in executable detail; CC-2/CC-3 get their own plans.

---

## 2. Target frameworks & integration model

- **`net10.0; net9.0; net8.0`** (no netstandard2.1 — client uses HttpClient +
  ClientWebSocket which are best on modern runtimes; matches the engine.io server
  target set).
- Polling: `System.Net.Http.HttpClient` (one-per-socket, pooled).
- WebSocket: `System.Net.WebSockets.ClientWebSocket`.
- `debug` → omitted (or trivial `ILogger` later).
- `@socket.io/component-emitter` → `SharpSocketIO.ComponentEmitter`.
- `engine.io-parser` → `SharpSocketIO.EngineIo.Parser`.

---

## 3. Type & API mapping (JS → C#)

| JS | C# |
|---|---|
| `class Socket extends Emitter<...>` (lib/socket.ts) | `class EngineIoClientSocket : Emitter<UnitEvents>` with `Open`/`Close`/`Send`/`OnPacket` and `SocketOptions`. |
| `protocol = 4` (from engine.io-parser) | `public const int Protocol = 4;` |
| `SocketOptions` | `SocketOptions` class: Host, Hostname, Secure, Port, Query, Upgrade (default true), ForceBase64, TimestampParam ("t"), TimestampRequests, Transports (["polling","websocket"]), Path ("/engine.io"), RememberUpgrade, etc. |
| `Transport extends Emitter` | `Transport : Emitter<UnitEvents>` abstract with `Open`/`Close`/`Send`/`Pause`, `OnOpen`/`OnData`/`OnPacket`/`OnClose`/`OnError`, abstract `Name`/`DoOpen`/`DoClose`/`Write`. |
| `Polling extends Transport` (polling-xhr) | `PollingTransport : Transport` using `HttpClient` for GET (long-poll) + POST (send). |
| `XHR`/`Fetch` | merged into `PollingTransport` (HttpClient covers both). |
| `WS extends Transport` | `WebSocketTransport : Transport` using `ClientWebSocket`. (CC-2) |
| `parse(str)` (parseuri) | `EngineIoUri.Parse(string)` returning `EngineIoUri` (protocol/host/port/path/query/secure). Or reuse `System.Uri` + `UriBuilder`; the JS parser handles ws:// schema and IPv6. We'll port it for parity. |
| `parseqs` encode/decode | helpers `Parseqs.Encode(dict)` / `Parseqs.Decode(str)`. |
| `byteLength` (util) | UTF-8 byte length via `Encoding.UTF8.GetByteCount`. |
| `createCookieJar` | `System.Net.CookieContainer` on `HttpClientHandler`. |

`EngineIoClientSocket` lifecycle port (lib/socket.ts):

1. `new Socket(uri, opts)` → parse uri, merge opts, set readyState=opening.
2. `open()` → `OpenSubTransport` → create first transport (polling by default),
   open it; on transport `open` → send handshake probe (`GET` returns open packet),
   parse sid + upgrades; emit `open`; schedule ping; maybe probe upgrades.
3. `send(data)` → encode packet → `transport.Send`.
4. `OnPacket(packet)` → emit `packet`/`message`; handle `open`/`ping`/`pong`/`close`/`message`/`noop`/`upgrade`.
5. `close()` → send close packet, transport close, emit `close`.
6. Upgrade: client opens a probe — on the new transport send `ping`/`probe`, await
   `pong`/`probe`, then `pause` polling, send `upgrade` packet, swap transport. (CC-2)
7. Reconnect: on close/error, `SocketOptions.Reconnection` (default true) →
   backoff retry. (CC-3)

---

## 4. Behavior to preserve verbatim

- Handshake: client `GET {path}?EIO=4&transport=polling` → server returns
  `0{sid,upgrades,pingInterval,pingTimeout,maxPayload}`. Client stores sid, emits
  `open`, uses `upgrades` to drive upgrade probes.
- Subsequent polling: `GET {path}?EIO=4&transport=polling&sid={sid}` (long-poll);
  `POST {path}?...&sid={sid}` with payload body for sends.
- Packet framing: polling payloads are `\x1e`-joined; binary forced base64.
- Ping/pong: client must respond to server `ping` with `pong` (same data) within
  `pingTimeout`, else close.
- Query string: client appends `EIO=4`, `transport=`, `sid=` (after handshake),
  plus user query + optional `t=<timestamp>`.
- URI parsing: ws/wss/http/https schemas; IPv6 hosts; query + path preserved.

---

## 5. Adaptations / deviations

1. **HttpClient for polling.** Single client per socket, configured with a
   `CookieContainer` for `withCredentials` parity. No XHR/Fetch split.
2. **ClientWebSocket for ws.** CC-2.
3. **WebTransport omitted.** No .NET client primitive; `transports["webtransport"]`
   not registered.
4. **`installTimerFunctions`/`nextTick`** → trivial wrappers (`Task.Delay`,
   `Task.Run`); timer hijack for testing via an injected `ITimer` if needed.
5. **Browser-only entrypoints (`browser-entrypoint`, globals.ts) omitted.** This is
   a server-side/.NET client; no `window`/`XMLHttpRequest`/`Worker`.
6. **No `debug` module.** Omit logging calls.
7. **`agent`/`pfx`/`rejectUnauthorized`/TLS options** → mapped to
   `HttpClientHandler`/`ClientWebSocketOptions` equivalents where applicable; full
   parity deferred.

---

## 6. Sub-cycle CC-1 scope (executable now)

- `SharpSocketIO.EngineIo.Client` project (net8/9/10) referencing EngineIo.Parser +
  ComponentEmitter.
- `Contrib/EngineIoUri.cs` (port of parseuri) + `Contrib/Parseqs.cs` + `Util.cs`.
- `Transport.cs` abstract (open/close/send/pause, packet/error/close events).
- `PollingTransport.cs` (HttpClient GET long-poll + POST send).
- `EngineIoClientSocket.cs` lifecycle (open/handshake/send/receive/ping/pong/close).
- Tests: `EngineIoUriTests` (← parseuri.js), `ParseqsTests`, plus a real-server
  integration test: client opens against our engine.io server, receives open,
  sends a message, server receives it.

### CC-1 success gate
- parseuri.js full parity.
- Client opens against a `SharpSocketIO.EngineIo` real Kestrel server via polling,
  receives the `open` packet, send/receive a string message round-trip.
- Lifecycle unit tests for send/close/ping-pong.

---

## 7. Risks

- **Async lifecycle.** JS is single-threaded with event loop; the C# client must
  serialize state transitions (lock around readyState/packets). Mitigation:
  per-socket lock; careful `await` ordering.
- **Long-poll concurrency.** HttpClient GET that holds open + concurrent POST;
  must not deadlock. Mitigation: separate HttpClient or per-request.
- **Cookie/session continuity.** `sid` query param carries the session (cookies
  are secondary). Tests assert sid-based polling works.

---

## Next step

writing-plans → CC-1 plan, then TDD.
