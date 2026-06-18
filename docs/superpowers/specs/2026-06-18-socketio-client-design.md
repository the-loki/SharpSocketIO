# Design + Plan: Port `socket.io-client` v4.8.3 to .NET (C#)

**Date:** 2026-06-18
**Scope:** Eighth cycle. The socket.io client (Manager + namespace Sockets + reconnect).
**Upstream source:** `packages/socket.io-client` v4.8.3 (~2,093 LoC lib + ~2,623 LoC test).

Built on the already-ported: `SharpSocketIO.EngineIo.Client` (transport), `SharpSocketIO.SocketIo.Parser`,
`SharpSocketIO.ComponentEmitter`.

---

## 1. Goal & success criteria

Port as-is to idiomatic C# (net8/9/10), preserving applicable upstream tests, test-first.

**Definition of done:**

1. `SharpSocketIO.SocketIo.Client` library: `Manager` (wraps an engine.io-client engine,
   reconnect with backoff, namespace dictionary), `SocketIoClientSocket` (per-namespace:
   on/emit/ack, connect/disconnect, active flag), `Url` (parse), `Backoff` (exponential
   backoff for reconnect).
2. End-to-end: socket.io-client connects to our `SharpSocketIO.SocketIo` server over a
   real Kestrel engine.io server, joins a namespace, emits events with ack, server
   replies — verified by integration tests.
3. `dotnet test` green on net8/9/10.

### Sub-cycle decomposition

- **SC-1 — Manager + Socket (logic + reconnect):** `Manager` (engine lifecycle, reconnect
  with `Backoff`, namespace dict), `SocketIoClientSocket` (connect/disconnect/on/emit/ack/
  reconnect-state), `Url`, `Backoff`. Tests: url.js, retry.js, socket.ts logic subset.
- **SC-2 — Live integration:** end-to-end against our socket.io server.

This is small enough (2,093 LoC) for two sub-cycles.

---

## 2. Type & API mapping

| JS | C# |
|---|---|
| `class Manager extends Emitter` (manager.ts, 634 LoC) | `class Manager : Emitter<UnitEvents>` — wraps `SharpSocketIO.EngineIo.Client.EngineIoClientSocket` (the engine), owns `Decoder`/`Encoder`, namespace dict, reconnect with `Backoff`. |
| `class Socket extends Emitter` (socket.ts, 1157 LoC) | `class SocketIoClientSocket : Emitter<UnitEvents>` — per-namespace: `Connect`/`Disconnect`, `On`/`Emit`/`EmitWithAck`, `Id`, `Connected`, `Active`, ack callbacks, reconnect handling. |
| `Backoff` (contrib/backo2.ts) | `class Backoff` — `Duration`, `Reset`, `Increase` (exponential + jitter). |
| `url(uri, opts)` | `Url.Parse(uri, opts)` — host/path/query extraction. |
| `reconnection` (ManagerOptions) | `ManagerOptions.Reconnection` etc. — and this is where the engine.io-client reconnect lives (confirmed earlier). |
| Packet flow: engine "open" → Manager → Socket CONNECT → namespace open | `Manager` listens engine "open"/"packet"; routes socket.io packets to namespace Sockets via the Decoder. |

Packet flow (client side):
1. `Manager` opens the engine.io-client engine (`OpenAsync`).
2. On engine "open", for each namespace Socket, send a socket.io CONNECT packet.
3. Engine "message" packets → `Decoder` → socket.io packets → route to namespace Socket
   by `packet.Nsp`.
4. Socket handles CONNECT (open), EVENT (emit to handlers / ack), ACK (ack callback),
   DISCONNECT, CONNECT_ERROR.
5. On engine close/error → reconnect (if `Reconnection`): backoff timer → re-open engine.

---

## 3. Adaptations / deviations

1. **Typed-events generics simplified** to string-keyed dispatch.
2. **`debug` omitted.**
3. **`multiplex` (cache Managers per origin) omitted** — each Manager opens its own engine.
4. **Browser entrypoint omitted** (server-side/.NET client).
5. **Promises → Tasks** for `EmitWithAck` (JS returns a Promise; C# returns via callback
   to match the rest of the API).

---

## 4. File structure

```
src/SharpSocketIO.SocketIo.Client/
  SharpSocketIO.SocketIo.Client.csproj
  ManagerOptions.cs
  Manager.cs                    // engine lifecycle + reconnect + namespace dict
  SocketIoClientSocket.cs       // per-namespace client socket
  Url.cs                        // port of url.ts
  Contrib/
    Backoff.cs                  // port of contrib/backo2.ts
tests/SharpSocketIO.SocketIo.Client.Tests/
  SharpSocketIO.SocketIo.Client.Tests.csproj
  UrlTests.cs                   // ← test/url.ts
  BackoffTests.cs               // ← test/retry.js
  SocketLogicTests.cs           // ← test/socket.ts logic subset
  IntegrationTests.cs           // ← end-to-end vs SharpSocketIO.SocketIo server
```

---

## 5. Behavior to preserve verbatim

- **Reconnect:** on close, if `Reconnection`, wait `Backoff.Duration` ms then re-open.
  `Backoff`: `ReconnectionDelay` * 2^attempt (capped at `ReconnectionDelayMax`) ± jitter
  (`randomizationFactor`). Reset on successful open.
- **Namespace CONNECT:** a Manager can host many namespace Sockets; each sends its own
  CONNECT on engine open.
- **CONNECT_ERROR:** server may reject (middleware) → Socket emits `connect_error`.
- **Reserved events:** `connect`, `connect_error`, `disconnect` (from socket.io-parser).
- **`volatile`:** events marked volatile are dropped if the socket isn't connected.
- **Ack:** client emits with an id; server's ACK packet invokes the registered callback.

---

## 6. Scope (SC-1 executable now)

- Project skeleton (net8/9/10) referencing EngineIo.Client + SocketIo.Parser + ComponentEmitter.
- `Backoff`, `Url`, `ManagerOptions`.
- `Manager` (engine lifecycle, reconnect, namespace routing).
- `SocketIoClientSocket` (connect/disconnect/on/emit/ack).
- Tests: url.js, retry.js, socket.ts logic (using a fake engine).
- Integration (SC-2): end-to-end against SharpSocketIO.SocketIo server.

---

## Next step

TDD execution (SC-1 logic + SC-2 integration), then final verification.
