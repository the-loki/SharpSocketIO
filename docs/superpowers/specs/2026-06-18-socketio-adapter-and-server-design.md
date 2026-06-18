# Design: Port `socket.io-adapter` (cycle 4) + `socket.io` (cycle 5) to .NET (C#)

**Date:** 2026-06-18
**Scope:** Sixth + seventh cycles. socket.io server depends on socket.io-adapter, so the
adapter goes first (cycle 4), then socket.io server (cycle 5). Both delivered against this spec.
**Upstream sources:**
- `packages/socket.io-adapter` v2.5.2 (in-memory adapter, ~509 LoC lib; cluster-adapter omitted)
- `packages/socket.io` v4.8.3 (server, ~4,916 LoC lib + ~5,096 LoC test)

These are the highest-layer server packages. Built on the already-ported:
`SharpSocketIO.SocketIo.Parser`, `SharpSocketIO.EngineIo`, `SharpSocketIO.ComponentEmitter`.

---

## 1. Goal & success criteria

Port both packages as-is to idiomatic C# (net8/9/10), preserving every applicable
upstream test (ported to xUnit), test-first.

**Definition of done:**

1. `SharpSocketIO.SocketIo.Adapter` — in-memory room/session/namespace adapter:
   `Adapter` base (add/del/has/sockets/rooms/broadcast), `SessionAwareAdapter`,
   `Room`/`SocketId`/`BroadcastFlags`/`Session` types, the offset/timeout broadcast logic.
2. `SharpSocketIO.SocketIo` — the server: `Server` (attaches to engine.io), `Namespace`,
   `Socket` (per-client), `Client` (engine.io connection wrapper), `ParentNamespace`,
   `BroadcastOperator`, `RemoteSocket`. Room join/leave, event emit/on, acks, middleware,
   server-side emit, volatile/compress/local broadcasts, disconnect reasons.
3. End-to-end: socket.io server mounts on a real Kestrel engine.io server; a ported
   `engine.io-client` (already done) carrying the socket.io protocol connects, joins a
   room, emits events with ack, server broadcasts to rooms — all verified by integration
   tests using our `SharpSocketIO.EngineIo.Client` as the transport.
4. `dotnet test` green on net8/9/10.
5. Wire format compatible (socket.io-parser packets over engine.io).

### Sub-cycle decomposition (both packages)

**Cycle 4 — socket.io-adapter (single cycle):** in-memory `Adapter` + `SessionAwareAdapter`.
Small enough for one cycle. Tests: `socket.io-adapter/test/` (room/session/broadcast logic).

**Cycle 5 — socket.io server, split into:**
- **5A — Core types + Namespace + Socket (logic):** `Server`, `Namespace`, `Socket`,
  `Client`, room join/leave, on/emit/ack, middleware, disconnect — *logic* layer
  testable with fakes (no live server). Tests: socket.ts, namespaces.ts, messaging-many
  (room broadcast) subsets.
- **5B — Live server integration:** mount socket.io server on engine.io + Kestrel;
  end-to-end event/ack/room tests via our engine.io-client. Tests: connection,
  messaging, namespaces over the wire.
- **5C — BroadcastOperator + extras:** `BroadcastOperator` (volatile/compress/local/server-
  side-emit), `RemoteSocket`, `ParentNamespace`, connection-state-recovery. Deferred/omitted
  bits: `serveClient` (static file serving — omit; not core), uWebSockets variant (omit),
  typed-events generic layer (simplified to string-keyed).

This design covers cycle 4 in detail and sketches 5A-5C; each sub-cycle gets its own plan.

---

## 2. Target frameworks & integration model

- **net10.0; net9.0; net8.0** for both packages (server-side, like engine.io).
- socket.io server references: `SharpSocketIO.EngineIo` (server), `SharpSocketIO.SocketIo.Parser`,
  `SharpSocketIO.SocketIo.Adapter`, `SharpSocketIO.ComponentEmitter`.
- `debug` → omitted. `accepts`/`cors`/`zlib` static-serving → omitted (5C notes).
- Adapter is pure logic (Dictionary/HashSet based); no I/O.

---

## 3. Type & API mapping

### Adapter (cycle 4)

| JS | C# |
|---|---|
| `class Adapter extends Emitter` | `class Adapter : Emitter<UnitEvents>` |
| `nsp: Namespace` | `Namespace Nsp` (typed as object to avoid cycle; or `WeakReference`) |
| `rooms: Map<Room, Set<SocketId>>` | `Dictionary<string, HashSet<string>> Rooms` |
| `sids: Map<SocketId, Set<Room>>` | `Dictionary<string, HashSet<string>> Sids` |
| `add(socketId, room)` / `del` / `delAll` / `hasRoom`/`hasSocket` | same names |
| `sockets(rooms)` → Promise<Set> | `Task<IReadOnlySet<string>> SocketsAsync(rooms)` (synchronous impl) |
| `broadcast(packet, opts)` | `Task BroadcastAsync(Packet, BroadcastOptions)` |
| `SessionAwareAdapter extends Adapter` | `class SessionAwareAdapter : Adapter` (private sessions) |
| `Room`, `SocketId`, `PrivateSessionId` | `using Room = System.String;` aliases (or just string) |

### socket.io server (cycle 5)

| JS | C# |
|---|---|
| `class Server extends EventEmitter` | `class Server : Emitter<UnitEvents>` with `Of(nsp)`, `Attach`, `On("connection", socket => ...)`. Wraps a `SharpSocketIO.EngineIo.Server`. |
| `class Namespace extends Emitter` | `class Namespace : Emitter<UnitEvents>` — `On("connection")`, `Use(middleware)`, `To(room)`, `Emit(event, ...args)`, adapter reference, sockets dict. |
| `class Socket extends Emitter` | `class Socket : Emitter<UnitEvents>` — `Id`, `Handshake`, `Rooms`, `On(event)`, `Emit(event, args)`, `Join/Leave(room)`, `To(room)`, `Disconnect()`, `Use(middleware)`, ack callbacks via `IReadOnlyList<object>` data + ack id. |
| `class Client` (engine.io connection → socket.io) | `class Client` — wraps a `SharpSocketIO.EngineIo.Socket`, parses socket.io packets, creates a `Socket` per namespace. |
| `BroadcastOperator` | `class BroadcastOperator` — `To/Except/Compress/Volatile/Local/Emit`. |
| Packet flow: socket.io-parser encode → engine.io socket.send (message) | `SocketIo.Parser.Encoder.Encode` → `EngineIoSocket.Send(message packet)`. |

---

## 4. Behavior to preserve verbatim (key invariants)

- **Namespace default:** `/`. Sockets connect to a namespace via the socket.io handshake
  (`nsp` field in the CONNECT packet).
- **Rooms:** a socket can join many; broadcast via `To(room)`/`In(room)` reaches all in room.
- **Adapter events:** `"create-room"`, `"delete-room"`, `"join"`, `"leave"`.
- **Acks:** client emits with ack id; server's `On(event, (msg, ack) => ack(reply))`. The
  ack is an ACK packet carrying the id.
- **Reserved events:** `connect`, `connect_error`, `disconnect`, `disconnecting`,
  `newListener`, `removeListener` (from our ported socket.io-parser ReservedEvents).
- **Broadcast flags:** `volatile` (drop if not connected), `compress`, `local` (this node only).
- **Server-side emit:** `io.Emit(event, args)` → all sockets in all namespaces.
- **Disconnect reasons:** `transport error/close`, `ping timeout`, `client namespace
  disconnect`, `server namespace disconnect`, `server shutting down`, `forced close`,
  `transport close`.

---

## 5. Adaptations / deviations

1. **Cluster-adapter omitted.** Redis/Postgres clustering is out of scope for the
   standalone server port (its own packages come later). The base `Adapter` is sufficient.
2. **Typed-events generics simplified.** JS's elaborate `EventsMap` typing collapses to
   string-keyed dispatch (matches our emitter). The `socket.io.test-d.ts` type tests are
   omitted (they test TS types, not runtime behavior).
3. **`serveClient` omitted.** Static file serving of the client bundle isn't core to the
   protocol; consumers serve the JS client themselves.
4. **uWebSockets.js variant omitted.** .NET uses Kestrel via engine.io.
5. **`debug` → omitted.**
6. **Promises → Tasks.** Adapter `sockets()`/`fetchSockets()` return `Task`.
7. **connection-state-recovery deferred to 5C** (session resumption); core flow first.

---

## 6. Cycle 4 scope (socket.io-adapter, executable now)

- `SharpSocketIO.SocketIo.Adapter` project (net8/9/10).
- `Adapter` base: `Rooms`/`Sids` maps, `Add`/`Del`/`DelAll`/`HasRoom`/`HasSocket`,
  `SocketsAsync`/`RoomsAsync`, `BroadcastAsync` (iterates matching sockets, sends via the
  namespace's emit path), session support stub.
- `SessionAwareAdapter` (private sessions, persist on disconnect for recovery — minimal).
- Tests: room add/remove, broadcast to room, sockets-in-room query, adapter events.

---

## 7. Risks

- **Server↔Adapter↔Namespace circular refs.** Adapter holds Namespace; Namespace holds
  Adapter; Namespace holds Sockets. Mitigation: careful interface seams; the Adapter's
  broadcast calls back into Namespace.Socket.Send via a delegate/interface, not a direct ref.
- **Packet flow complexity.** socket.io packet → engine.io message packet → transport.
  Mitigation: unit-test the encode/emit path in 5A before wiring live.
- **Live server test flakiness** (seen in engine.io-client). Mitigation: generous timeouts,
  isolate concerns.

---

## Next step

writing-plans → cycle 4 (adapter) plan, then TDD. Cycle 5 plans follow.
