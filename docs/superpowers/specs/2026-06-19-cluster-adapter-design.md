# Design + Plan: Port `socket.io-cluster-adapter` (cycle 7)

**Date:** 2026-06-19
**Scope:** Ninth cycle. The cluster adapter enables multi-instance socket.io servers
to broadcast across instances (each socket.io server instance sees sockets connected
to the others).
**Upstream source:** `packages/socket.io-cluster-adapter` v0.3.0 (116 LoC) — a thin
transport-specific adapter. Its base class `ClusterAdapter`/`ClusterAdapterWithHeartbeat`
lives in `packages/socket.io-adapter/lib/cluster-adapter.ts` (1017 LoC), which cycle 4
deferred.

---

## 1. Goal & success criteria

Port the cluster adapter to .NET so a socket.io server with multiple instances (or
multiple Server objects) can broadcast/coordinate across them.

**Adaptation (key):** Node's `cluster` module (single-process-forking IPC) has no .NET
equivalent. The .NET port provides:
- The `ClusterAdapter`/`ClusterAdapterWithHeartbeat` base classes (port of the 1017-LoC
  cluster-adapter.ts) with an abstract pub/sub seam (`IPubSub`/`doPublish`/`doPublishResponse`).
- An **in-process** `InProcessClusterAdapter` that uses a shared `Channel<ClusterMessage>`
  multiplexer — lets multiple `Server` instances in the same process coordinate
  (the most common .NET scenario, and what the upstream `test/index.ts` actually tests
  using a fake pub/sub).
- Tests ported from `test/index.ts` (broadcast across instances, sockets join/leave,
  fetch sockets, server-side emit) using the in-process multiplexer.

**Definition of done:**
1. `SharpSocketIO.ClusterAdapter` library with `ClusterAdapter` + `ClusterAdapterWithHeartbeat`
   base classes, `ClusterMessage` types, `MessageType` enum, `ClusterAdapterOptions`,
   and `InProcessClusterAdapter` + `InProcessCluster` (the multiplexer).
2. Tests: broadcast across two adapter instances (each representing a "server"), sockets
   join/leave, fetch sockets, server-side emit — ported from `test/index.ts`.
3. `dotnet test` green on net8/9/10.

---

## 2. Architecture

The upstream `ClusterAdapter`:
- Each instance has a `uid` (random server id).
- Publishes `ClusterMessage`s (BROADCAST, SOCKETS_JOIN, SOCKETS_LEAVE, FETCH_SOCKETS,
  SERVER_SIDE_EMIT, HEARTBEAT, etc.) via `doPublish`.
- Subscribes to messages via `onMessage`. Each message carries the source `uid` and
  the target `nsp`.
- Heartbeat: periodically publishes HEARTBEAT; if a peer isn't heard from within
  `heartbeatTimeout`, it's considered down.
- `BroadcastAsync` publishes a BROADCAST message; each receiving instance delivers the
  packet to its local sockets matching the rooms/except.

The .NET in-process variant:
- `InProcessCluster` is a singleton multiplexer holding a list of subscribed adapters.
- `InProcessClusterAdapter` registers with the cluster; `doPublish` enqueues the message
  to all *other* registered adapters (excluding the publisher, matching nsp).

---

## 3. Type mapping (key)

| JS | C# |
|---|---|
| `class ClusterAdapter extends Adapter` | `class ClusterAdapter : AdapterBase` |
| `MessageType` enum (INITIAL_HEARTBEAT..) | `enum MessageType` |
| `ClusterMessage` union | `abstract class ClusterMessage` + subclasses per type |
| `ClusterAdapterWithHeartbeat extends ClusterAdapter` | `class ClusterAdapterWithHeartbeat : ClusterAdapter` |
| `doPublish(message)` | `protected abstract Task<string> DoPublish(ClusterMessage message)` |
| `onMessage(message)` | `public void OnMessage(ClusterMessage message)` |
| `NodeClusterAdapter` (this package) | `InProcessClusterAdapter` (the .NET transport) |

---

## 4. File structure

```
src/SharpSocketIO.ClusterAdapter/
  SharpSocketIO.ClusterAdapter.csproj
  ClusterTypes.cs          // MessageType, ClusterMessage subclasses, ClusterAdapterOptions
  ClusterAdapter.cs        // base ClusterAdapter (port of cluster-adapter.ts core)
  ClusterAdapterWithHeartbeat.cs
  InProcessCluster.cs      // the in-process multiplexer (Node cluster IPC equivalent)
  InProcessClusterAdapter.cs
tests/SharpSocketIO.ClusterAdapter.Tests/
  ClusterAdapterTests.cs   // ← test/index.ts (broadcast/join/leave/fetch/server-side-emit)
```

References: `SharpSocketIO.SocketIo.Adapter` (the Adapter base), `SharpSocketIO.SocketIo.Parser`,
`SharpSocketIO.ComponentEmitter`.

---

## 5. Behavior to preserve

- **Broadcast fan-out:** a BROADCAST published by instance A is delivered to local sockets
  of instances B, C, ... matching the rooms/except (but NOT A's own — A delivers locally
  via its own adapter).
- **FetchSockets:** a FETCH_SOCKETS request collects socket info from all instances;
  responses aggregated.
- **ServerSideEmit:** fan-out + ack aggregation.
- **Heartbeat:** peers discovered/evicted by heartbeat presence (deferred to a minimal
  impl — the in-process cluster knows peers directly).
- **Namespace filtering:** messages are scoped to the adapter's namespace.

---

## 6. Adaptations / deviations

1. **Node `cluster` IPC → in-process multiplexer.** No .NET equivalent of Node's cluster
   model; the in-process cluster is the faithful .NET analogue (and what the upstream
   tests effectively use).
2. **`process.send` → `IPubSub.Publish`.** The transport is pluggable.
3. **`setupPrimary` omitted** (Node-cluster-specific routing).
4. **Connection-state-recovery across cluster deferred** (needs Offset tracking + the
   SessionAwareAdapter; minimal in cycle 4).

---

## Next step

TDD execution: project + ClusterAdapter base + InProcessCluster + tests.
