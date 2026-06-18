# Design + Plan: Port `socket.io-cluster-engine` (cycle 8)

**Date:** 2026-06-19
**Scope:** Tenth cycle. A cluster-friendly engine.io server that shares load between
multiple processes without sticky sessions (uses distributed session locks).
**Upstream source:** `packages/socket.io-cluster-engine` v0.1.0 (~960 LoC lib + ~711 LoC test).

The package = `ClusterEngine` base (engine.ts, 693 LoC) + transport-specific subclasses
(cluster.ts Node IPC, redis.ts Redis pubsub) + test in-memory state store.

---

## 1. Goal & success criteria

Port to .NET so multiple engine.io server instances (in-process) can share connections:
any instance can receive any client's request, acquire a distributed lock for that
session, drain buffered packets, and route packets to the owning instance.

**Adaptation:** Node's `cluster` IPC → in-process multiplexer (same approach as
cluster-adapter). The .NET port provides:
- `ClusterEngine` base (wraps a `SharpSocketIO.EngineIo.Server`, distributed lock
  protocol: ACQUIRE_LOCK / ACQUIRE_LOCK_RESPONSE / DRAIN / PACKET / UPGRADE / CLOSE).
- An **in-process** transport (`InProcessClusterEngine`) using a shared multiplexer.
- Tests ported from `test/in-memory.ts` (the core protocol: acquire lock, drain,
  packet routing, close).

**Definition of done:**
1. `SharpSocketIO.ClusterEngine` library with `ClusterEngine` base + `InProcessClusterEngine`
   + message types + in-process state multiplexer.
2. Tests: lock acquisition, packet routing to the owning instance, drain on lock release,
  close propagation — ported from `test/in-memory.ts`.
3. `dotnet test` green on net8/9/10.

---

## 2. Architecture (port of engine.ts)

Each engine instance:
- Has a `NodeId` (random).
- Wraps a `SharpSocketIO.EngineIo.Server`; on each incoming connection, when a request
  for an existing `sid` arrives, the instance must **acquire the lock** for that session
  before processing (ACQUIRE_LOCK → the current owner either releases or denies).
- Once locked, the instance owns the session; packets from the client are processed
  locally. Packets meant for a session owned by another instance are routed via PACKET
  messages.
- On lock release, buffered packets are DRAINed to the new owner.
- CLOSE propagates session teardown.

The in-process multiplexer delivers messages between instances (excluding sender, or
to a specific recipient by NodeId).

---

## 3. File structure

```
src/SharpSocketIO.ClusterEngine/
  SharpSocketIO.ClusterEngine.csproj
  ClusterEngineTypes.cs     // MessageType, Message subclasses, NodeId/SessionId
  ClusterEngine.cs          // base ClusterEngine (lock protocol + packet routing)
  InProcessClusterEngine.cs // in-process transport (multiplexer)
tests/SharpSocketIO.ClusterEngine.Tests/
  ClusterEngineTests.cs     // ← test/in-memory.ts (lock/drain/route/close)
```

Refs: `SharpSocketIO.EngineIo`, `SharpSocketIO.EngineIo.Parser`, `SharpSocketIO.ComponentEmitter`.

---

## 4. Adaptations / deviations

1. Node `cluster`/Redis transports → in-process multiplexer.
2. `engine.io` Server override: the upstream wraps the Server's `_handleUpgrade`/
   `_handleRequest`; .NET wraps via events/hooks where available.
3. `debug` omitted.
4. Minimal: lock + drain + packet + close. Full UPGRADE handshake deferred.

---

## Next step

TDD execution.
