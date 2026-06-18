# Design + Plan: Port `socket.io-postgres-emitter` (cycle 9)

**Date:** 2026-06-19
**Scope:** Eleventh cycle. A standalone emitter that publishes socket.io broadcast
events to a Postgres NOTIFY channel, consumed by socket.io servers in a cluster.
**Upstream source:** `packages/socket.io-postgres-emitter` v0.1.1 (~573 LoC lib + ~291 LoC test).

---

## 1. Goal & success criteria

Port to .NET so a non-server process can emit events to a cluster of socket.io servers
via Postgres (NOTIFY for small payloads, a table for large/binary).

**Adaptation:** Upstream uses `pg` (node-postgres) + `@msgpack/msgpack`. .NET port uses
`Npgsql` (optional, via an interface) + the emitter logic. The core is transport-agnostic:
`Emitter` + `BroadcastOperator` encode a broadcast document (type, nsp, rooms, except,
flags, packet) and hand it to an `IPostgresPublisher`. Tests use an in-memory publisher
capturing the published documents (same pattern as cluster packages; the upstream test
needs a live Postgres).

**Definition of done:**
1. `SharpSocketIO.PostgresEmitter` library: `Emitter` + `BroadcastOperator` (To/In/Except/
   Compress/Volatile + Emit), `EventType`, broadcast document type, `IPostgresPublisher`
   abstraction, `PostgresEmitterOptions`.
2. Tests: emit broadcasts to rooms/except/all, namespace scoping, volatile/compress flags,
   document encoding — ported from `test/index.ts` using an in-memory publisher.
3. `dotnet test` green on net8/9/10.

---

## 2. Architecture

- `Emitter(pool, nsp, opts)` → has a `channel` ("socket.io#/{nsp}"), `tableName`,
  `payloadThreshold`. `Of(nsp)` returns a new emitter for a namespace.
- `Emitter.Emit(ev, args)` → `new BroadcastOperator(this).Emit(ev, args)`.
- `BroadcastOperator`: immutable To/In/Except/Compress/Volatile; `Emit` builds a BROADCAST
  document `{uid, type, nsp, data:{rooms, except, flags, packet}}` and calls
  `emitter.Publish(document)`.
- `IPostgresPublisher.Publish(channel, payload)` — the transport seam. Npgsql
  implementation would do `NOTIFY channel, payload` (and table insert for large/binary).

---

## 3. File structure

```
src/SharpSocketIO.PostgresEmitter/
  SharpSocketIO.PostgresEmitter.csproj
  EmitterTypes.cs        // EventType, BroadcastDocument, BroadcastFlags, Options
  Emitter.cs             // Emitter class
  BroadcastOperator.cs   // builder + Emit
  IPostgresPublisher.cs  // transport seam
tests/SharpSocketIO.PostgresEmitter.Tests/
  EmitterTests.cs        // ← test/index.ts using in-memory publisher
```

Refs: `SharpSocketIO.SocketIo.Parser` (encode the EVENT packet), `SharpSocketIO.ComponentEmitter`.

---

## 4. Adaptations

1. `pg`/`NOTIFY` → `IPostgresPublisher` seam (Npgsql optional).
2. `@msgpack/msgpack` → omitted; the document is JSON-serializable (the publisher can
   msgpack-encode if it wants; tests use JSON).
3. `debug` omitted.
4. `hasBinary` → checks for byte[] in the payload (large/binary payloads go to the table).

---

## Next step

TDD execution.
