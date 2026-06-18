# Design + Plan: Port `socket.io-redis-streams-emitter` (cycle 10 — final)

**Date:** 2026-06-19
**Scope:** Twelfth and final cycle. A standalone emitter that publishes socket.io
broadcast events to a Redis Stream (XADD), consumed by socket.io servers in a cluster.
**Upstream source:** `packages/socket.io-redis-streams-emitter` v0.1.1 (~676 LoC lib + ~402 LoC test).

Structurally identical to postgres-emitter but uses Redis Streams (`XADD`) instead of
Postgres `NOTIFY`. The .NET port mirrors the postgres-emitter: `Emitter` + `BroadcastOperator`
+ `IRedisPublisher` seam + in-memory test publisher.

**Definition of done:**
1. `SharpSocketIO.RedisStreamsEmitter` library: `Emitter` + `BroadcastOperator` + `IRedisPublisher`
   + options + message types.
2. Tests: emit broadcasts to rooms/except/all, namespace, volatile/compress — using in-memory publisher.
3. `dotnet test` green on net8/9/10.
