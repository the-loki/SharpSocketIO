# cluster-adapter (cycle 7) Implementation Plan — focused, TDD

> Inline TDD. Checkbox steps.

**Goal:** Port the cluster adapter: `ClusterAdapter` base (broadcast/sockets-join/leave/fetch/server-side-emit via pub/sub) + `InProcessClusterAdapter` (in-process multiplexer). Tests ported from test/index.ts.

**Tech:** .NET 8/9/10, xUnit. Refs SocketIo.Adapter + SocketIo.Parser + ComponentEmitter.

---

## Task CA-1: Project + ClusterAdapter base + InProcessCluster + tests

**Files:** project files, `ClusterTypes.cs`, `ClusterAdapter.cs`, `InProcessCluster.cs`, `InProcessClusterAdapter.cs`, `ClusterAdapterTests.cs`.

- [ ] Step 1: Library csproj (net8/9/10) referencing SocketIo.Adapter + SocketIo.Parser + ComponentEmitter
- [ ] Step 2: Test csproj
- [ ] Step 3: Implement `ClusterTypes` — MessageType enum, ClusterMessage (Broadcast/SocketsJoin/SocketsLeave/FetchSockets/ServerSideEmit/Heartbeat/AdapterClose subclasses), ClusterResponse, ClusterAdapterOptions
- [ ] Step 4: Implement `ClusterAdapter : Adapter` base — uid, nsp, BroadcastAsync publishes BROADCAST + delivers locally, OnMessage handles incoming (broadcast delivery, sockets join/leave, fetch-sockets response, server-side-emit), FetchSocketsAsync, ServerSideEmitAsync. Abstract `DoPublish`/`DoPublishResponse`.
- [ ] Step 5: Implement `InProcessCluster` — singleton multiplexer; registers adapters, delivers messages to other adapters (excluding publisher, matching nsp).
- [ ] Step 6: Implement `InProcessClusterAdapter : ClusterAdapter` — registers with cluster; DoPublish → cluster.Publish.
- [ ] Step 7: Tests — two adapter instances sharing one InProcessCluster; broadcast reaches the other instance's sockets; sockets-join/leave propagate; fetch-sockets aggregates; server-side-emit fan-out.
- [ ] Step 8: Add to solution, run tests, iterate green.
- [ ] Step 9: Commit.

## Task CA-2: Final verification

- [ ] Release build + test across all TFMs.
- [ ] Commit.

## Execution

Inline TDD proceeding.
