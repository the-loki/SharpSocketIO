# socket.io-client (cycle 6) Implementation Plan

> Inline TDD execution. Checkbox steps.

**Goal:** Port socket.io-client v4.8.3 — Manager (engine + reconnect + namespaces), SocketIoClientSocket (per-namespace), Url, Backoff. End-to-end against SharpSocketIO.SocketIo server.

**Tech Stack:** .NET 8/9/10, xUnit. References: EngineIo.Client, SocketIo.Parser, ComponentEmitter.

---

## Task SC-1: Project + Backoff + Url + ManagerOptions

**Files:** project files, `Contrib/Backoff.cs`, `Url.cs`, `ManagerOptions.cs`, tests.

- [ ] Step 1: Library csproj (net8/9/10) referencing EngineIo.Client + SocketIo.Parser + ComponentEmitter
- [ ] Step 2: Test csproj
- [ ] Step 3: Implement `Backoff` (port of backo2.ts): Duration/Reset/SetMin/SetMax/SetJitter
- [ ] Step 4: Implement `Url.Parse` (port of url.ts — host/path/query/source/secure extraction)
- [ ] Step 5: Implement `ManagerOptions` (reconnection, attempts, delay, delayMax, randomizationFactor, timeout, autoConnect, path)
- [ ] Step 6: Tests — BackoffTests (duration grows exponentially, resets, jitter range), UrlTests (parse various)
- [ ] Step 7: Commit

## Task SC-2: Manager + SocketIoClientSocket

**Files:** `Manager.cs`, `SocketIoClientSocket.cs`, tests.

- [ ] Step 1: `Manager` — wraps `EngineIoClientSocket` engine, owns Decoder/Encoder, namespace dict, reconnect via Backoff, routes engine "open"/"message"/"close" to namespace Sockets
- [ ] Step 2: `SocketIoClientSocket` — Connect/Disconnect, On/Emit/EmitWithAck, Id/Connected/Active, packet handling (CONNECT/EVENT/ACK/DISCONNECT/CONNECT_ERROR), reserved events
- [ ] Step 3: Tests — SocketLogicTests with a fake engine (connect, emit/on, ack, reconnect flag)
- [ ] Step 4: Commit

## Task SC-3: Integration tests + verification

- [ ] Step 1: IntegrationTests — socket.io-client connects to SharpSocketIO.SocketIo server, emits with ack, server replies
- [ ] Step 2: Release build + test across all TFMs
- [ ] Step 3: Commit

## Execution

Inline TDD proceeding.
