# AGENTS.md

## Project Overview

SharpSocketIO is a complete C#/.NET port of the [socket.io](https://github.com/socketio/socket.io) v4 monorepo (12 packages), from TypeScript/Node.js to modern C#. All packages target `net10.0; net9.0; net8.0` (pure-logic libraries also target `netstandard2.1`).

## Build & Test

```bash
# Build entire solution (Debug)
dotnet build

# Build (Release, treat warnings as errors)
dotnet build -c Release

# Run all tests
dotnet test

# Run tests for a specific package
dotnet test tests/SharpSocketIO.EngineIo.Parser.Tests/

# Run a single test class
dotnet test --filter "FullyQualifiedName~ParserV3Tests"
```

## Solution Structure

```
SharpSocketIO.sln
├── src/                           # 12 library projects
│   ├── SharpSocketIO.EngineIo.Parser/          # engine.io protocol codec
│   ├── SharpSocketIO.ComponentEmitter/          # event emitter
│   ├── SharpSocketIO.SocketIo.Parser/           # socket.io protocol codec
│   ├── SharpSocketIO.SocketIo.Adapter/          # in-memory room/namespace adapter
│   ├── SharpSocketIO.EngineIo/                  # engine.io server (ASP.NET Core/Kestrel)
│   ├── SharpSocketIO.EngineIo.Client/           # engine.io client (HttpClient + ClientWebSocket)
│   ├── SharpSocketIO.SocketIo/                  # socket.io server (namespaces, rooms, broadcast)
│   ├── SharpSocketIO.SocketIo.Client/           # socket.io client (Manager + reconnect)
│   ├── SharpSocketIO.ClusterAdapter/            # multi-instance cluster adapter
│   ├── SharpSocketIO.ClusterEngine/             # distributed-lock engine.io server
│   ├── SharpSocketIO.PostgresEmitter/           # Postgres NOTIFY broadcast emitter
│   └── SharpSocketIO.RedisStreamsEmitter/       # Redis Streams broadcast emitter
├── tests/                         # 12 test projects (xUnit)
├── docs/superpowers/              # Design specs + implementation plans
├── Directory.Build.props          # Central: LangVersion=latest, Nullable=enable, TreatWarningsAsErrors=true
└── README.md
```

## Architecture & Dependency Graph

```
EngineIo.Parser ←── EngineIo (server) ←── SocketIo (server)
       ↑                  ↑                        ↑
ComponentEmitter    EngineIo.Client          SocketIo.Adapter
       ↑                  ↑                        ↑
SocketIo.Parser ←── SocketIo.Client ←── SocketIo (server)
                        ↓
                  ClusterAdapter, ClusterEngine, PostgresEmitter, RedisStreamsEmitter
```

Bottom-up build order (dependencies flow upward):
1. `EngineIo.Parser` + `ComponentEmitter` (no deps)
2. `SocketIo.Parser` (depends on ComponentEmitter)
3. `SocketIo.Adapter` (depends on ComponentEmitter)
4. `EngineIo` (depends on EngineIo.Parser, ComponentEmitter)
5. `EngineIo.Client` (depends on EngineIo.Parser, ComponentEmitter)
6. `SocketIo` (depends on SocketIo.Parser, SocketIo.Adapter, ComponentEmitter, EngineIo)
7. `SocketIo.Client` (depends on EngineIo.Client, SocketIo.Parser, ComponentEmitter)
8. `ClusterAdapter`, `ClusterEngine`, `PostgresEmitter`, `RedisStreamsEmitter` (leaf utilities)

## Key Design Decisions

- **ASP.NET Core / Kestrel** replaces Node's `http.Server` for engine.io server
- **HttpClient** + **ClientWebSocket** for engine.io client transports
- **System.Text.Json** for socket.io-parser data serialization
- **In-process multiplexers** replace Node's `cluster` IPC for cluster packages
- **IPublisher seams** (`IPostgresPublisher`, `IRedisPublisher`) decouple emitters from transport
- `IEngineIoConnection` abstracts the engine.io connection so socket.io logic is testable without a live server
- Node `Buffer` → `byte[]`; JS `ArrayBuffer` → C# `ArrayBuffer` struct (offset + length)
- Callback-based API (`Action<object[]>`) mirrors JS event emitter patterns

## Coding Conventions

- **C# latest** (LangVersion=latest in Directory.Build.props)
- **Nullable reference types** enabled (`<Nullable>enable</Nullable>`)
- **Warnings as errors** (`<TreatWarningsAsErrors>true</TreatWarningsAsErrors>`)
- **Implicit usings** enabled
- **File-scoped namespaces** and **record types** where appropriate
- Test framework: **xUnit** with `Microsoft.NET.Test.Sdk`
- All public types use XML doc comments on the public API surface

## Namespace Conventions

| Assembly | Root namespace |
|---|---|
| EngineIo.Parser | `SharpSocketIO.EngineIo.Parser` / `.Commons` |
| ComponentEmitter | `SharpSocketIO.ComponentEmitter` |
| SocketIo.Parser | `SharpSocketIO.SocketIo.Parser` / `.Commons` |
| SocketIo.Adapter | `SharpSocketIO.SocketIo.Adapter` |
| EngineIo (server) | `SharpSocketIO.EngineIo` / `.Commons` / `.Transports` / `.Http` |
| EngineIo.Client | `SharpSocketIO.EngineIo.Client` / `.Transports` |
| SocketIo (server) | `SharpSocketIO.SocketIo` |
| SocketIo.Client | `SharpSocketIO.SocketIoClient` (NOT `.SocketIo.Client` — avoids clash with server's `Client` type) |
| ClusterAdapter | `SharpSocketIO.ClusterAdapter` |
| ClusterEngine | `SharpSocketIO.ClusterEngine` |
| PostgresEmitter | `SharpSocketIO.PostgresEmitter` |
| RedisStreamsEmitter | `SharpSocketIO.RedisStreamsEmitter` |

## Protocol Compatibility Notes

- Wire format verified against upstream golden-string assertions (e.g. `"0\x1e1\x1e2probe"`, `"bAQIDBA=="`)
- Server handles client `Pong` packets (re-arms `pingInterval` timer) — critical for v4 client interop
- Server CONNECT reply carries `{sid}` as a JSON object
- Binary frames supported end-to-end (send + receive) via `IEngineIoConnection.Send(byte[])`
- `Emitter<TEvents>` is thread-safe (lock + snapshot pattern) since .NET timers fire on ThreadPool threads

## Known Limitations

- **Connection-state recovery** (CSR): `SessionAwareAdapter` has minimal persist/restore stubs; full offset tracking not implemented
- **socket.io-client upgrade**: `Manager` uses `Upgrade=false` (polling-only) for stability; the upgrade path works but has timing sensitivity under load
- **`BroadcastFlags`** (volatile/compress/local): parsed but not fully enforced on the broadcast path
- **Typed events**: API uses `object[]` args (no generic `On<T>` overloads); matches JS flexibility but loses compile-time safety
- **Cluster packages** use in-process multiplexers (not Node's `cluster` IPC or Redis/Postgres pub/sub directly)
- **`serveClient`** (static file serving) omitted — consumers serve the JS client themselves
- **uWebSockets.js / WebTransport** variants omitted — .NET uses Kestrel + `System.Net.WebSockets`

## Testing Patterns

- Unit tests for pure logic (parsers, adapter, emitter, backoff, URL parsing)
- Integration tests use **real Kestrel servers** (TestServer or ephemeral port) with `HttpClient`/`ClientWebSocket` drivers
- socket.io server tests use a `FakeEngineIoConnection` to test logic without a live server
- socket.io-client integration tests spin a real `SharpSocketIO.SocketIo` server on Kestrel
- Integration tests have generous timeouts (15s) due to real-network timing
- Some integration tests are timing-sensitive and may occasionally flake under heavy CI load

## Upstream Reference

The `_upstream/` directory (git-ignored) contains a shallow clone of `socketio/socket.io` used as the porting reference. Design specs in `docs/superpowers/specs/` document each package's porting decisions.

## Git Workflow

- Single `main` branch — all work merged
- Feature branches used during development, deleted after merge
- Commits follow conventional-commit style: `feat(package):`, `fix:`, `test:`, `docs:`, `build:`, `chore:`
