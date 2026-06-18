# SharpSocketIO

A complete C#/.NET port of the [socket.io](https://github.com/socketio/socket.io) v4 monorepo, from TypeScript/Node.js to modern C#. All 12 upstream packages have been ported test-first (TDD), targeting .NET 8/9/10 (with `netstandard2.1` for pure-logic libraries).

## Status

**All 12 packages complete.** 678 tests passing across `net8.0`, `net9.0`, `net10.0`. 0 warnings, 0 errors.

## Packages

| .NET Assembly | Upstream Package | Version | Tests | Description |
|---|---|---|---|---|
| `SharpSocketIO.EngineIo.Parser` | engine.io-parser | 5.2.3 | 45 | Engine.IO protocol packet codec |
| `SharpSocketIO.ComponentEmitter` | @socket.io/component-emitter | 3.1.0 | 14 | Event emitter |
| `SharpSocketIO.SocketIo.Parser` | socket.io-parser | 4.2.6 | 31 | Socket.IO protocol packet codec |
| `SharpSocketIO.EngineIo` | engine.io | 6.6.9 | 47 | Engine.IO server (HTTP polling + WebSocket) |
| `SharpSocketIO.EngineIo.Client` | engine.io-client | 6.6.6 | 16 | Engine.IO client |
| `SharpSocketIO.SocketIo.Adapter` | socket.io-adapter | 2.5.2 | 10 | In-memory room/namespace adapter |
| `SharpSocketIO.SocketIo` | socket.io | 4.8.3 | 20 | Socket.IO server (namespaces, rooms, broadcast) |
| `SharpSocketIO.SocketIo.Client` | socket.io-client | 4.8.3 | 13 | Socket.IO client (Manager + reconnect) |
| `SharpSocketIO.ClusterAdapter` | @socket.io/cluster-adapter | 0.3.0 | 6 | Multi-instance cluster adapter |
| `SharpSocketIO.ClusterEngine` | @socket.io/cluster-engine | 0.1.0 | 6 | Distributed-lock engine.io server |
| `SharpSocketIO.PostgresEmitter` | @socket.io/postgres-emitter | 0.1.1 | 9 | Postgres-based broadcast emitter |
| `SharpSocketIO.RedisStreamsEmitter` | @socket.io/redis-streams-emitter | 0.1.1 | 9 | Redis Streams broadcast emitter |

## Architecture

```
SharpSocketIO.sln
├── src/                          # 12 library projects
│   ├── SharpSocketIO.EngineIo.Parser/
│   ├── SharpSocketIO.ComponentEmitter/
│   ├── SharpSocketIO.SocketIo.Parser/
│   ├── SharpSocketIO.EngineIo/          # ASP.NET Core / Kestrel server
│   ├── SharpSocketIO.EngineIo.Client/   # HttpClient + ClientWebSocket
│   ├── SharpSocketIO.SocketIo.Adapter/
│   ├── SharpSocketIO.SocketIo/          # socket.io server
│   ├── SharpSocketIO.SocketIo.Client/   # socket.io client (Manager + Socket)
│   ├── SharpSocketIO.ClusterAdapter/
│   ├── SharpSocketIO.ClusterEngine/
│   ├── SharpSocketIO.PostgresEmitter/
│   └── SharpSocketIO.RedisStreamsEmitter/
├── tests/                        # 12 test projects
├── docs/superpowers/             # Design specs + implementation plans
└── Directory.Build.props         # Central LangVersion, Nullable, TreatWarningsAsErrors
```

## Target Frameworks

- **Server/client packages:** `net10.0; net9.0; net8.0` (ASP.NET Core / HttpClient / WebSockets)
- **Pure-logic libraries** (parsers, emitter, adapter): also `netstandard2.1`

## Key Design Decisions

- **ASP.NET Core / Kestrel** replaces Node's `http.Server` for the engine.io server
- **HttpClient** + **ClientWebSocket** replace Node's `http`/`ws` for clients
- **System.Text.Json** replaces JSON.stringify/parse for socket.io-parser data
- **In-process multiplexers** replace Node's `cluster` IPC for cluster packages
- **IPublisher seams** (IPostgresPublisher, IRedisPublisher) decouple emitters from transport

## Running Tests

```bash
dotnet test -c Release
```

## License

MIT (matching upstream socket.io)
