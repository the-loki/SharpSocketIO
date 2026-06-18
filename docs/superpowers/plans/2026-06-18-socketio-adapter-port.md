# socket.io-adapter (cycle 4) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Checkbox steps.

**Goal:** Port the in-memory `socket.io-adapter` v2.5.2 to `SharpSocketIO.SocketIo.Adapter` (net8/9/10), test-first. Cluster adapter omitted.

**Architecture:** Pure-logic `Adapter : Emitter` with `Rooms` (room→socketIds) and `Sids` (socketId→rooms) maps; `Add/Del/DelAll/HasRoom/HasSocket/SocketsAsync/BroadcastAsync`. Broadcast iterates matching sockets and calls a pluggable `Send` delegate (decouples from Namespace).

**Tech Stack:** .NET 8/9/10, xUnit.

**Reference:** `_upstream/packages/socket.io-adapter/lib/in-memory-adapter.ts`.

---

## Task A-1: Project skeleton + core types + Adapter — DONE

**Files:**
- Create: `src/SharpSocketIO.SocketIo.Adapter/SharpSocketIO.SocketIo.Adapter.csproj`
- Create: `tests/SharpSocketIO.SocketIo.Adapter.Tests/SharpSocketIO.SocketIo.Adapter.Tests.csproj`
- Create: `src/.../AdapterTypes.cs` (Room/SocketId/BroadcastFlags/BroadcastOptions)
- Create: `src/.../Adapter.cs`
- Test: `tests/.../AdapterTests.cs`

- [ ] **Step 1: Library csproj**

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFrameworks>net10.0;net9.0;net8.0;netstandard2.1</TargetFrameworks>
    <RootNamespace>SharpSocketIO.SocketIo.Adapter</RootNamespace>
    <AssemblyName>SharpSocketIO.SocketIo.Adapter</AssemblyName>
    <Version>2.5.2</Version>
    <Description>In-memory adapter for socket.io — C# port of socket.io-adapter v2.5.2.</Description>
  </PropertyGroup>
  <ItemGroup>
    <ProjectReference Include="..\SharpSocketIO.ComponentEmitter\SharpSocketIO.ComponentEmitter.csproj" />
  </ItemGroup>
</Project>
```

- [ ] **Step 2: Test csproj** (net8/9/10)

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFrameworks>net10.0;net9.0;net8.0</TargetFrameworks>
    <IsPackable>false</IsPackable>
    <RootNamespace>SharpSocketIO.SocketIo.Adapter.Tests</RootNamespace>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Microsoft.NET.Test.Sdk" Version="17.11.1" />
    <PackageReference Include="xunit" Version="2.9.2" />
    <PackageReference Include="xunit.runner.visualstudio" Version="2.8.2" />
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\..\src\SharpSocketIO.SocketIo.Adapter\SharpSocketIO.SocketIo.Adapter.csproj" />
  </ItemGroup>
</Project>
```

- [ ] **Step 3: Write failing tests**

`tests/.../AdapterTests.cs`:
```csharp
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using SharpSocketIO.SocketIo.Adapter;
using Xunit;

namespace SharpSocketIO.SocketIo.Adapter.Tests;

internal sealed class FakeNamespace
{
    public List<(string socketId, string packet)> Sent { get; } = new();
    public void Send(string socketId, string packet) => Sent.Add((socketId, packet));
}

public class AdapterTests
{
    private static Adapter NewAdapter()
    {
        var nsp = new FakeNamespace();
        return new Adapter(nsp);
    }

    [Fact]
    public void AddAll_adds_socket_to_rooms_and_emits_create_join()
    {
        var adapter = NewAdapter();
        var created = new List<string>();
        var joined = new List<(string room, string sid)>();
        adapter.On("create-room", args => created.Add((string)args[0]));
        adapter.On("join-room", args => joined.Add(((string)args[0], (string)args[1])));

        adapter.AddAll("s1", new HashSet<string> { "room1", "room2" });

        Assert.Equal(new[] { "room1", "room2" }, created);
        Assert.Equal(new[] { ("room1", "s1"), ("room2", "s1") }, joined.ToArray());
        Assert.True(adapter.HasRoom("room1"));
        Assert.True(adapter.HasSocket("s1"));
    }

    [Fact]
    public void Del_removes_socket_from_room_and_emits_leave_and_delete_when_empty()
    {
        var adapter = NewAdapter();
        adapter.AddAll("s1", new HashSet<string> { "room1" });
        var left = new List<(string room, string sid)>();
        var deleted = new List<string>();
        adapter.On("leave-room", args => left.Add(((string)args[0], (string)args[1])));
        adapter.On("delete-room", args => deleted.Add((string)args[0]));

        adapter.Del("s1", "room1");

        Assert.Equal(new[] { ("room1", "s1") }, left.ToArray());
        Assert.Equal(new[] { "room1" }, deleted);
        Assert.False(adapter.HasRoom("room1"));
    }

    [Fact]
    public void DelAll_removes_socket_from_all_rooms()
    {
        var adapter = NewAdapter();
        adapter.AddAll("s1", new HashSet<string> { "room1", "room2" });
        adapter.AddAll("s2", new HashSet<string> { "room1" });

        adapter.DelAll("s1");

        Assert.False(adapter.HasSocket("s1"));
        Assert.True(adapter.HasRoom("room1")); // still has s2
        Assert.False(adapter.HasRoom("room2")); // empty → deleted
    }

    [Fact]
    public async Task SocketsAsync_returns_sockets_in_given_rooms()
    {
        var adapter = NewAdapter();
        adapter.AddAll("s1", new HashSet<string> { "room1" });
        adapter.AddAll("s2", new HashSet<string> { "room1" });
        adapter.AddAll("s3", new HashSet<string> { "room2" });

        var inRoom1 = await adapter.SocketsAsync(new HashSet<string> { "room1" });
        Assert.Equal(new HashSet<string> { "s1", "s2" }, inRoom1);
    }

    [Fact]
    public async Task RoomsAsync_returns_rooms_for_a_socket()
    {
        var adapter = NewAdapter();
        adapter.AddAll("s1", new HashSet<string> { "room1", "room2" });
        var rooms = await adapter.RoomsAsync("s1");
        Assert.Equal(new HashSet<string> { "room1", "room2" }, rooms);
    }

    [Fact]
    public async Task BroadcastAsync_sends_to_matching_sockets_except_excluded()
    {
        var nsp = new FakeNamespace();
        var adapter = new Adapter(nsp);
        adapter.AddAll("s1", new HashSet<string> { "room1" });
        adapter.AddAll("s2", new HashSet<string> { "room1" });
        adapter.AddAll("s3", new HashSet<string> { "room2" });

        await adapter.BroadcastAsync(
            packet: "hello-packet",
            new BroadcastOptions
            {
                Rooms = new HashSet<string> { "room1" },
                Except = new HashSet<string> { "s2" },
            });

        Assert.Single(nsp.Sent);
        Assert.Equal(("s1", "hello-packet"), nsp.Sent[0]);
    }

    [Fact]
    public void ServerCount_is_one_for_in_memory()
    {
        Assert.Equal(1, NewAdapter().ServerCount());
    }
}
```

- [ ] **Step 4: Run, verify red**

- [ ] **Step 5: Implement `AdapterTypes` + `Adapter`**

`AdapterTypes.cs`:
```csharp
using System.Collections.Generic;

namespace SharpSocketIO.SocketIo.Adapter;

/// <summary>Room name (string alias).</summary>
public sealed class Room { } // marker; we use string for room names

/// <summary>Flags modifying a broadcast.</summary>
public sealed class BroadcastFlags
{
    public bool Volatile { get; set; }
    public bool Compress { get; set; }
    public bool Local { get; set; }
    public bool Broadcast { get; set; }
    public bool Binary { get; set; }
    public int? Timeout { get; set; }
}

/// <summary>Options for a broadcast.</summary>
public sealed class BroadcastOptions
{
    public ISet<string> Rooms { get; set; } = new HashSet<string>();
    public ISet<string>? Except { get; set; }
    public BroadcastFlags? Flags { get; set; }
}

/// <summary>The contract a Namespace must satisfy so the Adapter can push packets.</summary>
public interface IAdapterNamespace
{
    void Send(string socketId, string packet);
}
```

`Adapter.cs`:
```csharp
using System.Collections.Generic;
using System.Threading.Tasks;
using SharpSocketIO.ComponentEmitter;

namespace SharpSocketIO.SocketIo.Adapter;

/// <summary>
/// Port of socket.io-adapter in-memory Adapter. Maintains room→socketIds and
/// socketId→rooms maps; emits create-room/join-room/leave-room/delete-room.
/// Broadcast is decoupled from Namespace via IAdapterNamespace.Send.
/// </summary>
public class Adapter : Emitter<UnitEvents>
{
    public Dictionary<string, HashSet<string>> Rooms { get; } = new();
    public Dictionary<string, HashSet<string>> Sids { get; } = new();
    protected readonly IAdapterNamespace Nsp;

    public Adapter(IAdapterNamespace nsp) { Nsp = nsp; }
    public Adapter(object nsp) { Nsp = nsp as IAdapterNamespace ?? new ThrowingNamespace(); }

    public virtual void Init() { }
    public virtual void Close() { }
    public virtual int ServerCount() => 1;

    public void AddAll(string id, ISet<string> rooms)
    {
        if (!Sids.ContainsKey(id)) Sids[id] = new HashSet<string>();
        foreach (var room in rooms)
        {
            Sids[id].Add(room);
            if (!Rooms.ContainsKey(room))
            {
                Rooms[room] = new HashSet<string>();
                Emit("create-room", room);
            }
            if (!Rooms[room].Contains(id))
            {
                Rooms[room].Add(id);
                Emit("join-room", room, id);
            }
        }
    }

    public void Add(string id, string room) => AddAll(id, new HashSet<string> { room });

    public void Del(string id, string room)
    {
        if (Sids.ContainsKey(id)) Sids[id].Remove(room);
        DelInternal(room, id);
    }

    private void DelInternal(string room, string id)
    {
        if (!Rooms.TryGetValue(room, out var set)) return;
        if (set.Remove(id))
        {
            Emit("leave-room", room, id);
            if (set.Count == 0)
            {
                Rooms.Remove(room);
                Emit("delete-room", room);
            }
        }
    }

    public void DelAll(string id)
    {
        if (!Sids.TryGetValue(id, out var rooms)) return;
        foreach (var room in new List<string>(rooms)) DelInternal(room, id);
        Sids.Remove(id);
    }

    public bool HasRoom(string room) => Rooms.ContainsKey(room);
    public bool HasSocket(string id) => Sids.ContainsKey(id);

    public Task<IReadOnlySet<string>> SocketsAsync(ISet<string> rooms)
    {
        var result = new HashSet<string>();
        foreach (var room in rooms)
        {
            if (Rooms.TryGetValue(room, out var set))
                foreach (var sid in set) result.Add(sid);
        }
        return Task.FromResult<IReadOnlySet<string>>(result);
    }

    public Task<IReadOnlySet<string>> RoomsAsync(string socketId)
    {
        var result = new HashSet<string>();
        if (Sids.TryGetValue(socketId, out var rooms))
            foreach (var r in rooms) result.Add(r);
        return Task.FromResult<IReadOnlySet<string>>(result);
    }

    public virtual Task BroadcastAsync(string packet, BroadcastOptions opts)
    {
        var except = opts.Except ?? new HashSet<string>();
        foreach (var sid in MatchSockets(opts.Rooms, except))
        {
            Nsp.Send(sid, packet);
        }
        return Task.CompletedTask;
    }

    private IEnumerable<string> MatchSockets(ISet<string> rooms, ISet<string> except)
    {
        var matched = new HashSet<string>();
        if (rooms == null || rooms.Count == 0)
        {
            foreach (var sid in Sids.Keys) matched.Add(sid);
        }
        else
        {
            foreach (var room in rooms)
                if (Rooms.TryGetValue(room, out var set))
                    foreach (var sid in set) matched.Add(sid);
        }
        foreach (var ex in except) matched.Remove(ex);
        return matched;
    }

    private sealed class ThrowingNamespace : IAdapterNamespace
    {
        public void Send(string socketId, string packet) =>
            throw new System.NotImplementedException("Namespace does not implement IAdapterNamespace");
    }
}
```

- [ ] **Step 6: Make `FakeNamespace` implement `IAdapterNamespace`**

Update the test's `FakeNamespace`:
```csharp
internal sealed class FakeNamespace : IAdapterNamespace
{
    public List<(string socketId, string packet)> Sent { get; } = new();
    public void Send(string socketId, string packet) => Sent.Add((socketId, packet));
}
```

- [ ] **Step 7: Add projects to solution, run tests, verify green**

```
dotnet sln add src/SharpSocketIO.SocketIo.Adapter/SharpSocketIO.SocketIo.Adapter.csproj
dotnet sln add tests/SharpSocketIO.SocketIo.Adapter.Tests/SharpSocketIO.SocketIo.Adapter.Tests.csproj
dotnet test
```
Expected: all 7 AdapterTests pass.

- [ ] **Step 8: Commit**

```
git add -A
git commit -m "feat(adapter): port socket.io-adapter in-memory Adapter — rooms/sids maps + broadcast"
```

---

## Task A-2: SessionAwareAdapter (minimal) — DONE

**Files:**
- Create: `src/.../SessionAwareAdapter.cs`
- Test: extend `AdapterTests.cs`

- [ ] **Step 1: Implement `SessionAwareAdapter`** (persist session on disconnect for recovery; minimal for cycle 4 — just the data structure + persist/restore stubs)

```csharp
using System.Collections.Generic;

namespace SharpSocketIO.SocketIo.Adapter;

/// <summary>
/// Port of SessionAwareAdapter. Tracks private sessions (pid) for connection-state
/// recovery. Minimal for cycle 4: persist/restore stubs that subclasses or later cycles
/// can back with real storage.
/// </summary>
public class SessionAwareAdapter : Adapter
{
    private readonly Dictionary<string, Session> _sessions = new();

    public SessionAwareAdapter(IAdapterNamespace nsp) : base(nsp) { }

    public void PersistSession(Session session) => _sessions[session.Pid] = session;

    public Session? RestoreSession(string pid, string sid) =>
        _sessions.TryGetValue(pid, out var s) ? s with { Sid = sid } : null;
}

public sealed record Session(string Sid, string Pid, IReadOnlyList<string> Rooms, object? Data, IReadOnlyList<object[]> MissedPackets);
```

- [ ] **Step 2: Add a session round-trip test**

```csharp
    [Fact]
    public void SessionAwareAdapter_persists_and_restores_session()
    {
        var adapter = new SessionAwareAdapter(new FakeNamespace());
        var session = new Session("old-sid", "pid-1", new[] { "room1" }, null, new List<object[]>());
        adapter.PersistSession(session);
        var restored = adapter.RestoreSession("pid-1", "new-sid");
        Assert.NotNull(restored);
        Assert.Equal("new-sid", restored!.Sid);
        Assert.Equal("pid-1", restored.Pid);
    }
```

- [ ] **Step 3: Run, commit**

```
dotnet test
git add -A
git commit -m "feat(adapter): SessionAwareAdapter — minimal session persist/restore for connection-state recovery"
```

---

## Task A-3: Final cycle-4 verification — DONE

- [ ] **Step 1: Release build + test across all TFMs**

```
dotnet build -c Release
dotnet test -c Release
```
Expected: all green on net8/9/10 (+ lib compiles for netstandard2.1).

- [ ] **Step 2: Commit**

```
git add -A
git commit -m "chore(adapter): cycle 4 verification across TFMs" --allow-empty
```

---

## Self-review

**Spec coverage:** cycle 4 covers §6 items: Adapter (rooms/sids/broadcast), SessionAwareAdapter. §3 type mapping honored (IAdapterNamespace seam decouples adapter from Namespace). §4 events (create-room/join-room/leave-room/delete-room) honored.

**Placeholder scan:** none.

**Type consistency:** `Adapter : Emitter<UnitEvents>`, `SessionAwareAdapter : Adapter`. `IAdapterNamespace.Send` is the broadcast seam. `BroadcastOptions.Rooms/Except/Flags` consistent.

**Scope honesty:** cycle 4 is in-memory adapter only; cluster-adapter omitted. Broadcast sends a string "packet" (the encoded form); cycle 5 wires real socket.io-parser packets.

## Execution Handoff

Plan saved to `docs/superpowers/plans/2026-06-18-socketio-adapter-port.md`. Inline TDD execution proceeding.
