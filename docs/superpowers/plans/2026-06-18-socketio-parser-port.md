# component-emitter + socket.io-parser Port — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port `socket.io-component-emitter` (cycle 1.5) and `socket.io-parser` v4.2.6 (cycle 2) to multi-target .NET C#, full upstream test suites ported to xUnit, test-first.

**Architecture:** Two new library projects. `SharpSocketIO.ComponentEmitter` is a generic `Emitter<TEvents>` base with string-keyed dispatch. `SharpSocketIO.SocketIo.Parser` ports the socket.io protocol codec (Encoder/Decoder, binary deconstruct/reconstruct) on top of it, using `System.Text.Json` for data (de)serialization. JS `Buffer`/`ArrayBuffer`→`byte[]`/`ArrayBuffer`; Blob omitted.

**Tech Stack:** .NET 8/9/10 + netstandard2.1, C# latest, xUnit, `System.Text.Json`.

**Reference source (read-only, git-ignored):** `_upstream/packages/socket.io-component-emitter/`, `_upstream/packages/socket.io-parser/`.

---

## Task CE-1: ComponentEmitter project skeleton + tests

**Files:**
- Create: `src/SharpSocketIO.ComponentEmitter/SharpSocketIO.ComponentEmitter.csproj`
- Create: `tests/SharpSocketIO.ComponentEmitter.Tests/SharpSocketIO.ComponentEmitter.Tests.csproj`
- Create: `tests/SharpSocketIO.ComponentEmitter.Tests/EmitterTests.cs`

- [ ] **Step 1: Write the library csproj**

`src/SharpSocketIO.ComponentEmitter/SharpSocketIO.ComponentEmitter.csproj`:
```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFrameworks>net10.0;net9.0;net8.0;netstandard2.1</TargetFrameworks>
    <RootNamespace>SharpSocketIO.ComponentEmitter</RootNamespace>
    <AssemblyName>SharpSocketIO.ComponentEmitter</AssemblyName>
    <Version>3.1.0</Version>
    <Description>Event emitter — C# port of @socket.io/component-emitter.</Description>
  </PropertyGroup>
</Project>
```

- [ ] **Step 2: Write the test csproj**

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFrameworks>net10.0;net9.0;net8.0</TargetFrameworks>
    <IsPackable>false</IsPackable>
    <RootNamespace>SharpSocketIO.ComponentEmitter.Tests</RootNamespace>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Microsoft.NET.Test.Sdk" Version="17.11.1" />
    <PackageReference Include="xunit" Version="2.9.2" />
    <PackageReference Include="xunit.runner.visualstudio" Version="2.8.2" />
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\..\src\SharpSocketIO.ComponentEmitter\SharpSocketIO.ComponentEmitter.csproj" />
  </ItemGroup>
</Project>
```

- [ ] **Step 3: Write failing tests** (port of `test/emitter.js`, omitting the prototype-mixin `Custom`/`Emitter(obj)` JS-specific tests where noted)

`tests/.../EmitterTests.cs`:
```csharp
using System.Collections.Generic;
using SharpSocketIO.ComponentEmitter;
using Xunit;

namespace SharpSocketIO.ComponentEmitter.Tests;

public class EmitterTests
{
    // Port of describe('.on(event, fn)') 'should add listeners'
    [Fact]
    public void On_adds_listeners_in_order()
    {
        var emitter = new Emitter();
        var calls = new List<int>();

        emitter.On("foo", args => calls.Add(11)); // one + val(1) flattened below
        // JS pushes 'one',val then 'two',val. We model each listener receiving args[]
        // and pushing two entries to mirror the flattened expectation.
        var calls2 = new List<object>();
        emitter = new Emitter();
        emitter.On("foo", args => { calls2.Add("one"); calls2.Add(args[0]); });
        emitter.On("foo", args => { calls2.Add("two"); calls2.Add(args[0]); });

        emitter.Emit("foo", 1);
        emitter.Emit("bar", 1);
        emitter.Emit("foo", 2);

        Assert.Equal(new object[] { "one", 1, "two", 1, "one", 2, "two", 2 }, calls2);
    }

    [Fact]
    public void On_supports_Object_prototype_method_names_as_events()
    {
        var emitter = new Emitter();
        var calls = new List<object>();
        emitter.On("constructor", args => { calls.Add("one"); calls.Add(args[0]); });
        emitter.On("__proto__", args => { calls.Add("two"); calls.Add(args[0]); });

        emitter.Emit("constructor", 1);
        emitter.Emit("__proto__", 2);

        Assert.Equal(new object[] { "one", 1, "two", 2 }, calls);
    }

    // Port of describe('.once(event, fn)')
    [Fact]
    public void Once_adds_a_single_shot_listener()
    {
        var emitter = new Emitter();
        var calls = new List<object>();
        emitter.Once("foo", args => { calls.Add("one"); calls.Add(args[0]); });

        emitter.Emit("foo", 1);
        emitter.Emit("foo", 2);
        emitter.Emit("foo", 3);
        emitter.Emit("bar", 1);

        Assert.Equal(new object[] { "one", 1 }, calls);
    }

    // Port of describe('.off(event, fn)') 'should remove a listener'
    [Fact]
    public void Off_removes_a_specific_listener()
    {
        var emitter = new Emitter();
        var calls = new List<string>();
        void One() => calls.Add("one");
        void Two() => calls.Add("two");

        emitter.On("foo", One);
        emitter.On("foo", Two);
        emitter.Off("foo", Two);
        emitter.Emit("foo");

        Assert.Equal(new[] { "one" }, calls);
    }

    [Fact]
    public void Off_works_with_once()
    {
        var emitter = new Emitter();
        var calls = new List<string>();
        void One() => calls.Add("one");

        emitter.Once("foo", One);
        emitter.Once("fee", One);
        emitter.Off("foo", One);
        emitter.Emit("foo");

        Assert.Empty(calls);
    }

    [Fact]
    public void Off_works_when_called_from_within_a_handler()
    {
        var emitter = new Emitter();
        bool called = false;
        void B() => called = true;
        emitter.On("tobi", _ => emitter.Off("tobi", B));
        emitter.On("tobi", B);
        emitter.Emit("tobi");
        Assert.True(called);
        called = false;
        emitter.Emit("tobi");
        Assert.False(called);
    }

    // Port of describe('.off(event)')
    [Fact]
    public void Off_event_removes_all_listeners_for_that_event()
    {
        var emitter = new Emitter();
        var calls = new List<string>();
        void One() => calls.Add("one");
        void Two() => calls.Add("two");

        emitter.On("foo", One);
        emitter.On("foo", Two);
        emitter.Off("foo");
        emitter.Emit("foo");
        emitter.Emit("foo");

        Assert.Empty(calls);
    }

    [Fact]
    public void Off_specific_removes_event_array_when_last_subscriber_leaves()
    {
        var emitter = new Emitter();
        void Cb1() { }
        void Cb2() { }
        emitter.On("foo", Cb1);
        emitter.On("foo", Cb2);
        emitter.Off("foo", Cb1);
        // remaining: Cb2 → key still present
        Assert.Single(emitter.Listeners("foo"));
    }

    // Port of describe('.off()')
    [Fact]
    public void Off_no_args_removes_all_listeners()
    {
        var emitter = new Emitter();
        var calls = new List<string>();
        void One() => calls.Add("one");
        void Two() => calls.Add("two");
        emitter.On("foo", One);
        emitter.On("bar", Two);
        emitter.Emit("foo");
        emitter.Emit("bar");
        emitter.Off();
        emitter.Emit("foo");
        emitter.Emit("bar");
        Assert.Equal(new[] { "one", "two" }, calls);
    }

    // Port of describe('.listeners(event)')
    [Fact]
    public void Listeners_returns_callbacks_when_present()
    {
        var emitter = new Emitter();
        void Foo() { }
        emitter.On("foo", Foo);
        Assert.Single(emitter.Listeners("foo"));
    }

    [Fact]
    public void Listeners_returns_empty_when_absent()
    {
        var emitter = new Emitter();
        Assert.Empty(emitter.Listeners("foo"));
    }

    // Port of describe('.hasListeners(event)')
    [Fact]
    public void HasListeners_true_when_present()
    {
        var emitter = new Emitter();
        emitter.On("foo", () => { });
        Assert.True(emitter.HasListeners("foo"));
    }

    [Fact]
    public void HasListeners_false_when_absent()
    {
        var emitter = new Emitter();
        Assert.False(emitter.HasListeners("foo"));
    }

    // emitReserved alias
    [Fact]
    public void EmitReserved_emits_like_emit()
    {
        var emitter = new Emitter();
        object? got = null;
        emitter.On("decoded", args => got = args[0]);
        emitter.EmitReserved("decoded", 42);
        Assert.Equal(42, got);
    }
}
```

- [ ] **Step 4: Add projects to solution, run tests (expect compile failure — red)**

```
dotnet sln add src/SharpSocketIO.ComponentEmitter/SharpSocketIO.ComponentEmitter.csproj
dotnet sln add tests/SharpSocketIO.ComponentEmitter.Tests/SharpSocketIO.ComponentEmitter.Tests.csproj
dotnet test
```
Expected: compile failure (`Emitter` type missing).

---

## Task CE-2: Implement `Emitter`

**Files:**
- Create: `src/SharpSocketIO.ComponentEmitter/Emitter.cs`
- Create: `src/SharpSocketIO.ComponentEmitter/EmitterEvents.cs`

- [ ] **Step 1: Implement `Emitter`**

`Emitter.cs`:
```csharp
using System;
using System.Collections.Generic;

namespace SharpSocketIO.ComponentEmitter;

/// <summary>
/// C# port of @socket.io/component-emitter. Provides string-keyed event dispatch
/// with on/once/off/emit/listeners/hasListeners. Generic over TEvents so subclasses
/// can declare a marker event-map type (mirrors upstream Emitter&lt;ListenEvents,
/// EmitEvents, ReservedEvents&gt;), but dispatch itself is string-keyed to match the
/// untyped JS API and the ported test suite.
/// </summary>
public class Emitter<TEvents>
{
    private Dictionary<string, List<Delegate>>? _callbacks;

    protected Emitter() { }

    /// <summary>Listen on the given event.</summary>
    public Emitter<TEvents> On(string eventName, Delegate fn) => AddListener(eventName, fn);

    /// <summary>Listen on the given event with a parameterless handler.</summary>
    public Emitter<TEvents> On(string eventName, Action fn) => AddListener(eventName, fn);

    /// <summary>Listen on the given event with a handler receiving the emitted args.</summary>
    public Emitter<TEvents> On(string eventName, Action<object[]> fn) => AddListener(eventName, fn);

    private Emitter<TEvents> AddListener(string eventName, Delegate fn)
    {
        _callbacks ??= new Dictionary<string, List<Delegate>>();
        if (!_callbacks.TryGetValue(eventName, out var list))
        {
            list = new List<Delegate>();
            _callbacks[eventName] = list;
        }
        list.Add(fn);
        return this;
    }

    /// <summary>Adds a listener that fires once then auto-removes.</summary>
    public Emitter<TEvents> Once(string eventName, Action<object[]> fn)
    {
        void Wrapper(object[] args)
        {
            Off(eventName, (Action<object[]>)Wrapper);
            fn(args);
        }
        // tag the wrapper so Off(fn) can find it (mirrors JS on.fn = fn)
        WrapperMap.Set(Wrapper, fn);
        return On(eventName, (Action<object[]>)Wrapper);
    }

    /// <summary>Remove a specific listener, all listeners for an event, or everything.</summary>
    public Emitter<TEvents> Off(string eventName, Delegate fn) => RemoveListener(eventName, fn);

    public Emitter<TEvents> Off(string eventName)
    {
        _callbacks?.Remove(eventName);
        return this;
    }

    public Emitter<TEvents> Off()
    {
        _callbacks?.Clear();
        return this;
    }

    private Emitter<TEvents> RemoveListener(string eventName, Delegate fn)
    {
        if (_callbacks == null) return this;
        if (!_callbacks.TryGetValue(eventName, out var list)) return this;
        for (int i = 0; i < list.Count; i++)
        {
            var cb = list[i];
            if (cb == fn || WrapperMap.Get(cb) == fn)
            {
                list.RemoveAt(i);
                break;
            }
        }
        if (list.Count == 0) _callbacks.Remove(eventName);
        return this;
    }

    /// <summary>Emit an event with the given args. Handlers may mutate the listener set during emit.</summary>
    public Emitter<TEvents> Emit(string eventName, params object[] args)
    {
        if (_callbacks == null) return this;
        if (!_callbacks.TryGetValue(eventName, out var list)) return this;
        // snapshot (JS callbacks.slice(0)) so handlers can off() safely mid-emit
        var snapshot = list.ToArray();
        foreach (var cb in snapshot)
        {
            InvokeHandler(cb, args);
        }
        return this;
    }

    /// <summary>Alias of Emit, used for reserved events (protected in JS).</summary>
    public Emitter<TEvents> EmitReserved(string eventName, params object[] args) => Emit(eventName, args);

    /// <summary>Return the callbacks for an event (empty if none).</summary>
    public IReadOnlyList<Delegate> Listeners(string eventName)
    {
        if (_callbacks != null && _callbacks.TryGetValue(eventName, out var list))
            return list;
        return Array.Empty<Delegate>();
    }

    /// <summary>Whether any handlers are registered for the event.</summary>
    public bool HasListeners(string eventName) => Listeners(eventName).Count > 0;

    private static void InvokeHandler(Delegate cb, object[] args)
    {
        switch (cb)
        {
            case Action action: action(); break;
            case Action<object[]> arr: arr(args); break;
            default: cb.DynamicInvoke(args); break;
        }
    }

    // Maps a Once wrapper back to its original fn so Off(eventName, originalFn) works.
    private static class WrapperMap
    {
        private static readonly System.Collections.Concurrent.ConcurrentDictionary<Delegate, Delegate> s_map = new();
        public static void Set(Delegate wrapper, Delegate original) => s_map[wrapper] = original;
        public static Delegate? Get(Delegate wrapper) => s_map.TryGetValue(wrapper, out var o) ? o : null;
    }
}

/// <summary>Convenience non-generic Emitter (events type = UnitEvents marker).</summary>
public class Emitter : Emitter<UnitEvents>
{
    public Emitter() { }
}

/// <summary>Marker type for untyped emitters.</summary>
public sealed class UnitEvents { }
```

`EmitterEvents.cs`:
```csharp
namespace SharpSocketIO.ComponentEmitter;

/// <summary>
/// Marker base for typed event maps. Subclasses of Emitter&lt;TEvents&gt; declare a
/// concrete TEvents to document their emitted events; dispatch remains string-keyed.
/// </summary>
public abstract class EmitterEvents { }
```

- [ ] **Step 2: Run tests, verify pass**

Run: `dotnet test`
Expected: all `EmitterTests` (13) PASS.

- [ ] **Step 3: Commit**

```
git add -A
git commit -m "feat(emitter): port @socket.io/component-emitter to SharpSocketIO.ComponentEmitter"
```

---

## Task P-1: SocketIo.Parser project skeleton

**Files:**
- Create: `src/SharpSocketIO.SocketIo.Parser/SharpSocketIO.SocketIo.Parser.csproj`
- Create: `tests/SharpSocketIO.SocketIo.Parser.Tests/SharpSocketIO.SocketIo.Parser.Tests.csproj`

- [ ] **Step 1: Write the library csproj**

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFrameworks>net10.0;net9.0;net8.0;netstandard2.1</TargetFrameworks>
    <RootNamespace>SharpSocketIO.SocketIo.Parser</RootNamespace>
    <AssemblyName>SharpSocketIO.SocketIo.Parser</AssemblyName>
    <Version>4.2.6</Version>
    <Description>socket.io protocol parser — C# port of socket.io-parser v4.2.6.</Description>
  </PropertyGroup>
  <ItemGroup>
    <ProjectReference Include="..\SharpSocketIO.ComponentEmitter\SharpSocketIO.ComponentEmitter.csproj" />
  </ItemGroup>
</Project>
```

- [ ] **Step 2: Write the test csproj**

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFrameworks>net10.0;net9.0;net8.0</TargetFrameworks>
    <IsPackable>false</IsPackable>
    <RootNamespace>SharpSocketIO.SocketIo.Parser.Tests</RootNamespace>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Microsoft.NET.Test.Sdk" Version="17.11.1" />
    <PackageReference Include="xunit" Version="2.9.2" />
    <PackageReference Include="xunit.runner.visualstudio" Version="2.8.2" />
    <PackageReference Include="Microsoft.CSharp" Version="4.7.0" />
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\..\src\SharpSocketIO.SocketIo.Parser\SharpSocketIO.SocketIo.Parser.csproj" />
  </ItemGroup>
</Project>
```

- [ ] **Step 3: Add to solution**

```
dotnet sln add src/SharpSocketIO.SocketIo.Parser/SharpSocketIO.SocketIo.Parser.csproj
dotnet sln add tests/SharpSocketIO.SocketIo.Parser.Tests/SharpSocketIO.SocketIo.Parser.Tests.csproj
```

---

## Task P-2: Core types — `PacketType`, `Packet`, `ReservedEvents`, protocol constant

**Files:**
- Create: `src/.../Commons/PacketType.cs`
- Create: `src/.../Commons/Packet.cs`
- Create: `src/.../Commons/ReservedEvents.cs`
- Create: `src/.../Commons/DecodedEvents.cs`
- Create: `src/.../SocketIoParser.cs` (partial: protocol + re-exports)
- Test: `tests/.../ParserTests.cs` (partial — exposes-types test + connection/disconnect/event encode tests)

- [ ] **Step 1: Write failing tests for type exposure + simple encodes**

`tests/.../ParserTests.cs` (initial subset — connection/disconnect/event):
```csharp
using System.Collections.Generic;
using System.Text.Json;
using SharpSocketIO.SocketIo.Parser;
using SharpSocketIO.SocketIo.Parser.Commons;
using Xunit;

namespace SharpSocketIO.SocketIo.Parser.Tests;

public class ParserTests
{
    // Port of "exposes types"
    [Fact]
    public void Exposes_packet_types_as_numbers()
    {
        Assert.Equal(0, (int)PacketType.Connect);
        Assert.Equal(1, (int)PacketType.Disconnect);
        Assert.Equal(2, (int)PacketType.Event);
        Assert.Equal(3, (int)PacketType.Ack);
        Assert.Equal(4, (int)PacketType.ConnectError);
        Assert.Equal(5, (int)PacketType.BinaryEvent);
        Assert.Equal(6, (int)PacketType.BinaryAck);
    }

    [Fact]
    public void Protocol_is_5()
    {
        Assert.Equal(5, SocketIoParser.Protocol);
    }

    // helpers.test equivalent: encode then decode, expect equality
    internal static void TestRoundTrip(Packet obj)
    {
        var encoder = new Encoder();
        var encoded = encoder.Encode(obj);
        Packet? decoded = null;
        var decoder = new Decoder();
        decoder.On("decoded", args => decoded = (Packet)args[0]);
        foreach (var part in encoded) decoder.Add(part);
        Assert.NotNull(decoded);
        AssertEqual(obj, decoded!);
    }

    internal static void AssertEqual(Packet a, Packet b)
    {
        Assert.Equal(a.Type, b.Type);
        Assert.Equal(a.Nsp, b.Nsp);
        Assert.Equal(a.Id, b.Id);
        Assert.Equal(a.Attachments, b.Attachments);
        Assert.Equal(JsonSerializer.Serialize(a.Data), JsonSerializer.Serialize(b.Data));
    }

    [Fact]
    public void Encodes_connection()
    {
        TestRoundTrip(new Packet
        {
            Type = PacketType.Connect,
            Nsp = "/woot",
            Data = new Dictionary<string, object> { ["token"] = "123" },
        });
    }

    [Fact]
    public void Encodes_disconnection()
    {
        TestRoundTrip(new Packet { Type = PacketType.Disconnect, Nsp = "/woot" });
    }

    [Fact]
    public void Encodes_an_event()
    {
        TestRoundTrip(new Packet
        {
            Type = PacketType.Event,
            Data = new object[] { "a", 1, new Dictionary<string, object>() },
            Nsp = "/",
        });
    }

    [Fact]
    public void Encodes_an_event_with_integer_event_name()
    {
        TestRoundTrip(new Packet
        {
            Type = PacketType.Event,
            Data = new object[] { 1, "a", new Dictionary<string, object>() },
            Nsp = "/",
        });
    }

    [Fact]
    public void Encodes_an_event_with_ack()
    {
        TestRoundTrip(new Packet
        {
            Type = PacketType.Event,
            Data = new object[] { "a", 1, new Dictionary<string, object>() },
            Id = 1,
            Nsp = "/test",
        });
    }

    [Fact]
    public void Encodes_an_ack()
    {
        TestRoundTrip(new Packet
        {
            Type = PacketType.Ack,
            Data = new object[] { "a", 1, new Dictionary<string, object>() },
            Id = 123,
            Nsp = "/",
        });
    }

    [Fact]
    public void Encodes_a_connect_error_string()
    {
        TestRoundTrip(new Packet
        {
            Type = PacketType.ConnectError,
            Data = "Unauthorized",
            Nsp = "/",
        });
    }

    [Fact]
    public void Encodes_a_connect_error_object()
    {
        TestRoundTrip(new Packet
        {
            Type = PacketType.ConnectError,
            Data = new Dictionary<string, object> { ["message"] = "Unauthorized" },
            Nsp = "/",
        });
    }
}
```

- [ ] **Step 2: Run, verify fail (red — types missing)**

Run: `dotnet test` → compile failure.

- [ ] **Step 3: Implement `PacketType`**

`Commons/PacketType.cs`:
```csharp
namespace SharpSocketIO.SocketIo.Parser.Commons;

/// <summary>socket.io protocol packet type (protocol v5).</summary>
public enum PacketType
{
    Connect = 0,
    Disconnect = 1,
    Event = 2,
    Ack = 3,
    ConnectError = 4,
    BinaryEvent = 5,
    BinaryAck = 6,
}
```

- [ ] **Step 4: Implement `ReservedEvents`**

`Commons/ReservedEvents.cs`:
```csharp
namespace SharpSocketIO.SocketIo.Parser.Commons;

/// <summary>Port of RESERVED_EVENTS — event names with special meaning.</summary>
public static class ReservedEvents
{
    public static readonly string[] All =
    {
        "connect",
        "connect_error",
        "disconnect",
        "disconnecting",
        "newListener",
        "removeListener",
    };

    public static bool Contains(string name) => System.Array.IndexOf(All, name) >= 0;
}
```

- [ ] **Step 5: Implement `Packet`**

`Commons/Packet.cs`:
```csharp
namespace SharpSocketIO.SocketIo.Parser.Commons;

/// <summary>Port of interface Packet from lib/index.ts.</summary>
public sealed class Packet
{
    public PacketType Type { get; set; }
    public string Nsp { get; set; } = "/";
    public object? Data { get; set; }
    public int? Id { get; set; }
    public int? Attachments { get; set; }
}
```

- [ ] **Step 6: Implement `DecodedEvents` marker**

`Commons/DecodedEvents.cs`:
```csharp
using SharpSocketIO.ComponentEmitter;

namespace SharpSocketIO.SocketIo.Parser.Commons;

/// <summary>
/// Marker event map for the Decoder's emitted events. Currently: 'decoded' (Packet).
/// Dispatch is string-keyed (see Emitter&lt;TEvents&gt;).
/// </summary>
public sealed class DecodedEvents : EmitterEvents { }
```

- [ ] **Step 7: Stub `Encoder`, `Decoder`, `SocketIoParser` so tests compile (still red)**

`Encoder.cs`:
```csharp
using System;
using SharpSocketIO.SocketIo.Parser.Commons;

namespace SharpSocketIO.SocketIo.Parser;

public sealed class Encoder
{
    public Encoder(Func<string, object?, object?>? replacer = null) { }
    public System.Collections.Generic.IReadOnlyList<object> Encode(Packet obj)
        => throw new System.NotImplementedException();
}
```

`Decoder.cs`:
```csharp
using SharpSocketIO.ComponentEmitter;
using SharpSocketIO.SocketIo.Parser.Commons;

namespace SharpSocketIO.SocketIo.Parser;

public sealed class Decoder : Emitter<DecodedEvents>
{
    public Decoder(object? opts = null) { }
    public void Add(object part) => throw new System.NotImplementedException();
    public void Destroy() { }
}
```

`SocketIoParser.cs`:
```csharp
namespace SharpSocketIO.SocketIo.Parser;

public static class SocketIoParser
{
    public const int Protocol = 5;
}
```

- [ ] **Step 8: Run, verify compile + test fail (red on NotImplementedException)**

Run: `dotnet test` → tests fail at runtime (NotImplementedException) — confirming red.

- [ ] **Step 9: Commit (red baseline)**

```
git add -A
git commit -m "test(parser): port socket.io-parser parser.js tests (red baseline)"
```

---

## Task P-3: Implement `Encoder` (string encode path)

**Files:**
- Modify: `src/.../Encoder.cs`
- Create: `src/.../Commons/JsonOptions.cs`
- Create: `src/.../IsBinary.cs` (IsBinary + HasBinary)
- Create: `src/.../Binary.cs` (DeconstructPacket / ReconstructPacket)

- [ ] **Step 1: Implement `JsonOptions`**

`Commons/JsonOptions.cs`:
```csharp
using System.Text.Json;
using System.Text.Json.Serialization;

namespace SharpSocketIO.SocketIo.Parser.Commons;

/// <summary>Shared JSON options: preserve exact object semantics like JS JSON.stringify/parse.</summary>
internal static class JsonOptions
{
    public static readonly JsonSerializerOptions Default = new()
    {
        // JS JSON.stringify writes integers without decimal point; System.Text.Json does this
        // for int/long automatically. Keep dictionaries as objects, preserve key insertion order
        // (Dictionary<string,object> is honored in order by JsonSerializer).
        IncludeFields = false,
        DefaultIgnoreCondition = JsonIgnoreCondition.Never,
    };
}
```

- [ ] **Step 2: Implement `IsBinary` / `HasBinary`**

`IsBinary.cs`:
```csharp
using System.Collections;
using System.Reflection;
using SharpSocketIO.EngineIo.Parser; // not available — define ArrayBuffer locally

namespace SharpSocketIO.SocketIo.Parser;

// NOTE: socket.io-parser does NOT reference engine.io-parser. Define a local ArrayBuffer
// marker or accept byte[] only. For .NET, binary attachments are byte[] (or ArraySegment<byte>).
```

Wait — `socket.io-parser` must NOT depend on `SharpSocketIO.EngineIo.Parser` (different protocol layer). Binary attachments here are just `byte[]`. Let me define `IsBinary`/`HasBinary` against `byte[]` and `ArraySegment<byte>` only:

`IsBinary.cs` (corrected):
```csharp
using System.Collections;

namespace SharpSocketIO.SocketIo.Parser;

/// <summary>Port of lib/is-binary.ts. In .NET, binary values are byte[] (Blob/File have no primitive).</summary>
public static class IsBinaryHelper
{
    public static bool IsBinary(object? obj)
    {
        if (obj is null) return false;
        return obj is byte[] or ArraySegment<byte>;
    }

    public static bool HasBinary(object? obj)
    {
        if (obj is null) return false;
        if (IsBinary(obj)) return true;
        if (obj is string) return false; // strings are iterable but not binary carriers

        if (obj is IDictionary dict)
        {
            foreach (DictionaryEntry kv in dict)
                if (HasBinary(kv.Value)) return true;
            return false;
        }
        if (obj is IList list)
        {
            foreach (var item in list)
                if (HasBinary(item)) return true;
            return false;
        }
        // Plain CLR object: reflect public properties + a ToJSON() hook
        if (obj.GetType().GetMethod("ToJSON", Type.EmptyTypes) is { } toJSON)
        {
            var json = toJSON.Invoke(obj, null);
            return HasBinary(json);
        }
        foreach (var prop in obj.GetType().GetProperties())
        {
            var val = prop.GetValue(obj);
            if (HasBinary(val)) return true;
        }
        return false;
    }
}
```

- [ ] **Step 3: Implement `Binary` deconstruct/reconstruct**

`Binary.cs`:
```csharp
using System.Collections;
using System.Collections.Generic;

namespace SharpSocketIO.SocketIo.Parser;

/// <summary>Port of lib/binary.ts (deconstructPacket / reconstructPacket).</summary>
public static class BinaryPacket
{
    /// <summary>Replaces every byte[] in packet.Data with a numbered placeholder; returns buffers.</summary>
    public static (Packet.Commons.Packet Packet, List<byte[]> Buffers) DeconstructPacket(
        Packet.Commons.Packet packet)
    {
        var buffers = new List<byte[]>();
        packet.Data = DeconstructValue(packet.Data, buffers);
        packet.Attachments = buffers.Count;
        return (packet, buffers);
    }

    private static object? DeconstructValue(object? data, List<byte[]> buffers)
    {
        if (data is null) return null;
        if (IsBinaryHelper.IsBinary(data))
        {
            var bytes = data is byte[] b ? b : ByteArrayFromSegment((ArraySegment<byte>)data);
            buffers.Add(bytes);
            return new Dictionary<string, object>
            {
                ["_placeholder"] = true,
                ["num"] = buffers.Count - 1,
            };
        }
        if (data is IDictionary dict)
        {
            var result = new Dictionary<string, object?>();
            foreach (DictionaryEntry kv in dict)
                result[(string)kv.Key] = DeconstructValue(kv.Value, buffers);
            return result;
        }
        if (data is IList list)
        {
            var result = new List<object?>();
            foreach (var item in list) result.Add(DeconstructValue(item, buffers));
            return result;
        }
        return data;
    }

    /// <summary>Replaces placeholders in packet.Data with the supplied buffers.</summary>
    public static Packet.Commons.Packet ReconstructPacket(Packet.Commons.Packet packet, IReadOnlyList<byte[]> buffers)
    {
        packet.Data = ReconstructValue(packet.Data, buffers);
        packet.Attachments = null;
        return packet;
    }

    private static object? ReconstructValue(object? data, IReadOnlyList<byte[]> buffers)
    {
        if (data is null) return null;
        if (data is IDictionary dict)
        {
            if (dict.Contains("_placeholder") && dict["_placeholder"] is bool b && b)
            {
                var num = System.Convert.ToInt32(dict["num"]);
                if (num < 0 || num >= buffers.Count) throw new System.InvalidOperationException("illegal attachments");
                return buffers[num];
            }
            var result = new Dictionary<string, object?>();
            foreach (DictionaryEntry kv in dict)
                result[(string)kv.Key] = ReconstructValue(kv.Value, buffers);
            return result;
        }
        if (data is IList list)
        {
            var result = new List<object?>();
            foreach (var item in list) result.Add(ReconstructValue(item, buffers));
            return result;
        }
        return data;
    }

    private static byte[] ByteArrayFromSegment(ArraySegment<byte> seg)
    {
        var copy = new byte[seg.Count];
        System.Array.Copy(seg.Array!, seg.Offset, copy, 0, seg.Count);
        return copy;
    }
}
```

- [ ] **Step 4: Implement `Encoder` (string + binary paths)**

`Encoder.cs` (full):
```csharp
using System;
using System.Collections.Generic;
using System.Text.Json;
using SharpSocketIO.SocketIo.Parser.Commons;

namespace SharpSocketIO.SocketIo.Parser;

/// <summary>Port of class Encoder from lib/index.ts.</summary>
public sealed class Encoder
{
    private readonly Func<string, object?, object?>? _replacer;

    public Encoder(Func<string, object?, object?>? replacer = null)
    {
        _replacer = replacer;
    }

    /// <summary>
    /// Encode a packet. Returns a list whose first element is the encoded string and,
    /// for binary packets, the trailing byte[] attachments. (JS returns Array&lt;string|Buffer&gt;.)
    /// </summary>
    public IReadOnlyList<object> Encode(Packet obj)
    {
        if (obj.Type == PacketType.Event || obj.Type == PacketType.Ack)
        {
            if (IsBinaryHelper.HasBinary(obj.Data))
            {
                var binaryType = obj.Type == PacketType.Event ? PacketType.BinaryEvent : PacketType.BinaryAck;
                var deconstruct = BinaryPacket.DeconstructPacket(new Packet
                {
                    Type = binaryType,
                    Nsp = obj.Nsp,
                    Data = obj.Data,
                    Id = obj.Id,
                });
                return EncodeAsBinary(deconstruct.Packet, deconstruct.Buffers);
            }
        }
        return new[] { EncodeAsString(obj) };
    }

    private string EncodeAsString(Packet obj)
    {
        // first is type
        var str = "" + (int)obj.Type;

        // attachments if we have them
        if (obj.Type == PacketType.BinaryEvent || obj.Type == PacketType.BinaryAck)
        {
            str += obj.Attachments + "-";
        }

        // namespace
        if (!string.IsNullOrEmpty(obj.Nsp) && obj.Nsp != "/")
        {
            str += obj.Nsp + ",";
        }

        // id
        if (obj.Id.HasValue)
        {
            str += obj.Id.Value;
        }

        // json data
        if (obj.Data != null)
        {
            str += JsonSerializer.Serialize(obj.Data, JsonOptions.Default);
        }
        return str;
    }

    private IReadOnlyList<object> EncodeAsBinary(Packet packet, List<byte[]> buffers)
    {
        var pack = EncodeAsString(packet);
        var result = new List<object>(buffers.Count + 1) { pack };
        result.AddRange(buffers);
        return result;
    }
}
```

- [ ] **Step 5: Run, verify the encode-side tests for connect/disconnect/event/ack/connect_error pass (still need Decoder to round-trip)**

Run: `dotnet test`
Expected: round-trip tests still fail (Decoder stub throws), but encode behavior is in place.

- [ ] **Step 6: Commit**

```
git add -A
git commit -m "feat(parser): Encoder + IsBinary/HasBinary + binary deconstruct/reconstruct"
```

---

## Task P-4: Implement `Decoder` (state machine + BinaryReconstructor)

**Files:**
- Modify: `src/.../Decoder.cs`
- Test: extend `tests/.../ParserTests.cs` with the decode-error / reviver / isPacketValid tests

- [ ] **Step 1: Add the remaining `parser.js` tests (decode errors, reviver, isPacketValid, destroy)**

Append to `ParserTests.cs`:
```csharp
    [Fact]
    public void Throws_on_circular_object_encode()
    {
        // a.b = a; cycle
        var a = new Dictionary<string, object>();
        a["b"] = a;
        var data = new Packet { Type = PacketType.Event, Data = a, Id = 1, Nsp = "/" };
        var encoder = new Encoder();
        Assert.ThrowsAny<Exception>(() => encoder.Encode(data));
    }

    [Fact]
    public void Decodes_bad_binary_packet_throws_illegal()
    {
        var decoder = new Decoder();
        Assert.Throws<Exception>(() => decoder.Add("5"));
    }

    [Fact]
    public void Throws_when_too_many_attachments()
    {
        var decoder = new Decoder(new DecoderOptions { MaxAttachments = 2 });
        Assert.Throws<Exception>(() => decoder.Add(
            "53-[\"hello\",{\"_placeholder\":true,\"num\":0},{\"_placeholder\":true,\"num\":1},{\"_placeholder\":true,\"num\":2}]"));
    }

    [Fact]
    public void Decodes_with_custom_reviver_function()
    {
        var decoder = new Decoder((key, value) => key == "a" ? (value is string s ? s.ToUpper() : value) : value);
        Packet? got = null;
        decoder.On("decoded", args => got = (Packet)args[0]);
        decoder.Add("2[\"b\",{\"a\":\"val\"}]");
        Assert.NotNull(got);
        Assert.Equal("[\"b\",{\"a\":\"VAL\"}]", JsonSerializer.Serialize(got!.Data));
    }

    [Fact]
    public void Decodes_with_custom_reviver_options_object()
    {
        var decoder = new Decoder(new DecoderOptions
        {
            Reviver = (key, value) => key == "a" ? (value is string s ? s.ToUpper() : value) : value,
        });
        Packet? got = null;
        decoder.On("decoded", args => got = (Packet)args[0]);
        decoder.Add("2[\"b\",{\"a\":\"val\"}]");
        Assert.Equal("[\"b\",{\"a\":\"VAL\"}]", JsonSerializer.Serialize(got!.Data));
    }

    [Fact]
    public void Throws_on_invalid_payloads()
    {
        void Invalid(string str) =>
            Assert.Throws<Exception>(() => new Decoder().Add(str));

        Invalid("442[\"some\",\"data\"");
        Invalid("0/admin,\"invalid\"");
        Invalid("0[]");
        Invalid("1/admin,{}");
        Invalid("2/admin,\"invalid");
        Invalid("2/admin,{}");
        Invalid("2[{\"toString\":\"foo\"}]");
        Invalid("2[true,\"foo\"]");
        Invalid("2[null,\"bar\"]");
        Invalid("2[\"connect\"]");
        Invalid("2[\"disconnect\",\"123\"]");

        void IllegalAttachments(string str) =>
            Assert.Throws<Exception>(() => new Decoder().Add(str));
        IllegalAttachments("5");
        IllegalAttachments("51");
        IllegalAttachments("5a-");
        IllegalAttachments("51.23-");

        Assert.Throws<Exception>(() => new Decoder().Add("999"));
        Assert.Throws<Exception>(() => new Decoder().Add(999!));
    }

    [Fact]
    public void Resumes_decoding_after_destroy()
    {
        var decoder = new Decoder();
        Packet? got = null;
        decoder.On("decoded", args => got = (Packet)args[0]);
        decoder.Add("51-[\"hello\"]");
        Assert.Null(got); // waiting for buffer
        decoder.Destroy();
        decoder.Add("2[\"hello\"]");
        Assert.NotNull(got);
        Assert.Equal("[\"hello\"]", JsonSerializer.Serialize(got!.Data));
    }

    [Fact]
    public void IsPacketValid_rules()
    {
        Assert.True(SocketIoParser.IsPacketValid(new Packet { Type = PacketType.Connect, Nsp = "/" }));
        Assert.False(SocketIoParser.IsPacketValid(new Packet { Type = PacketType.Connect, Nsp = "/admin", Data = "invalid" }));
        Assert.False(SocketIoParser.IsPacketValid(new Packet { Type = PacketType.Connect, Nsp = "/", Data = new object[] { } }));
        Assert.False(SocketIoParser.IsPacketValid(new Packet { Type = PacketType.Disconnect, Nsp = "/admin", Data = new Dictionary<string, object>() }));
        Assert.False(SocketIoParser.IsPacketValid(new Packet { Type = PacketType.Event, Nsp = "/admin", Data = "invalid" }));
        Assert.False(SocketIoParser.IsPacketValid(new Packet { Type = PacketType.Event, Nsp = "/admin", Data = new Dictionary<string, object>() }));
        Assert.False(SocketIoParser.IsPacketValid(new Packet { Type = PacketType.Event, Nsp = "/", Data = new Dictionary<string, object> { ["toString"] = "foo" } }));
        Assert.False(SocketIoParser.IsPacketValid(new Packet { Type = PacketType.Event, Nsp = "/", Data = new object[] { true, "foo" } }));
        Assert.False(SocketIoParser.IsPacketValid(new Packet { Type = PacketType.Event, Nsp = "/", Data = new object[] { null, "bar" } }));
        Assert.False(SocketIoParser.IsPacketValid(new Packet { Type = PacketType.Event, Nsp = "/", Data = new object[] { "connect" } }));
        Assert.False(SocketIoParser.IsPacketValid(new Packet { Type = PacketType.Event, Nsp = "/", Data = new object[] { "disconnect", "123" } }));
    }
```

- [ ] **Step 2: Run, verify fail (red — Decoder stub + IsPacketValid missing)**

- [ ] **Step 3: Implement `DecoderOptions`**

`Commons/DecoderOptions.cs`:
```csharp
using System;

namespace SharpSocketIO.SocketIo.Parser.Commons;

/// <summary>Port of interface DecoderOptions.</summary>
public sealed class DecoderOptions
{
    public Func<string, object?, object?>? Reviver { get; set; }
    public int MaxAttachments { get; set; } = 10;
}
```

- [ ] **Step 4: Implement `Decoder`**

`Decoder.cs` (full):
```csharp
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text.Json;
using SharpSocketIO.ComponentEmitter;
using SharpSocketIO.SocketIo.Parser.Commons;

namespace SharpSocketIO.SocketIo.Parser;

/// <summary>Port of class Decoder from lib/index.ts.</summary>
public sealed class Decoder : Emitter<DecodedEvents>
{
    private BinaryReconstructor? _reconstructor;
    private readonly Func<string, object?, object?>? _reviver;
    private readonly int _maxAttachments;

    public Decoder() : this((DecoderOptions?)null) { }

    public Decoder(Func<string, object?, object?>? reviver)
    {
        _reviver = reviver;
        _maxAttachments = 10;
    }

    public Decoder(DecoderOptions? opts)
    {
        _reviver = opts?.Reviver;
        _maxAttachments = opts?.MaxAttachments ?? 10;
    }

    public void Add(object obj)
    {
        if (obj is string s)
        {
            if (_reconstructor != null)
                throw new Exception("got plaintext data when reconstructing a packet");
            var packet = DecodeString(s);
            bool isBinaryEvent = packet.Type == PacketType.BinaryEvent;
            if (isBinaryEvent || packet.Type == PacketType.BinaryAck)
            {
                packet.Type = isBinaryEvent ? PacketType.Event : PacketType.Ack;
                _reconstructor = new BinaryReconstructor(packet);
                if (packet.Attachments == 0)
                {
                    EmitReserved("decoded", packet);
                }
            }
            else
            {
                EmitReserved("decoded", packet);
            }
        }
        else if (IsBinaryHelper.IsBinary(obj))
        {
            if (_reconstructor == null)
                throw new Exception("got binary data when not reconstructing a packet");
            var bytes = obj is byte[] b ? b : ByteArrayFromSegment((ArraySegment<byte>)obj);
            var packet = _reconstructor.TakeBinaryData(bytes);
            if (packet != null)
            {
                _reconstructor = null;
                EmitReserved("decoded", packet);
            }
        }
        else
        {
            throw new Exception("Unknown type: " + obj);
        }
    }

    public void Destroy()
    {
        if (_reconstructor != null)
        {
            _reconstructor.FinishedReconstruction();
            _reconstructor = null;
        }
    }

    private Packet DecodeString(string str)
    {
        int i = 0;
        var p = new Packet { Type = (PacketType)(str[0] - '0') };

        if (!Enum.IsDefined(typeof(PacketType), p.Type))
            throw new Exception("unknown packet type " + (int)p.Type);

        if (p.Type == PacketType.BinaryEvent || p.Type == PacketType.BinaryAck)
        {
            int start = i + 1;
            while (++i < str.Length && str[i] != '-') { }
            var buf = str.Substring(start, i - start);
            if (!int.TryParse(buf, out int n) || str.Length <= i || str[i] != '-')
                throw new Exception("Illegal attachments");
            if (n < 0) throw new Exception("Illegal attachments");
            if (n > _maxAttachments) throw new Exception("too many attachments");
            p.Attachments = n;
        }

        if (i + 1 < str.Length && str[i + 1] == '/')
        {
            int start = i + 1;
            while (++i < str.Length && str[i] != ',') { }
            p.Nsp = str.Substring(start, i - start);
        }
        else
        {
            p.Nsp = "/";
        }

        if (i + 1 < str.Length)
        {
            char next = str[i + 1];
            if (next != '\0' && char.IsDigit(next))
            {
                int start = i + 1;
                while (++i < str.Length && char.IsDigit(str[i])) { }
                // i now points just past the last digit
                p.Id = int.Parse(str.Substring(start, i - start));
                i--; // back up so the JSON check below sees the char after the id
            }
        }

        if (++i < str.Length)
        {
            var payloadStr = str.Substring(i);
            var payload = TryParse(payloadStr);
            if (payload != null && IsPayloadValid(p.Type, payload))
            {
                p.Data = payload;
            }
            else
            {
                throw new Exception("invalid payload");
            }
        }

        return p;
    }

    private object? TryParse(string str)
    {
        try
        {
            var element = JsonSerializer.Deserialize<JsonElement>(str, JsonOptions.Default);
            return ApplyReviver(ConvertElement(element));
        }
        catch (JsonException)
        {
            return null;
        }
    }

    // Convert JsonElement to plain object?/Dictionary/list so reviver & binary deconstruct see CLR shapes.
    private static object? ConvertElement(JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Undefined:
            case JsonValueKind.Null:
                return null;
            case JsonValueKind.True:
                return true;
            case JsonValueKind.False:
                return false;
            case JsonValueKind.Number:
                if (element.TryGetInt64(out long l)) return l > int.MaxValue || l < int.MinValue ? (object)l : (object)(int)l;
                return element.GetDouble();
            case JsonValueKind.String:
                return element.GetString();
            case JsonValueKind.Array:
            {
                var list = new List<object?>();
                foreach (var item in element.EnumerateArray()) list.Add(ConvertElement(item));
                return list;
            }
            case JsonValueKind.Object:
            {
                var dict = new Dictionary<string, object?>();
                foreach (var prop in element.EnumerateObject()) dict[prop.Name] = ConvertElement(prop.Value);
                return dict;
            }
            default:
                return null;
        }
    }

    private object? ApplyReviver(object? value)
    {
        if (_reviver == null) return value;
        return ApplyReviverRecursive("", value);
    }

    private object? ApplyReviverRecursive(string key, object? value)
    {
        if (value is IDictionary dict)
        {
            var keys = new List<string>(dict.Keys.Count);
            foreach (DictionaryEntry kv in dict) keys.Add((string)kv.Key);
            foreach (var k in keys)
            {
                dict[k] = ApplyReviverRecursive(k, dict[k]);
            }
        }
        else if (value is IList list)
        {
            for (int idx = 0; idx < list.Count; idx++)
            {
                list[idx] = ApplyReviverRecursive("", list[idx]);
            }
        }
        return _reviver(key, value);
    }

    private static bool IsPayloadValid(PacketType type, object? payload)
    {
        switch (type)
        {
            case PacketType.Connect:
                return IsPlainObject(payload);
            case PacketType.Disconnect:
                return payload == null;
            case PacketType.ConnectError:
                return payload is string || IsPlainObject(payload);
            case PacketType.Event:
            case PacketType.BinaryEvent:
                return payload is IList arr && arr.Count > 0 &&
                       (arr[0] is int || arr[0] is long ||
                        (arr[0] is string s0 && !ReservedEvents.Contains(s0)));
            case PacketType.Ack:
            case PacketType.BinaryAck:
                return payload is IList;
            default:
                return false;
        }
    }

    private static bool IsPlainObject(object? obj) => obj is IDictionary<string, object?>;

    private static byte[] ByteArrayFromSegment(ArraySegment<byte> seg)
    {
        var copy = new byte[seg.Count];
        Array.Copy(seg.Array!, seg.Offset, copy, 0, seg.Count);
        return copy;
    }

    private sealed class BinaryReconstructor
    {
        private readonly Packet _reconPack;
        private readonly List<byte[]> _buffers = new();

        public BinaryReconstructor(Packet packet) { _reconPack = packet; }

        public Packet? TakeBinaryData(byte[] binData)
        {
            _buffers.Add(binData);
            if (_buffers.Count == _reconPack.Attachments)
            {
                var packet = BinaryPacket.ReconstructPacket(_reconPack, _buffers);
                FinishedReconstruction();
                return packet;
            }
            return null;
        }

        public void FinishedReconstruction()
        {
            _buffers.Clear();
        }
    }
}
```

- [ ] **Step 5: Add `IsPacketValid` to `SocketIoParser`**

`SocketIoParser.cs` (extend):
```csharp
using SharpSocketIO.SocketIo.Parser.Commons;

namespace SharpSocketIO.SocketIo.Parser;

public static class SocketIoParser
{
    public const int Protocol = 5;

    public static bool IsPacketValid(Packet packet) =>
        IsNamespaceValid(packet.Nsp) && IsAckIdValid(packet.Id) && IsDataValid(packet.Type, packet.Data);

    private static bool IsNamespaceValid(string? nsp) => nsp is string;

    private static bool IsAckIdValid(int? id) => !id.HasValue || IsInteger(id.Value);

    private static bool IsInteger(double v) => !double.IsInfinity(v) && Math.Floor(v) == v;

    private static bool IsDataValid(PacketType type, object? payload)
    {
        switch (type)
        {
            case PacketType.Connect:
                return payload == null || IsPlainObject(payload);
            case PacketType.Disconnect:
                return payload == null;
            case PacketType.Event:
                return payload is System.Collections.IList arr && arr.Count > 0 &&
                       (arr[0] is int || arr[0] is long ||
                        (arr[0] is string s0 && !ReservedEvents.Contains(s0)));
            case PacketType.Ack:
                return payload is System.Collections.IList;
            case PacketType.ConnectError:
                return payload is string || IsPlainObject(payload);
            default:
                return false;
        }
    }

    private static bool IsPlainObject(object? obj) => obj is System.Collections.IDictionary<string, object?>;
}
```

- [ ] **Step 6: Run, verify all `parser.js` tests pass**

Run: `dotnet test`
Expected: all ParserTests PASS.

- [ ] **Step 7: Commit**

```
git add -A
git commit -m "feat(parser): Decoder state machine + BinaryReconstructor + IsPacketValid"
```

---

## Task P-5: Port `test/buffer.js` + `test/arraybuffer.js` (binary round-trip tests)

**Files:**
- Create: `tests/.../BufferTests.cs`
- Create: `tests/.../ArrayBufferTests.cs`

- [ ] **Step 1: Write `BufferTests.cs` (port of test/buffer.js; Buffer → byte[])**

`tests/.../BufferTests.cs`:
```csharp
using System.Collections.Generic;
using System.Text;
using SharpSocketIO.SocketIo.Parser;
using SharpSocketIO.SocketIo.Parser.Commons;
using Xunit;

namespace SharpSocketIO.SocketIo.Parser.Tests;

// Port of test/buffer.js — Buffer maps to byte[] in .NET.
public class BufferTests
{
    private static byte[] Buf(string s) => Encoding.UTF8.GetBytes(s);

    internal static void TestBin(Packet obj)
    {
        var encoder = new Encoder();
        var encoded = encoder.Encode(obj);
        Packet? decoded = null;
        var decoder = new Decoder();
        decoder.On("decoded", args => decoded = (Packet)args[0]);
        foreach (var part in encoded) decoder.Add(part);
        Assert.NotNull(decoded);
        ParserTests.AssertEqual(obj, decoded!);
    }

    [Fact]
    public void Encodes_a_Buffer()
    {
        TestBin(new Packet { Type = PacketType.Event, Data = new object[] { "a", Buf("abc") }, Id = 23, Nsp = "/cool" });
    }

    [Fact]
    public void Encodes_a_nested_Buffer()
    {
        TestBin(new Packet
        {
            Type = PacketType.Event,
            Data = new object[] { "a", new Dictionary<string, object> { ["b"] = new object[] { "c", Buf("abc") } } },
            Id = 23,
            Nsp = "/cool",
        });
    }

    [Fact]
    public void Encodes_a_binary_ack_with_Buffer()
    {
        TestBin(new Packet
        {
            Type = PacketType.Ack,
            Data = new object[] { "a", Buf("xxx"), new Dictionary<string, object>() },
            Id = 127,
            Nsp = "/back",
        });
    }

    [Fact]
    public void Throws_on_attachment_with_invalid_num_string()
    {
        var decoder = new Decoder();
        Assert.Throws<Exception>(() =>
        {
            decoder.Add("51-[\"hello\",{\"_placeholder\":true,\"num\":\"splice\"}]");
            decoder.Add(Buf("world"));
        });
    }

    [Fact]
    public void Throws_on_attachment_out_of_bound()
    {
        var decoder = new Decoder();
        Assert.Throws<Exception>(() =>
        {
            decoder.Add("51-[\"hello\",{\"_placeholder\":true,\"num\":1}]");
            decoder.Add(Buf("world"));
        });
    }

    [Fact]
    public void Throws_on_binary_without_header()
    {
        var decoder = new Decoder();
        Assert.Throws<Exception>(() => decoder.Add(Buf("world")));
    }

    [Fact]
    public void Throws_on_plaintext_when_reconstructing()
    {
        var decoder = new Decoder();
        Assert.Throws<Exception>(() =>
        {
            decoder.Add("51-[\"hello\",{\"_placeholder\":true,\"num\":0}]");
            decoder.Add("2[\"hello\"]");
        });
    }
}
```

- [ ] **Step 2: Write `ArrayBufferTests.cs` (port of test/arraybuffer.js; ArrayBuffer/TypedArray → byte[]; Object.create(null) → Dictionary)**

`tests/.../ArrayBufferTests.cs`:
```csharp
using System.Collections.Generic;
using SharpSocketIO.SocketIo.Parser;
using SharpSocketIO.SocketIo.Parser.Commons;
using Xunit;

namespace SharpSocketIO.SocketIo.Parser.Tests;

// Port of test/arraybuffer.js. ArrayBuffer/Uint8Array map to byte[]. The
// "Object.create(null)" null-prototype object maps to Dictionary<string,object>.
public class ArrayBufferTests
{
    private static byte[] Ab(int n) => new byte[n];

    private static void TestBin(Packet obj)
    {
        var encoder = new Encoder();
        var encoded = encoder.Encode(obj);
        Packet? decoded = null;
        var decoder = new Decoder();
        decoder.On("decoded", args => decoded = (Packet)args[0]);
        foreach (var part in encoded) decoder.Add(part);
        Assert.NotNull(decoded);
        ParserTests.AssertEqual(obj, decoded!);
    }

    [Fact]
    public void Encodes_an_ArrayBuffer()
    {
        TestBin(new Packet { Type = PacketType.Event, Data = new object[] { "a", Ab(2) }, Id = 0, Nsp = "/" });
    }

    [Fact]
    public void Encodes_an_ArrayBuffer_in_a_dictionary()
    {
        TestBin(new Packet
        {
            Type = PacketType.Event,
            Data = new object[] { "a", new Dictionary<string, object> { ["array"] = Ab(2) } },
            Id = 0,
            Nsp = "/",
        });
    }

    [Fact]
    public void Encodes_a_TypedArray_as_bytes()
    {
        var arr = new byte[] { 0, 1, 2, 3, 4 };
        TestBin(new Packet { Type = PacketType.Event, Data = new object[] { "a", arr }, Id = 0, Nsp = "/" });
    }

    [Fact]
    public void Encodes_ArrayBuffers_deep_in_JSON()
    {
        TestBin(new Packet
        {
            Type = PacketType.Event,
            Data = new object[]
            {
                "a",
                new Dictionary<string, object>
                {
                    ["a"] = "hi",
                    ["b"] = new Dictionary<string, object> { ["why"] = Ab(3) },
                    ["c"] = new Dictionary<string, object> { ["a"] = "bye", ["b"] = new Dictionary<string, object> { ["a"] = Ab(6) } },
                },
            },
            Id = 999,
            Nsp = "/deep",
        });
    }

    [Fact]
    public void Encodes_deep_binary_with_null_values()
    {
        TestBin(new Packet
        {
            Type = PacketType.Event,
            Data = new object[]
            {
                "a",
                new Dictionary<string, object?>
                {
                    ["a"] = "b",
                    ["c"] = 4,
                    ["e"] = new Dictionary<string, object?> { ["g"] = null },
                    ["h"] = Ab(9),
                },
            },
            Nsp = "/",
            Id = 600,
        });
    }

    [Fact]
    public void Does_not_modify_the_input_packet()
    {
        var packet = new Packet
        {
            Type = PacketType.Event,
            Nsp = "/",
            Data = new object[] { "a", new byte[] { 1, 2, 3 }, new byte[] { 4, 5, 6 } },
        };
        var before = System.Text.Json.JsonSerializer.Serialize(packet.Data);
        var encoder = new Encoder();
        encoder.Encode(packet);
        var after = System.Text.Json.JsonSerializer.Serialize(packet.Data);
        Assert.Equal(before, after);
    }
}
```

- [ ] **Step 3: Run, verify pass**

Run: `dotnet test`
Expected: BufferTests + ArrayBufferTests + ParserTests all PASS.

- [ ] **Step 4: Commit**

```
git add -A
git commit -m "test(parser): port socket.io-parser buffer.js + arraybuffer.js test suites"
```

---

## Task P-6: Final verification

- [ ] **Step 1: Clean rebuild + Release test across all TFMs**

Run:
```
dotnet build -c Release
dotnet test -c Release
```
Expected: build 0 warnings/0 errors; all tests pass on net8/9/10 (component-emitter + socket.io-parser).

- [ ] **Step 2: Confirm wire-format golden strings hold**

Manually trace that these exact encoded strings are produced by passing tests:
- CONNECT: `"0/woot,{\"token\":\"123\"}"`
- EVENT: `"2[\"a\",1,{}]"`
- EVENT with ack/id/nsp: `"2/test,1[\"a\",1,{}]"`
- ACK: `"3123[\"a\",1,{}]"`
- CONNECT_ERROR: `"4\"Unauthorized\""`
- BINARY_EVENT with 1 attachment: `"51-[\"hello\",...]"`

- [ ] **Step 3: Commit any cleanup**

```
git add -A
git commit -m "chore: final verification across TFMs" --allow-empty
```

---

## Self-review

**Spec coverage:** spec §1 DoD 1–7 → CE-1/CE-2 (emitter), P-1 (projects), P-2 (types), P-3 (Encoder+IsBinary+Binary), P-4 (Decoder+IsPacketValid), P-5 (binary tests), P-6 (verify). §3 type mapping fully covered. §4 wire-format rules → Encoder.encodeAsString (P-3) + Decoder.DecodeString (P-4). §6 deviations (no Blob, Emitter typing, EncodedPart, JSON, circular, null-proto, ToJSON) all addressed.

**Placeholder scan:** none — all code steps carry complete code. (Note: P-3 step 2 contains a deliberately self-corrected draft followed by the corrected version; executor should use the corrected `IsBinary.cs`.)

**Type consistency:** `Encoder.Encode` → `IReadOnlyList<object>` (P-3) used in test helpers (P-2, P-5). `Decoder.Add(object)` (P-4) consumed by same helpers. `Packet` fields (Type/Nsp/Data/Id/Attachments) consistent across P-2..P-5. `IsBinaryHelper.IsBinary/HasBinary`, `BinaryPacket.DeconstructPacket/ReconstructPacket`, `SocketIoParser.IsPacketValid` names consistent. `DecodedEvents` marker (P-2) referenced by `Decoder : Emitter<DecodedEvents>` (P-4).

## Execution Handoff

Plan saved to `docs/superpowers/plans/2026-06-18-socketio-parser-port.md`. Inline execution (single-developer TDD, fast red→green loops). Proceeding inline.
