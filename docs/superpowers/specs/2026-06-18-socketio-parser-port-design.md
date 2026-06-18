# Design: Port `socket.io-component-emitter` (cycle 1.5) + `socket.io-parser` (cycle 2) to .NET (C#)

**Date:** 2026-06-18
**Scope:** Second cycle(s) of the larger "port socket.io v4 monorepo to .NET" objective.
**Upstream sources:**
- `packages/socket.io-component-emitter` (component-emitter, ~176 LoC source)
- `packages/socket.io-parser` v4.2.6 (socket.io protocol parser, ~440 LoC source)

`socket.io-parser` depends on `@socket.io/component-emitter` (its `Decoder extends Emitter`).
Per the bottom-up principle established in cycle 1, the dependency goes first — hence
component-emitter is cycle 1.5 and socket.io-parser is cycle 2, both delivered in this spec.

Both are pure logic (no transport I/O). Later cycles: `engine.io` + `engine.io-client`
(transports), then `socket.io` + `socket.io-client`.

---

## 1. Goal & success criteria

Port **both** packages as-is to idiomatic C#, multi-targeted to recent .NET, preserving
**every** test case from upstream Mocha suites (ported to xUnit), test-first.

**Definition of done:**

1. Two new library projects:
   - `SharpSocketIO.ComponentEmitter` (assembly, port of `component-emitter`)
   - `SharpSocketIO.SocketIo.Parser` (assembly, port of `socket.io-parser` v4.2.6)
   Both added to `SharpSocketIO.sln`.
2. **ComponentEmitter** exposes: `Emitter<TEvents>` base class with `On`, `Once`, `Off`
   (all 3 arities), `Off`-all, `Emit`, `EmitReserved`, `Listeners`, `HasListeners`, plus
   the `Emitter(obj)` mixin helper analogue. (See §3 for the C# adaptation.)
3. **SocketIo.Parser** exposes: `PacketType` enum (protocol 5), `Packet`, `Encoder`
   (`Encode` → `string[]`), `Decoder : Emitter<{Decoded}>` (`Add`, `Destroy`), `IsPacketValid`,
   plus the binary deconstruct/reconstruct and `IsBinary`/`HasBinary` helpers.
4. Every test in upstream `socket.io-parser` (`test/parser.js`, `test/buffer.js`,
   `test/arraybuffer.js`) and `socket.io-component-emitter` (`test/emitter.js`) has a
   corresponding ported xUnit test. Blob-only tests (`test/blob.js`) are omitted
   (no .NET Blob primitive — same rationale as cycle 1, spec §6.1).
5. `dotnet test` passes with all ported tests green across net8.0/net9.0/net10.0.
6. Wire format is byte-for-byte compatible: golden strings from upstream tests
   (`'0/woot,{"token":"123"}'`, `'2["a",1,{}]'`, `'51-["hello"]...'`, etc.) are reproduced.
7. Work committed to git on the feature branch.

---

## 2. Target frameworks & tooling

Same as cycle 1: `net10.0; net9.0; net8.0; netstandard2.1` for libraries; tests on
`net10.0; net9.0; net8.0`. xUnit + `Microsoft.NET.Test.Sdk`. `Directory.Build.props`
already centralizes LangVersion/Nullable/TreatWarningsAsErrors.

**New projects layout** (extends cycle 1 monorepo):

```
src/
  SharpSocketIO.ComponentEmitter/
    SharpSocketIO.ComponentEmitter.csproj
    Emitter.cs                  // Emitter<TEvents> base + reserved/typed-emit helpers
    EmitterMixin.cs             // static MixIn / functional helper analogue
  SharpSocketIO.SocketIo.Parser/
    SharpSocketIO.SocketIo.Parser.csproj
    Commons/PacketType.cs       // enum CONNECT=0..BINARY_ACK=6
    Commons/Packet.cs           // record-like, mutable Nsp/Data/Id/Attachments
    Commons/ReservedEvents.cs   // reserved event names list
    Commons/JsonOptions.cs      // shared JsonSerializerOptions
    IsBinary.cs                 // IsBinary(object?) + HasBinary(object?)
    Binary.cs                   // DeconstructPacket / ReconstructPacket
    Encoder.cs                  // Encoder class (Encode → string[])
    Decoder.cs                  // Decoder : Emitter<DecodedEvents>
    SocketIoParser.cs           // protocol const + IsPacketValid + re-exports
tests/
  SharpSocketIO.ComponentEmitter.Tests/
    EmitterTests.cs             // ← test/emitter.js
  SharpSocketIO.SocketIo.Parser.Tests/
    ParserTests.cs              // ← test/parser.js
    BufferTests.cs              // ← test/buffer.js
    ArrayBufferTests.cs         // ← test/arraybuffer.js
    Helpers.cs                  // ← test/helpers.js (Test / TestBin)
```

`SharpSocketIO.SocketIo.Parser` references `SharpSocketIO.ComponentEmitter`. Neither
references `SharpSocketIO.EngineIo.Parser` (socket.io-parser sits at a *different*
protocol layer than engine.io-parser; they're independent codecs).

---

## 3. Type & API mapping (JS → C#)

### ComponentEmitter

JS's emitter is a prototype-mixin (`Emitter(obj)` mutates an arbitrary object). C#
idiom is a generic base class `Emitter<TEvents>` where `TEvents` describes the typed
event map (matching the upstream `Emitter<{}, {}, DecoderReservedEvents>` pattern).
Since the upstream tests use untyped `.on('foo', fn)` + `.emit('foo', val)` and we need
1:1 test ports, we provide **string-keyed** dispatch (event names are `string`) while
still being generic so subclasses can declare a marker events type.

| JS | C# |
|---|---|
| `Emitter(obj)` mixin | `static class Emitter { static T MixIn<T>(T obj) ... }` — adds an `Emitter` field to `obj` if needed. (In practice the C# tests subclass `Emitter<UnitEvents>` directly, mirroring JS's `new Emitter`.) |
| `on(event, fn)` / `addEventListener` | `Emitter On(string event, Action fn)` and `On(string event, Action<object[]> fn)` overloads. Returns `this`. |
| `once(event, fn)` | `Emitter Once(string event, Action<object[]> fn)` — wraps a self-removing handler. |
| `off(event, fn)` (and 3 aliases) | `Emitter Off(string event, Delegate fn)` — handles the `once` `.fn` unwrap. |
| `off(event)` | `Emitter Off(string event)` — removes all handlers for event. |
| `off()` | `Emitter Off()` — clears everything. |
| `emit(event, ...args)` | `Emitter Emit(string event, params object[] args)` — iterates a snapshot (`callbacks.slice(0)` in JS) so handlers can mutate the list during emit. |
| `emitReserved` | `EmitReserved` — alias of `Emit`. |
| `listeners(event)` | `IReadOnlyList<Delegate> Listeners(string event)` — empty list if none. |
| `hasListeners(event)` | `bool HasListeners(string event)`. |

Event-name keys are stored in a `Dictionary<string, List<Delegate>>`. The JS
"constructor"/"__proto__" same-name-as-Object.prototype test maps trivially since C#
`Dictionary` keys are arbitrary strings.

### SocketIo.Parser

| JS (`lib/index.ts`) | C# |
|---|---|
| `enum PacketType { CONNECT, DISCONNECT, EVENT, ACK, CONNECT_ERROR, BINARY_EVENT, BINARY_ACK }` | `enum PacketType { Connect=0, Disconnect=1, Event=2, Ack=3, ConnectError=4, BinaryEvent=5, BinaryAck=6 }` |
| `protocol = 5` | `public const int Protocol = 5;` |
| `interface Packet { type; nsp; data?; id?; attachments? }` | `sealed class Packet { PacketType Type; string Nsp="/"; object? Data; int? Id; int? Attachments; }` with value equality for the test's `.eql(packet)` assertions. **Mutable** Nsp/Data/etc because JS mutates them in deconstruct/reconstruct (and the `should not modify the input packet` test asserts encode doesn't mutate). |
| `class Encoder { encode(obj) → string[]\|Buffer[] }` | `sealed class Encoder { Encoder(Func replacer=null); string[] Encode(Packet obj); }`. Returns `string[]` always (binary buffers are represented as their placeholder-prefixed strings for the test helper which feeds them back to the decoder; but to preserve binary round-trip we need raw bytes too). **Resolution:** `Encode` returns `IReadOnlyList<EncodedPart>` where `EncodedPart` is a discriminated `string` or `byte[]`, exactly mirroring JS's mixed `Array<string|Buffer>` return. The test helper iterates both kinds into `decoder.Add(part)`. |
| `class Decoder extends Emitter<Decoded>` | `sealed class Decoder : Emitter<DecodedEvents> { Decoder(DecoderOptions?); void Add(object part); void Destroy(); }` where `DecodedEvents { void Decoded(Packet p); }` is the typed event marker. The test helper does `decoder.On("decoded", p => ...)`. |
| `JSON.stringify` / `JSON.parse` | `System.Text.Json.JsonSerializer` with options: **no** camel-casing (JS preserves the exact object keys), indented=false, default number handling. Numbers: JS treats all numbers as doubles; `JsonSerializer` will emit `1` for integer-valued doubles — must verify against golden `'2["a",1,{}]'`. |
| `isBinary` / `hasBinary` | `IsBinary(object?)` returns true for `byte[]` or `ArrayBuffer` (Blob/File have no .NET primitive; absorbed as bytes). `HasBinary(object?)` deep-scans arrays/objects, calls `obj.ToJson()` if a `ToJSON()` method exists. |
| `deconstructPacket` / `reconstructPacket` | `DeconstructPacket` / `ReconstructPacket` returning `{ Packet, List<byte[]> Buffers }`. Binary values become `{ _placeholder: true, num: n }` JSON objects (so they survive `JsonSerializer` round-trip); reconstructed back. |
| `isPacketValid(packet)` | `bool IsPacketValid(Packet)` — namespace/ack-id/data validation rules ported verbatim. |
| RESERVED_EVENTS list | `static readonly string[] ReservedEvents`. |
| `BinaryReconstructor` | nested/private class in `Decoder`, same `takeBinaryData`/`finishedReconstruction` logic. |

---

## 4. Behavior to preserve verbatim

### Encoder wire format (`encodeAsString`)
1. `str = "" + type` (digit 0–6)
2. If BINARY_EVENT/BINARY_ACK: `str += attachments + "-"`
3. If `nsp && nsp != "/"`: `str += nsp + ","`
4. If `id != null`: `str += id`
5. If `data != null`: `str += JSON.stringify(data)`

`encode()` returns `[encodeAsString(obj)]` for non-binary, or
`[encodeAsString(deconstructedPacket), ...buffers]` for binary (buffers **prepended** by
the string — `buffers.unshift(pack)`). Wait — re-reading: `buffers.unshift(pack)` puts the
**string first**, then the buffers. So encoded output is `[packString, buf0, buf1, ...]`.
The decoder `add()` is called with each in order.

### Decoder `add(obj)`
- string + reconstructor active → throw `"got plaintext data when reconstructing a packet"`
- string → `decodeString`; if BINARY_EVENT/BINARY_ACK, flip type to EVENT/ACK and create
  reconstructor; if `attachments==0` emit decoded immediately; else wait for buffers.
- binary (`isBinary(obj) || obj.base64`) + no reconstructor → throw `"got binary data when not reconstructing a packet"`
- binary + reconstructor → `takeBinaryData`; if returns a packet, emit decoded, clear reconstructor.
- else → throw `"Unknown type: " + obj`.

### Decoder `decodeString(str)` — the parser state machine
1. `type = Number(str[0])`; if `PacketType[type]` undefined → throw `"unknown packet type " + type`.
2. If BINARY_EVENT/BINARY_ACK: parse digits until `-`; must be integer ≥0 and ≤
   `maxAttachments` (default 10); else throw `"Illegal attachments"` / `"too many attachments"`.
3. If next char `/`: parse namespace until `,` (or end); else `nsp = "/"`.
4. If next char is a number: parse id digits.
5. If more chars: `tryParse` JSON; if parse fails OR `isPayloadValid` returns false → throw `"invalid payload"`.

### `isPayloadValid(type, payload)` rules
- CONNECT → payload is a plain object
- DISCONNECT → payload undefined
- CONNECT_ERROR → string or plain object
- EVENT / BINARY_EVENT → array whose [0] is number OR (string not in RESERVED_EVENTS)
- ACK / BINARY_ACK → array

### Binary deconstruct/reconstruct
Walk data; each binary leaf becomes `{_placeholder:true, num:n}` and is pushed to buffers.
`reconstructPacket` walks again, replacing placeholders with `buffers[num]` (throw
`"illegal attachments"` if num invalid). After reconstruct, `delete packet.attachments`.

---

## 5. Development approach (TDD)

Per-package red→green, mirroring cycle 1:
1. component-emitter: tests first (compile-fail), then minimal `Emitter`, green.
2. socket.io-parser: project skeleton + tests first, then types, then Encoder, then
   Decoder, then binary helpers, then IsPacketValid. Each step flips a test group green.
3. `dotnet test` all green; commit per logical step.

---

## 6. Adaptations & explicit deviations

1. **No Blob/File.** Same as cycle 1. `test/blob.js` (3 tests) omitted; `BinaryType.Blob`
   not produced. Binary attachments in .NET are `byte[]` (or `ArrayBuffer`).
2. **Emitter typing.** JS's untyped mixin becomes a generic `Emitter<TEvents>` base
   with string-keyed dispatch, so the untyped upstream tests port directly while
   downstream code (Decoder) gets the typed-events marker.
3. **`Encoder.Encode` return type.** JS returns a mixed `Array<string|Buffer>`. We expose
   `IReadOnlyList<EncodedPart>` (sum of `string` | `byte[]`). The test helper feeds each
   part to `Decoder.Add(object)`, which dispatches by runtime type — exactly as JS's
   `typeof obj === "string"` check.
4. **`JSON` ↔ `System.Text.Json`.** Same default semantics; key order preserved (insertion
   order for objects, which `JsonObject`/`JsonSerializer` honor for `Dictionary<string,object>`
   via `JsonNode`). Numbers: integers serialize without `.0`.
5. **Circular-reference encode.** JS throws on `JSON.stringify(circular)`. C# `JsonSerializer`
   throws `JsonException` for cycles → `Encoder.Encode` lets it propagate (test asserts throw).
6. **`Object.create(null)` (null-prototype object) test.** A plain object with no prototype.
   In C# this maps to a `Dictionary<string,object>` (or `JsonObject`), which round-trips as
   a JSON object — same observable behavior. The test only checks encode/decode equality.
7. **`hasBinary` `toJSON` recursion.** `HasBinary` calls `obj.ToJSON()` if such a method
   exists (C# convention: a public parameterless `ToJSON()` returning a serializable value).

---

## 7. Out of scope for these cycles

- `engine.io`, `engine.io-client`, `socket.io`, `socket.io-client`, adapters, msgpack,
  postgres/redis emitters — all later cycles.
- Any HTTP/WebSocket transport.
- The `bench/` and `wdio` (browser-runner) configs.

## 8. Risks

- **Number formatting in JSON.** JS `JSON.stringify(1)` → `"1"`. `System.Text.Json`
  serializes `double 1.0` → `"1"` only with care. We'll back packet data with
  `JsonNode`/`JsonElement` so round-trip is lossless and the golden strings hold.
- **Object key equality.** `expect(packet).to.eql(obj)` deep-compares including key sets.
  Our `Packet` value-equality and the JSON round-trip must preserve key order/sets.

---

## Next step

writing-plans → implementation plan (component-emitter tasks first, then parser), then TDD.
