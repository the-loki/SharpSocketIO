# Design: Port `engine.io-parser` (socket.io v4 / parser v5.2.3) to .NET (C#)

**Date:** 2026-06-18
**Scope:** First cycle of the larger "port socket.io v4 monorepo to .NET" objective.
**Upstream source:** `packages/engine.io-parser` from `socketio/socket.io` @ `main`
(upstream package version `5.2.3`, protocol version `4`).

This is the bottom-up foundation: a pure packet codec with no transport I/O.
Later cycles will build on this:
`socket.io-parser` → `engine.io` + `engine.io-client` (transports) →
`socket.io` + `socket.io-client` → adapters / extras.

---

## 1. Goal & success criteria

Port `engine.io-parser` **as-is** to idiomatic, modern C#, compatible with recent
.NET targets, preserving **every** test case from the upstream Mocha suite
(ported to xUnit), developed test-first (red → green).

**Definition of done for this cycle:**

1. A .NET solution `SharpSocketIO.sln` exists at the repo root.
2. A library project produces the `SharpSocketIO.EngineIo.Parser` assembly,
   covering: `Packet`, `PacketType`, `RawData`, `BinaryType`,
   `encodePacket`, `encodePacketToBinary`, `decodePacket`,
   `encodePayload`, `decodePayload`, `createPacketEncoderStream`,
   `createPacketDecoderStream`, plus `ERROR_PACKET` and `protocol = 4`.
3. Every test in upstream `test/index.ts`, `test/node.ts`, and `test/browser.ts`
   has a corresponding, ported xUnit test (browser-only Blob tests are adapted —
   see §6).
4. `dotnet test` passes with all ported tests green.
5. Wire format is byte-for-byte compatible with the JS parser (verified by the
   golden-string assertions carried over from the upstream tests, e.g.
   `"0\x1e1\x1e2probe\x1e3probe\x1e4test"` and `"bAQIDBA=="`).
6. Work committed to git on a feature branch; `main` left clean.

---

## 2. Target frameworks & tooling

**Target frameworks (multi-target):** `net8.0; net9.0; net10.0`
plus `netstandard2.1` for broad reach (covers .NET Core 3.x, .NET 5–7, and
Mono/Xamarin-era consumers without dragging in polyfills).

- The installed SDKs are 8, 9, and 10; these are the LTS/current lines "of the
  last few years" the objective calls for.
- `netstandard2.1` keeps the library usable on slightly older runtimes and on
  non-Microsoft .NET implementations; all APIs we need
  (`Span<T>`, `MemoryStream`, `ArraySegment`, `TextEncoder`-free UTF-8 via
  `Encoding.UTF8`, `Convert.ToBase64String`) are available there.

**Test SDK:** xUnit (`xunit` + `xunit.runner.visualstudio`) + `Microsoft.NET.Test.Sdk`.
Tests are multi-targeted the same way as the library so each TFM is exercised.

**Language version:** `latest` (C# 12/13 features are fine since the build SDK is 8+).

**Solution layout** (single-solution, per-package projects — mirrors upstream's
per-package monorepo so subsequent cycles slot in cleanly):

```
SharpSocketIO.sln
src/
  SharpSocketIO.EngineIo.Parser/
    SharpSocketIO.EngineIo.Parser.csproj
    Commons/PacketType.cs
    Commons/Packet.cs
    Commons/RawData.cs
    Commons/BinaryType.cs
    Commons/ErrorPacket.cs
    Commons/PacketTypeMap.cs
    Contrib/Base64ArrayBuffer.cs
    EncodePacket.cs
    EncodePacketToBinary.cs
    DecodePacket.cs
    EngineIoParser.cs          // encodePayload / decodePayload / streams / public re-exports
    PacketEncoderStream.cs
    PacketDecoderStream.cs
tests/
  SharpSocketIO.EngineIo.Parser.Tests/
    SharpSocketIO.EngineIo.Parser.Tests.csproj
    TestUtil.cs                // areArraysEqual, createArrayBuffer
    IndexTests.cs              // from test/index.ts
    NodeTests.cs               // from test/node.ts
    BrowserTests.cs            // from test/browser.ts (adapted)
_upstream/                      # git-ignored clone of socketio/socket.io (reference only)
docs/superpowers/specs/         # this document + future cycle specs
Directory.Build.props          # central LangVersion, nullable, treat warnings as errors, etc.
```

The `_upstream/` clone used as reference is excluded from the repo via `.gitignore`.

---

## 3. Type & API mapping (JS → C#)

The core design decision is how to represent the union type `RawData = string |
Buffer | ArrayBuffer | ArrayBufferView | Blob`, since C# has no clean equivalent
of `data instanceof ArrayBuffer` over a dynamic value.

**Approach: typed `object?`-backed `RawData` plus an `IRawData` discriminator,
with extension helpers.** This keeps the public surface faithful to the JS
"accept any of these shapes" contract while staying type-safe at call sites and
trivially serializable for the transport layers later.

| JS (commons.ts) | C# |
|---|---|
| `type PacketType = "open"\|"close"\|"ping"\|"pong"\|"message"\|"upgrade"\|"noop"\|"error"` | `enum PacketType { Open=0, Close=1, Ping=2, Pong=3, Message=4, Upgrade=5, Noop=6, Error }` — numeric values match the on-wire codes so `PACKET_TYPES`/`PACKET_TYPES_REVERSE` collapses to one map. |
| `interface Packet { type; options?; data? }` | `sealed class Packet { PacketType Type; PacketOptions? Options; RawData? Data; }` with value equality (`IEquatable<Packet>`) so the ported `expect(...).to.eql(packet)` deep-equality assertions map directly to `Assert.Equal`. |
| `RawData = any` | `readonly struct RawData { public readonly object? Value; public readonly RawDataKind Kind; }` where `Kind ∈ { None, String, Int32, ByteArray, ArrayBuffer }`. Constructors accept `string`, `int`, `byte[]`, `ArraySegment<byte>`, `Array`. `ArrayBuffer` is modelled as a C# `ArrayBuffer` value type wrapping `byte[] + offset + length` (mirrors JS `ArrayBuffer`/typed-array-with-offset cases the tests exercise). |
| `BinaryType = "nodebuffer"\|"arraybuffer"\|"blob"` | `enum BinaryType { NodeBuffer, ArrayBuffer, Blob }`. (Blob exists only as an enum member; in .NET there's no Blob primitive — the browser-specific Blob tests are adapted, see §6.) |
| `ERROR_PACKET` | `static readonly Packet ErrorPacket = new(PacketType.Error, "parser error");` |
| `protocol = 4` | `public const int Protocol = 4;` |
| `PACKET_TYPES` / `PACKET_TYPES_REVERSE` | `static class PacketTypeMap { ... }` with two `Dictionary`/lookup helpers; produced from the `PacketType` enum so it can't drift. |

**Callback-as-continuation → callback delegates.** JS `encodePacket(packet,
supportsBinary, cb)` where `cb(encodedPacket => …)` becomes
`void EncodePacket(Packet packet, bool supportsBinary, Action<RawData> callback)`
— a 1:1 signature so the control flow of the ported tests is identical. (There
is no meaningful async work here: the only async-ish path in JS is the browser
`FileReader`/Blob path, which has no .NET analogue we port here.)

**Stream API → push/pull adapter objects.** JS uses the WHATWG `TransformStream`
with `writer.write()`/`reader.read()`. There is no built-in `TransformStream` on
.NET. Rather than bind to one specific async stream framework prematurely (we
haven't designed the transport layer yet, which will dictate the I/O model), we
port these as **minimal, self-contained adapter objects** that replicate the
observable behavior the tests assert:

- `IPacketEncoderStream` with `void Write(Packet packet)` and
  `IReadOnlyList<byte[]> ReadChunk()` (or `bool TryReadChunk(out byte[] chunk)`).
  Each `Write` produces exactly the header + payload chunks the JS version
  enqueues, in order, so a test reads header then payload.
- `IPacketDecoderStream(maxPayload, binaryType)` with `void Write(byte[] chunk)`
  and `bool TryRead(out Packet packet)`. It re-implements the JS state machine
  (`READ_HEADER` / `READ_EXTENDED_LENGTH_16` / `READ_EXTENDED_LENGTH_64` /
  `READ_PAYLOAD`) byte-for-byte, including the 2^53 guard, the
  `expectedLength === 0 → ERROR_PACKET` rule, and the `> maxPayload → ERROR_PACKET`
  rule, and accumulating across multiple `Write` calls (the "bytes by bytes" test
  case).

These are deliberately *not* `System.IO.Pipelines`/`System.Threading.Channels`
types. They are push-in / pull-out accumulators that preserve the exact semantics
of the JS `TransformStream` for the purposes of this codec and its tests. When we
design the transport layer in a later cycle we will revisit whether to add
async/streaming facades on top — that decision is out of scope here and
premature now.

---

## 4. Behavior to preserve verbatim (correctness contract)

These are the invariants that make the port wire-compatible. Each is covered by
an upstream test we carry forward.

**Packet type codes:** open=`0`, close=`1`, ping=`2`, pong=`3`, message=`4`,
upgrade=`5`, noop=`6`. Unknown leading char → `error / "parser error"`.

**`encodePacket`:**
- `data` is binary (ArrayBuffer / typed array / byte[]) and `supportsBinary`:
  returns `data` **unchanged** (echoes the original `RawData` value — mirrors JS
  `callback(data)`, so an ArrayBuffer stays an ArrayBuffer and an offset view
  stays an offset view; equality checks compare addressed bytes). The cast to a
  flat `byte[]` only happens in `EncodeToBinary` (the stream path).
- `data` is binary and `!supportsBinary`: returns `"b" + base64(data)`.
- otherwise: returns `PACKET_TYPES[type] + (data ?? "")`.

**`decodePacket(encoded, binaryType?)`:**
- non-string input → `{ type: "message", data: mapBinary(input, binaryType) }`.
- leading char `"b"` → `{ type: "message", data: mapBinary(base64decode(rest), binaryType) }`.
- leading char not in PACKET_TYPES_REVERSE → `ERROR_PACKET`.
- else `{ type: reverseMap[c0], data: rest }` (or no `data` if length==1).

**`mapBinary` (node variant):**
- `arraybuffer`: ArrayBuffer stays; byte[]→`buffer.slice(offset, offset+len)`
  equivalent; Uint8Array→its underlying buffer.
- `nodebuffer`/default: byte[] stays; Uint8Array→`Buffer.from(data)`.

**Payloads:** packets joined by `\x1e` (U+001E RECORD SEPARATOR).
`encodePayload` force-encodes every packet as base64-binary-unsafe (i.e.
`supportsBinary=false`) so binary packets become `b...` strings. `decodePayload`
splits on `\x1e`, decodes each, and **stops** the moment a decoded packet is an
error.

**Encoder stream framing (WebSocket-inspired):**
- `len < 126`: 1-byte header = `len`.
- `126 ≤ len < 65536`: 3-byte header = `126, big-endian uint16 len`.
- `len ≥ 65536`: 9-byte header = `127, big-endian uint64 len`.
- If payload is binary, set high bit of header[0] (`|= 0x80`).
- Then enqueue the payload bytes (UTF-8 of the encoded string for text packets).

**Decoder stream framing:** inverse of the above, plus:
- `len === 0 → ERROR_PACKET` and enqueue immediately.
- `len > maxPayload → ERROR_PACKET`.
- 64-bit length where the high 32 bits exceed `2^(53-32) - 1` → `ERROR_PACKET`
  (mirrors JS's `Number.MAX_SAFE_INTEGER` guard).

**UTF-8:** text packets are UTF-8 encoded/decoded (`Encoding.UTF8`). The test
case `"1€"` → `[52, 49, 226, 130, 172]` pins this.

---

## 5. Development approach (TDD)

Strict red → green, mirroring the upstream test structure:

1. Create solution + empty library project + test project (both TFMs wired up).
2. Port the **test suite first** (it won't compile until the API exists; that's
   the red state). Group tests 1:1 with the upstream files:
   `IndexTests.cs` ← `test/index.ts`, `NodeTests.cs` ← `test/node.ts`,
   `BrowserTests.cs` ← `test/browser.ts` (adapted).
3. Implement the minimum API surface (`Packet`, `PacketType`, `RawData`, etc.)
   so the tests **compile**; bodies throw `NotImplementedException` (still red).
4. Implement behavior file by file (`EncodePacket`, `DecodePacket`,
   `EngineIoParser` payloads, the two streams), flipping groups of tests green.
5. `dotnet test` all green; commit per logical step.

The `NotImplementedException` stubs are intentional scaffolding so we always
have a compiling red baseline, in line with the TDD discipline.

---

## 6. Adaptations & explicit deviations

These are the only places the port is not a mechanical 1:1; each is forced by a
platform difference and is documented so reviewers can challenge it.

1. **No `Blob`.** `Blob` is a browser-only primitive. The four Blob-specific
   upstream tests (encode/decode Blob, Blob as base64, encode/decode a Blob via
   the streams) have no direct .NET analogue. We keep the `BinaryType.Blob` enum
   member for wire-compatibility and future browser-facing consumers, but the
   Blob-only test cases are **omitted** (not silently skipped) with a comment
   referencing this section. Everything else in `browser.ts` (ArrayBuffer cases)
   **is** ported.

2. **`ArrayBuffer` modeling.** JS `ArrayBuffer` + typed-array-with-offset is
   represented by a small `ArrayBuffer` value type (`byte[] Buffer, int
   ByteOffset, int ByteLength`). This is what lets us faithfully reproduce the
   "encode a typed array with offset and length" test (`Int8Array(buffer, 1, 2)`
   → encodes bytes `[2,3]`) and the `data.buffer.slice(offset, offset+len)`
   path of `mapBinary`.

3. **`Buffer` → `byte[]`.** Node's `Buffer` is `byte[]` in .NET. The
   `Buffer.isBuffer` / `Buffer.from` branches collapse to direct `byte[]`
   handling.

4. **`TextEncoder`/`TextDecoder`.** Replaced by `Encoding.UTF8.GetBytes` /
   `GetString`. Same bytes (UTF-8).

5. **`TransformStream` → push/pull adapters.** As discussed in §3. Later cycle
   may add Channels/Pipelines facades; not now.

6. **Async/await in tests.** Upstream stream tests are `async`. Our adapters are
   synchronous push/pull, so the ported tests are synchronous (read header, read
   payload, assert). Behavior being asserted is unchanged.

---

## 7. Out of scope for this cycle

- `socket.io-parser`, `engine.io`, `engine.io-client`, `socket.io`,
  `socket.io-client`, adapters, msgpack — all later cycles.
- Any HTTP/WebSocket transport. The parser is pure logic.
- Async streaming facades (`System.IO.Pipelines`, `System.Threading.Channels`).
- NuGet packaging / versioning / CI. (We can add these once the foundation is
  green.)
- The `benchmarks/` directory from upstream.

## 8. Risks

- **Stream adapter divergence.** The biggest design latitude is in §3's
  adapters. Mitigation: the upstream stream tests are carried over verbatim in
  intent, so divergence shows up as test failures immediately.
- **`RawData` ergonomics.** A `struct`-wrapping-`object` can feel heavy.
  Mitigation: it's only at the codec boundary; internal calls are typed. We'll
  revisit if it proves awkward in the transport cycle.

---

## Next step

After this spec is approved, proceed via the writing-plans skill to a concrete
implementation plan, then TDD execution starting with the test port.
