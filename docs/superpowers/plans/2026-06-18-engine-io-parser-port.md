# engine.io-parser Port to .NET — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port `engine.io-parser` (socket.io v4 / upstream package v5.2.3) to a multi-target .NET C# library with the full upstream test suite ported to xUnit, test-first.

**Architecture:** Per-package monorepo mirroring upstream. The codec is pure logic: `Packet`/`PacketType`/`RawData`/`BinaryType` types, encode/decode/payload functions, and two push/pull stream adapters that replicate the WHATWG `TransformStream` framing semantics. JS `Buffer`→`byte[]`, `ArrayBuffer`→a small `ArrayBuffer` struct, callbacks→`Action<>` delegates, `TextEncoder/Decoder`→`Encoding.UTF8`.

**Tech Stack:** .NET 8/9/10 + netstandard2.1, C# (latest), xUnit, `Microsoft.NET.Test.Sdk`.

**Reference source (read-only, git-ignored):** `_upstream/packages/engine.io-parser/`.

---

## File Structure

```
SharpSocketIO.sln
Directory.Build.props
src/SharpSocketIO.EngineIo.Parser/
  SharpSocketIO.EngineIo.Parser.csproj
  Commons/PacketType.cs           // enum, numeric codes match wire
  Commons/PacketTypeMap.cs        // PACKET_TYPES + reverse lookup
  Commons/PacketOptions.cs        // compress/wsPreEncoded/wsPreEncodedFrame
  Commons/RawData.cs              // struct wrapping object? + Kind discriminator
  Commons/RawDataKind.cs          // enum
  Commons/ArrayBuffer.cs          // byte[] + offset + length value type
  Commons/BinaryType.cs           // enum NodeBuffer/ArrayBuffer/Blob
  Commons/Packet.cs               // sealed class, IEquatable<Packet>
  Commons/ErrorPacket.cs          // static readonly ErrorPacket
  Contrib/Base64ArrayBuffer.cs    // port of lib/contrib/base64-arraybuffer.ts
  EncodePacket.cs                 // static EncodePacket + EncodePacketToBinary
  DecodePacket.cs                 // static DecodePacket (+ mapBinary)
  PacketEncoderStream.cs          // push/pull adapter (encoder)
  PacketDecoderStream.cs          // push/pull adapter (decoder, state machine)
  EngineIoParser.cs               // EncodePayload/DecodePayload + re-exports + protocol const
tests/SharpSocketIO.EngineIo.Parser.Tests/
  SharpSocketIO.EngineIo.Parser.Tests.csproj
  TestUtil.cs                     // AreArraysEqual, CreateArrayBuffer, Eq helpers
  IndexTests.cs                   // ← test/index.ts
  NodeTests.cs                    // ← test/node.ts
  BrowserTests.cs                 // ← test/browser.ts (ArrayBuffer cases only)
```

---

## Task 1: Solution skeleton + projects — ✅ DONE

**Files:**
- Create: `Directory.Build.props`
- Create: `src/SharpSocketIO.EngineIo.Parser/SharpSocketIO.EngineIo.Parser.csproj`
- Create: `tests/SharpSocketIO.EngineIo.Parser.Tests/SharpSocketIO.EngineIo.Parser.Tests.csproj`
- Create: `SharpSocketIO.sln`

- [ ] **Step 1: Write `Directory.Build.props`**

```xml
<Project>
  <PropertyGroup>
    <LangVersion>latest</LangVersion>
    <Nullable>enable</Nullable>
    <TreatWarningsAsErrors>true</TreatWarningsAsErrors>
    <ImplicitUsings>enable</ImplicitUsings>
    <GenerateDocumentationFile>false</GenerateDocumentationFile>
  </PropertyGroup>
</Project>
```

- [ ] **Step 2: Write the library csproj**

`src/SharpSocketIO.EngineIo.Parser/SharpSocketIO.EngineIo.Parser.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFrameworks>net10.0;net9.0;net8.0;netstandard2.1</TargetFrameworks>
    <RootNamespace>SharpSocketIO.EngineIo.Parser</RootNamespace>
    <AssemblyName>SharpSocketIO.EngineIo.Parser</AssemblyName>
    <Version>5.2.3</Version>
  </PropertyGroup>
</Project>
```

- [ ] **Step 3: Write the test csproj**

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFrameworks>net10.0;net9.0;net8.0</TargetFrameworks>
    <IsPackable>false</IsPackable>
    <RootNamespace>SharpSocketIO.EngineIo.Parser.Tests</RootNamespace>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Microsoft.NET.Test.Sdk" Version="17.11.1" />
    <PackageReference Include="xunit" Version="2.9.2" />
    <PackageReference Include="xunit.runner.visualstudio" Version="2.8.2" />
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\..\src\SharpSocketIO.EngineIo.Parser\SharpSocketIO.EngineIo.Parser.csproj" />
  </ItemGroup>
</Project>
```

- [ ] **Step 4: Create solution + add projects**

Run:
```
dotnet new sln -n SharpSocketIO
dotnet sln add src/SharpSocketIO.EngineIo.Parser/SharpSocketIO.EngineIo.Parser.csproj
dotnet sln add tests/SharpSocketIO.EngineIo.Parser.Tests/SharpSocketIO.EngineIo.Parser.Tests.csproj
```

- [ ] **Step 5: Verify build**

Run: `dotnet build`
Expected: Build succeeded (no source yet; projects are empty but valid).

- [ ] **Step 6: Commit**

```
git add -A
git commit -m "build: solution skeleton for SharpSocketIO.EngineIo.Parser"
```

---

## Task 2: Core types — `PacketType`, `PacketTypeMap`, `BinaryType`, `ArrayBuffer`, `RawData` — ✅ DONE

**Files:**
- Create: `src/SharpSocketIO.EngineIo.Parser/Commons/PacketType.cs`
- Create: `src/SharpSocketIO.EngineIo.Parser/Commons/PacketTypeMap.cs`
- Create: `src/SharpSocketIO.EngineIo.Parser/Commons/BinaryType.cs`
- Create: `src/SharpSocketIO.EngineIo.Parser/Commons/ArrayBuffer.cs`
- Create: `src/SharpSocketIO.EngineIo.Parser/Commons/RawDataKind.cs`
- Create: `src/SharpSocketIO.EngineIo.Parser/Commons/RawData.cs`
- Create: `src/SharpSocketIO.EngineIo.Parser/Commons/PacketOptions.cs`
- Create: `src/SharpSocketIO.EngineIo.Parser/Commons/Packet.cs`
- Create: `src/SharpSocketIO.EngineIo.Parser/Commons/ErrorPacket.cs`
- Test: write a placeholder test that just constructs a `Packet` to lock the API shape (compiled red→green within this task).

- [ ] **Step 1: Write failing test for core type shape**

`tests/.../CoreTypesTests.cs`:

```csharp
using SharpSocketIO.EngineIo.Parser.Commons;
using Xunit;

namespace SharpSocketIO.EngineIo.Parser.Tests;

public class CoreTypesTests
{
    [Fact]
    public void Packet_equality_is_value_based()
    {
        var a = new Packet(PacketType.Message, "test");
        var b = new Packet(PacketType.Message, "test");
        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    [Fact]
    public void PacketTypeMap_round_trips_wire_codes()
    {
        Assert.Equal("4", PacketTypeMap.Code(PacketType.Message));
        Assert.Equal(PacketType.Message, PacketTypeMap.FromCode("4"));
    }
}
```

- [ ] **Step 2: Run, verify fail (compile error — types missing)**

Run: `dotnet test`
Expected: compile failure (`Packet`, `PacketType`, `PacketTypeMap` not found).

- [ ] **Step 3: Implement `PacketType`**

`Commons/PacketType.cs`:
```csharp
namespace SharpSocketIO.EngineIo.Parser.Commons;

/// <summary>Engine.io packet type. Numeric values are the on-wire codes.</summary>
public enum PacketType
{
    Open = 0,
    Close = 1,
    Ping = 2,
    Pong = 3,
    Message = 4,
    Upgrade = 5,
    Noop = 6,
    Error = 7,
}
```

- [ ] **Step 4: Implement `PacketTypeMap`**

`Commons/PacketTypeMap.cs`:
```csharp
using System.Collections.Generic;

namespace SharpSocketIO.EngineIo.Parser.Commons;

/// <summary>
/// Port of PACKET_TYPES / PACKET_TYPES_REVERSE from lib/commons.ts.
/// Single source of truth: derived from the PacketType enum so codes can't drift.
/// </summary>
public static class PacketTypeMap
{
    private static readonly IReadOnlyDictionary<PacketType, string> s_toCode = new Dictionary<PacketType, string>
    {
        [PacketType.Open] = "0",
        [PacketType.Close] = "1",
        [PacketType.Ping] = "2",
        [PacketType.Pong] = "3",
        [PacketType.Message] = "4",
        [PacketType.Upgrade] = "5",
        [PacketType.Noop] = "6",
    };

    private static readonly IReadOnlyDictionary<string, PacketType> s_fromCode = Invert(s_toCode);

    public static string Code(PacketType type) => s_toCode[type];

    public static bool TryFromCode(string code, out PacketType type) => s_fromCode.TryGetValue(code, out type);

    public static PacketType FromCode(string code) => s_fromCode[code];

    public static bool IsKnownCode(string code) => s_fromCode.ContainsKey(code);

    private static Dictionary<string, PacketType> Invert(IReadOnlyDictionary<PacketType, string> src)
    {
        var d = new Dictionary<string, PacketType>(src.Count);
        foreach (var kv in src) d[kv.Value] = kv.Key;
        return d;
    }
}
```

- [ ] **Step 5: Implement `BinaryType`**

`Commons/BinaryType.cs`:
```csharp
namespace SharpSocketIO.EngineIo.Parser.Commons;

/// <summary>Port of BinaryType = "nodebuffer" | "arraybuffer" | "blob".</summary>
public enum BinaryType
{
    NodeBuffer,
    ArrayBuffer,
    Blob,
}
```

- [ ] **Step 6: Implement `ArrayBuffer`**

`Commons/ArrayBuffer.cs`:
```csharp
namespace SharpSocketIO.EngineIo.Parser.Commons;

/// <summary>
/// .NET analogue of a JS ArrayBuffer / typed-array-with-offset. Wraps a byte
/// buffer plus a byte offset and length, mirroring the cases the upstream
/// tests exercise (e.g. Int8Array(buffer, 1, 2)).
/// </summary>
public readonly struct ArrayBuffer
{
    public ArrayBuffer(byte[] buffer) : this(buffer, 0, buffer.Length) { }

    public ArrayBuffer(byte[] buffer, int byteOffset, int byteLength)
    {
        Buffer = buffer;
        ByteOffset = byteOffset;
        ByteLength = byteLength;
    }

    public byte[] Buffer { get; }
    public int ByteOffset { get; }
    public int ByteLength { get; }

    /// <summary>Returns a copy of the addressed byte range as a standalone array.</summary>
    public byte[] ToArray()
    {
        var copy = new byte[ByteLength];
        System.Array.Copy(Buffer, ByteOffset, copy, 0, ByteLength);
        return copy;
    }

    /// <summary>JS ArrayBuffer.prototype.slice(start, end) equivalent.</summary>
    public ArrayBuffer Slice(int start, int end)
    {
        var len = end - start;
        var copy = new byte[len];
        System.Array.Copy(Buffer, ByteOffset + start, copy, 0, len);
        return new ArrayBuffer(copy);
    }

    public bool BytesEqual(ArrayBuffer other)
    {
        if (ByteLength != other.ByteLength) return false;
        for (int i = 0; i < ByteLength; i++)
            if (Buffer[ByteOffset + i] != other.Buffer[other.ByteOffset + i]) return false;
        return true;
    }
}
```

- [ ] **Step 7: Implement `RawDataKind` and `RawData`**

`Commons/RawDataKind.cs`:
```csharp
namespace SharpSocketIO.EngineIo.Parser.Commons;

public enum RawDataKind
{
    None,
    String,
    Int32,
    ByteArray,
    ArrayBuffer,
}
```

`Commons/RawData.cs`:
```csharp
namespace SharpSocketIO.EngineIo.Parser.Commons;

/// <summary>
/// Port of RawData = string | Buffer | ArrayBuffer | ArrayBufferView | Blob.
/// Discriminated wrapper so we can do the equivalent of
/// "data instanceof ArrayBuffer / ArrayBuffer.isView(data)" over a dynamic value.
/// Blob has no .NET primitive (see design spec §6); it is never produced here.
/// </summary>
public readonly struct RawData
{
    public RawData() { Value = null; Kind = RawDataKind.None; }

    public RawData(string value) { Value = value; Kind = RawDataKind.String; }
    public RawData(int value) { Value = value; Kind = RawDataKind.Int32; }
    public RawData(byte[] value) { Value = value; Kind = RawDataKind.ByteArray; }
    public RawData(ArrayBuffer value) { Value = value; Kind = RawDataKind.ArrayBuffer; }

    public object? Value { get; }
    public RawDataKind Kind { get; }

    public bool IsBinary => Kind == RawDataKind.ByteArray || Kind == RawDataKind.ArrayBuffer;

    public bool IsNull => Kind == RawDataKind.None;

    public string? AsString() => Value as string;
    public byte[]? AsByteArray() => Value as byte[];
    public ArrayBuffer? AsArrayBuffer() => Value as ArrayBuffer?;

    public static implicit operator RawData(string? s) => s is null ? default : new RawData(s);
    public static implicit operator RawData(byte[]? b) => b is null ? default : new RawData(b);
    public static implicit operator RawData(ArrayBuffer ab) => new RawData(ab);
    public static implicit operator RawData(int i) => new RawData(i);
}
```

- [ ] **Step 8: Implement `PacketOptions` and `Packet`**

`Commons/PacketOptions.cs`:
```csharp
namespace SharpSocketIO.EngineIo.Parser.Commons;

/// <summary>Port of Packet.options (compress, wsPreEncoded, wsPreEncodedFrame).</summary>
public sealed class PacketOptions
{
    public bool Compress { get; set; }
    public string? WsPreEncoded { get; set; }
    public byte[]? WsPreEncodedFrame { get; set; }
}
```

`Commons/Packet.cs`:
```csharp
using System;

namespace SharpSocketIO.EngineIo.Parser.Commons;

/// <summary>Port of interface Packet from lib/commons.ts.</summary>
public sealed class Packet : IEquatable<Packet>
{
    public Packet(PacketType type) { Type = type; }
    public Packet(PacketType type, RawData? data) { Type = type; Data = data; }
    public Packet(PacketType type, RawData? data, PacketOptions? options)
    { Type = type; Data = data; Options = options; }

    public PacketType Type { get; }
    public RawData? Data { get; }
    public PacketOptions? Options { get; }

    public bool Equals(Packet? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        if (Type != other.Type) return false;
        return DataEqual(Data, other.Data);
    }

    public override bool Equals(object? obj) => Equals(obj as Packet);

    public override int GetHashCode() => HashCode.Combine(Type, Data);

    public override string ToString()
        => Data is { Kind: not RawDataKind.None } d ? $"{Type}:{d.Value}" : Type.ToString();

    // Mirrors the structural equality the JS tests assert via expect(...).to.eql(packet):
    // two RawData are equal iff same Kind and same byte/string content.
    private static bool DataEqual(RawData? a, RawData? b)
    {
        if (a is null && b is null) return true;
        if (a is null || b is null) return false;
        if (a.Value.Kind != b.Value.Kind) return false;
        return a.Value.Kind switch
        {
            RawDataKind.None => true,
            RawDataKind.String => a.Value.AsString() == b.Value.AsString(),
            RawDataKind.Int32 => (int)a.Value.Value! == (int)b.Value.Value!,
            RawDataKind.ByteArray => BytesEqual(a.Value.AsByteArray()!, b.Value.AsByteArray()!),
            RawDataKind.ArrayBuffer => a.Value.AsArrayBuffer()!.Value.BytesEqual(b.Value.AsArrayBuffer()!.Value),
            _ => false,
        };
    }

    private static bool BytesEqual(byte[] x, byte[] y)
    {
        if (x.Length != y.Length) return false;
        for (int i = 0; i < x.Length; i++) if (x[i] != y[i]) return false;
        return true;
    }
}
```

- [ ] **Step 9: Implement `ErrorPacket`**

`Commons/ErrorPacket.cs`:
```csharp
namespace SharpSocketIO.EngineIo.Parser.Commons;

/// <summary>Port of ERROR_PACKET = { type: "error", data: "parser error" }.</summary>
public static class ErrorPacket
{
    public static readonly Packet Instance = new(PacketType.Error, new RawData("parser error"));
}
```

- [ ] **Step 10: Run test, verify pass**

Run: `dotnet test`
Expected: `CoreTypesTests` (2 tests) PASS.

- [ ] **Step 11: Commit**

```
git add -A
git commit -m "feat(parser): core types — PacketType, Packet, RawData, ArrayBuffer, BinaryType"
```

---

## Task 3: `Base64ArrayBuffer` contrib + `EncodePacket` / `EncodePacketToBinary` — ✅ DONE

**Files:**
- Create: `src/.../Contrib/Base64ArrayBuffer.cs`
- Create: `src/.../EncodePacket.cs`
- Test: `tests/.../EncodePacketTests.cs`

- [ ] **Step 1: Write failing tests for encodePacket (text + base64 binary + binary noop + typed-array-with-offset)**

`tests/.../EncodePacketTests.cs`:
```csharp
using SharpSocketIO.EngineIo.Parser.Commons;
using Xunit;

namespace SharpSocketIO.EngineIo.Parser.Tests;

public class EncodePacketTests
{
    private static RawData Encode(Packet p, bool supportsBinary)
    {
        RawData result = default;
        EncodePacket.Encode(p, supportsBinary, r => result = r);
        return result;
    }

    [Fact]
    public void Encodes_a_string()
    {
        var packet = new Packet(PacketType.Message, new RawData("test"));
        var encoded = Encode(packet, true);
        Assert.Equal(RawDataKind.String, encoded.Kind);
        Assert.Equal("4test", encoded.AsString());
    }

    [Fact]
    public void Encodes_a_buffer_as_noop_when_supports_binary()
    {
        var packet = new Packet(PacketType.Message, new RawData(new byte[] { 1, 2, 3, 4 }));
        var encoded = Encode(packet, true);
        Assert.Equal(RawDataKind.ByteArray, encoded.Kind);
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, encoded.AsByteArray());
    }

    [Fact]
    public void Encodes_a_buffer_as_base64_when_no_binary_support()
    {
        var packet = new Packet(PacketType.Message, new RawData(new byte[] { 1, 2, 3, 4 }));
        var encoded = Encode(packet, false);
        Assert.Equal("bAQIDBA==", encoded.AsString());
    }

    [Fact]
    public void Encodes_a_typed_array_with_offset_and_length()
    {
        var buffer = new byte[] { 1, 2, 3, 4 };
        var view = new ArrayBuffer(buffer, 1, 2); // bytes [2,3]
        var packet = new Packet(PacketType.Message, new RawData(view));
        var encoded = Encode(packet, true);
        Assert.Equal(RawDataKind.ByteArray, encoded.Kind);
        Assert.Equal(new byte[] { 2, 3 }, encoded.AsByteArray());
    }

    [Fact]
    public void Encodes_a_buffer_without_data()
    {
        var packet = new Packet(PacketType.Open);
        var encoded = Encode(packet, true);
        Assert.Equal("0", encoded.AsString());
    }
}
```

- [ ] **Step 2: Run, verify fail (compile error — EncodePacket missing)**

Run: `dotnet test`
Expected: compile failure.

- [ ] **Step 3: Implement `Base64ArrayBuffer`**

`Contrib/Base64ArrayBuffer.cs` (faithful port of `lib/contrib/base64-arraybuffer.ts`; .NET's `Convert.ToBase64String`/`FromBase64String` produce identical output, but we keep this as a self-contained port for parity/fidelity):
```csharp
using System.Text;
using SharpSocketIO.EngineIo.Parser.Commons;

namespace SharpSocketIO.EngineIo.Parser.Contrib;

/// <summary>
/// Direct port of lib/contrib/base64-arraybuffer.ts (encode/decode). Standard
/// base64 (RFC 4648) — identical to System.Convert's base64 output for the
/// inputs the parser deals with. Kept as a self-contained port for fidelity.
/// </summary>
internal static class Base64ArrayBuffer
{
    private const string Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    public static string Encode(byte[] bytes, int offset, int length)
    {
        var sb = new StringBuilder(length * 4 / 3 + 4);
        int i;
        for (i = 0; i + 2 < length; i += 3)
        {
            byte b0 = bytes[offset + i], b1 = bytes[offset + i + 1], b2 = bytes[offset + i + 2];
            sb.Append(Chars[b0 >> 2]);
            sb.Append(Chars[((b0 & 3) << 4) | (b1 >> 4)]);
            sb.Append(Chars[((b1 & 15) << 2) | (b2 >> 6)]);
            sb.Append(Chars[b2 & 63]);
        }
        int rem = length - i;
        if (rem == 2)
        {
            byte b0 = bytes[offset + i], b1 = bytes[offset + i + 1];
            sb.Append(Chars[b0 >> 2]);
            sb.Append(Chars[((b0 & 3) << 4) | (b1 >> 4)]);
            sb.Append(Chars[(b1 & 15) << 2]);
            sb.Append('=');
        }
        else if (rem == 1)
        {
            byte b0 = bytes[offset + i];
            sb.Append(Chars[b0 >> 2]);
            sb.Append(Chars[(b0 & 3) << 4]);
            sb.Append('=').Append('=');
        }
        return sb.ToString();
    }

    public static byte[] Decode(string base64)
    {
        int bufferLength = base64.Length * 3 / 4;
        int len = base64.Length;
        if (len > 0 && base64[len - 1] == '=')
        {
            bufferLength--;
            if (len > 1 && base64[len - 2] == '=') bufferLength--;
        }
        var bytes = new byte[bufferLength];
        int p = 0;
        for (int i = 0; i < len; i += 4)
        {
            int e1 = Lookup(base64[i]);
            int e2 = Lookup(base64[i + 1]);
            int e3 = Lookup(base64[i + 2]);
            int e4 = Lookup(base64[i + 3]);
            bytes[p++] = (byte)((e1 << 2) | (e2 >> 4));
            if (p < bufferLength) bytes[p++] = (byte)(((e2 & 15) << 4) | (e3 >> 2));
            if (p < bufferLength) bytes[p++] = (byte)(((e3 & 3) << 6) | (e4 & 63));
        }
        return bytes;
    }

    private static int Lookup(char c) => c switch
    {
        >= 'A' and <= 'Z' => c - 'A',
        >= 'a' and <= 'z' => c - 'a' + 26,
        >= '0' and <= '9' => c - '0' + 52,
        '+' => 62,
        '/' => 63,
        _ => 0,
    };
}
```

- [ ] **Step 4: Implement `EncodePacket` + `EncodePacketToBinary`**

`EncodePacket.cs`:
```csharp
using System;
using System.Text;
using SharpSocketIO.EngineIo.Parser.Commons;

namespace SharpSocketIO.EngineIo.Parser;

/// <summary>Port of lib/encodePacket.ts (node variant).</summary>
public static class EncodePacket
{
    /// <summary>
    /// Encodes a packet. Callback receives either:
    ///  - the raw binary data (when supportsBinary and data is binary), or
    ///  - "b" + base64(data) (when !supportsBinary and data is binary), or
    ///  - PACKET_TYPES[type] + (data ?? "").
    /// </summary>
    public static void Encode(Packet packet, bool supportsBinary, Action<RawData> callback)
    {
        if (packet.Data is { Kind: RawDataKind.ByteArray or RawDataKind.ArrayBuffer })
        {
            if (supportsBinary)
            {
                callback(ToRawByteArray(packet.Data.Value));
            }
            else
            {
                var (buf, off, len) = ToBytes(packet.Data.Value);
                callback(new RawData("b" + Contrib.Base64ArrayBuffer.Encode(buf, off, len)));
            }
            return;
        }
        // plain string
        callback(new RawData(PacketTypeMap.Code(packet.Type) + StringOrEmpty(packet.Data)));
    }

    /// <summary>Port of encodePacketToBinary: returns raw bytes (binary payload) or UTF-8 of the text packet.</summary>
    public static void EncodeToBinary(Packet packet, Action<RawData> callback)
    {
        if (packet.Data is { Kind: RawDataKind.ByteArray or RawDataKind.ArrayBuffer })
        {
            callback(ToRawByteArray(packet.Data.Value));
            return;
        }
        Encode(packet, true, encoded =>
        {
            callback(new RawData(Encoding.UTF8.GetBytes(encoded.AsString()!)));
        });
    }

    private static string StringOrEmpty(RawData? data)
    {
        if (data is null || data.Value.Kind == RawDataKind.None) return string.Empty;
        if (data.Value.Kind == RawDataKind.String) return data.Value.AsString()!;
        if (data.Value.Kind == RawDataKind.Int32) return ((int)data.Value.Value!).ToString();
        return string.Empty;
    }

    private static RawData ToRawByteArray(RawData data)
    {
        if (data.Kind == RawDataKind.ByteArray)
            return new RawData(data.AsByteArray()!);
        var ab = data.AsArrayBuffer()!.Value;
        if (ab.ByteOffset == 0 && ab.ByteLength == ab.Buffer.Length)
            return new RawData(ab.Buffer);
        // JS Buffer.from(data.buffer, data.byteOffset, data.byteLength) — copies the addressed range
        return new RawData(ab.ToArray());
    }

    private static (byte[] buf, int off, int len) ToBytes(RawData data)
    {
        if (data.Kind == RawDataKind.ByteArray)
        {
            var b = data.AsByteArray()!;
            return (b, 0, b.Length);
        }
        var ab = data.AsArrayBuffer()!.Value;
        return (ab.Buffer, ab.ByteOffset, ab.ByteLength);
    }
}
```

- [ ] **Step 5: Run tests, verify pass**

Run: `dotnet test`
Expected: `EncodePacketTests` (5) + `CoreTypesTests` (2) all PASS.

- [ ] **Step 6: Commit**

```
git add -A
git commit -m "feat(parser): EncodePacket + EncodeToBinary + Base64ArrayBuffer contrib"
```

---

## Task 4: `DecodePacket` — ✅ DONE

**Files:**
- Create: `src/.../DecodePacket.cs`
- Test: `tests/.../DecodePacketTests.cs`

- [ ] **Step 1: Write failing tests (mirrors node.ts + index.ts single-packet cases)**

`tests/.../DecodePacketTests.cs`:
```csharp
using SharpSocketIO.EngineIo.Parser.Commons;
using Xunit;

namespace SharpSocketIO.EngineIo.Parser.Tests;

public class DecodePacketTests
{
    [Fact]
    public void Decodes_a_string()
    {
        var decoded = DecodePacket.Decode(new RawData("4test"));
        Assert.Equal(new Packet(PacketType.Message, new RawData("test")), decoded);
    }

    [Fact]
    public void Fails_to_decode_empty()
    {
        Assert.Equal(ErrorPacket.Instance, DecodePacket.Decode(new RawData("")));
    }

    [Fact]
    public void Fails_to_decode_malformed()
    {
        Assert.Equal(ErrorPacket.Instance, DecodePacket.Decode(new RawData("a123")));
    }

    [Fact]
    public void Decodes_a_buffer_as_base64_nodebuffer()
    {
        var decoded = DecodePacket.Decode(new RawData("bAQIDBA=="), BinaryType.NodeBuffer);
        Assert.Equal(PacketType.Message, decoded.Type);
        Assert.Equal(RawDataKind.ByteArray, decoded.Data!.Value.Kind);
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, decoded.Data!.Value.AsByteArray());
    }

    [Fact]
    public void Decodes_a_buffer_as_base64_arraybuffer()
    {
        var decoded = DecodePacket.Decode(new RawData("bAQIDBA=="), BinaryType.ArrayBuffer);
        Assert.Equal(PacketType.Message, decoded.Type);
        Assert.Equal(RawDataKind.ArrayBuffer, decoded.Data!.Value.Kind);
        Assert.True(decoded.Data!.Value.AsArrayBuffer()!.Value
            .BytesEqual(new ArrayBuffer(new byte[] { 1, 2, 3, 4 })));
    }

    [Fact]
    public void Decodes_a_binary_byte_array_input_as_message()
    {
        var decoded = DecodePacket.Decode(new RawData(new byte[] { 1, 2, 3, 4 }), BinaryType.NodeBuffer);
        Assert.Equal(PacketType.Message, decoded.Type);
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, decoded.Data!.Value.AsByteArray());
    }

    [Fact]
    public void Decodes_packet_with_no_data()
    {
        var decoded = DecodePacket.Decode(new RawData("2"));
        Assert.Equal(new Packet(PacketType.Ping), decoded);
    }
}
```

- [ ] **Step 2: Run, verify fail**

Run: `dotnet test` → compile error.

- [ ] **Step 3: Implement `DecodePacket`**

`DecodePacket.cs`:
```csharp
using SharpSocketIO.EngineIo.Parser.Commons;
using SharpSocketIO.EngineIo.Parser.Contrib;

namespace SharpSocketIO.EngineIo.Parser;

/// <summary>Port of lib/decodePacket.ts (node variant).</summary>
public static class DecodePacket
{
    public static Packet Decode(RawData encodedPacket, BinaryType binaryType = BinaryType.NodeBuffer)
    {
        if (encodedPacket.Kind != RawDataKind.String)
        {
            return new Packet(PacketType.Message, MapBinary(encodedPacket, binaryType));
        }
        var s = encodedPacket.AsString()!;
        if (s.Length == 0)
        {
            return ErrorPacket.Instance;
        }
        var typeChar = s[0];
        if (typeChar == 'b')
        {
            var bytes = Base64ArrayBuffer.Decode(s.Substring(1));
            return new Packet(PacketType.Message, MapBinary(new RawData(bytes), binaryType));
        }
        if (!PacketTypeMap.IsKnownCode(s.Substring(0, 1)))
        {
            return ErrorPacket.Instance;
        }
        var type = PacketTypeMap.FromCode(s.Substring(0, 1));
        return s.Length > 1
            ? new Packet(type, new RawData(s.Substring(1)))
            : new Packet(type);
    }

    // Port of mapBinary (node variant) from lib/decodePacket.ts.
    private static RawData MapBinary(RawData data, BinaryType binaryType)
    {
        switch (binaryType)
        {
            case BinaryType.ArrayBuffer:
                if (data.Kind == RawDataKind.ArrayBuffer)
                {
                    return data; // already an ArrayBuffer
                }
                if (data.Kind == RawDataKind.ByteArray)
                {
                    var b = data.AsByteArray()!;
                    return new RawData(new ArrayBuffer((byte[])b.Clone()));
                }
                // Uint8Array → data.slice().buffer (no Uint8Array in our model; treat byte[] as byte[])
                return new RawData(new ArrayBuffer(data.AsByteArray()!));
            case BinaryType.NodeBuffer:
            default:
                if (data.Kind == RawDataKind.ByteArray) return data;
                if (data.Kind == RawDataKind.ArrayBuffer)
                    return new RawData(data.AsArrayBuffer()!.Value.ToArray());
                return data;
        }
    }
}
```

- [ ] **Step 4: Run, verify pass**

Run: `dotnet test` → all tests PASS.

- [ ] **Step 5: Commit**

```
git add -A
git commit -m "feat(parser): DecodePacket with base64/binary mapping"
```

---

## Task 5: Payloads (`EncodePayload` / `DecodePayload`) + public façade — ✅ DONE

**Files:**
- Create: `src/.../EngineIoParser.cs`
- Test: `tests/.../PayloadTests.cs`

- [ ] **Step 1: Write failing tests (mirrors index.ts + node.ts payload cases)**

`tests/.../PayloadTests.cs`:
```csharp
using SharpSocketIO.EngineIo.Parser.Commons;
using Xunit;

namespace SharpSocketIO.EngineIo.Parser.Tests;

public class PayloadTests
{
    private static readonly string Sep = ((char)30).ToString();

    [Fact]
    public void Encodes_and_decodes_all_packet_types()
    {
        var packets = new[]
        {
            new Packet(PacketType.Open),
            new Packet(PacketType.Close),
            new Packet(PacketType.Ping, new RawData("probe")),
            new Packet(PacketType.Pong, new RawData("probe")),
            new Packet(PacketType.Message, new RawData("test")),
        };
        var payload = EngineIoParser.EncodePayload(packets);
        Assert.Equal("0" + Sep + "1" + Sep + "2probe" + Sep + "3probe" + Sep + "4test", payload);
        Assert.Equal(packets, EngineIoParser.DecodePayload(payload));
    }

    [Fact]
    public void Encodes_string_plus_buffer_payload_as_base64()
    {
        var packets = new[]
        {
            new Packet(PacketType.Message, new RawData("test")),
            new Packet(PacketType.Message, new RawData(new byte[] { 1, 2, 3, 4 })),
        };
        var payload = EngineIoParser.EncodePayload(packets);
        Assert.Equal("4test" + Sep + "bAQIDBA==", payload);
        Assert.Equal(packets, EngineIoParser.DecodePayload(payload, BinaryType.NodeBuffer));
    }

    [Fact]
    public void Fails_to_decode_malformed_payload_brace()
    {
        var result = EngineIoParser.DecodePayload("{");
        Assert.Equal(new[] { ErrorPacket.Instance }, result);
    }

    [Fact]
    public void Fails_to_decode_malformed_payload_braces()
    {
        var result = EngineIoParser.DecodePayload("{}");
        Assert.Equal(new[] { ErrorPacket.Instance }, result);
    }

    [Fact]
    public void Fails_to_decode_malformed_payload_array_string()
    {
        var result = EngineIoParser.DecodePayload("[\"a123\", \"a456\"]");
        Assert.Equal(new[] { ErrorPacket.Instance }, result);
    }

    [Fact]
    public void Protocol_is_4()
    {
        Assert.Equal(4, EngineIoParser.Protocol);
    }
}
```

- [ ] **Step 2: Run, verify fail**

- [ ] **Step 3: Implement `EngineIoParser`**

`EngineIoParser.cs`:
```csharp
using System.Collections.Generic;
using System.Text;
using SharpSocketIO.EngineIo.Parser.Commons;

namespace SharpSocketIO.EngineIo.Parser;

/// <summary>
/// Port of lib/index.ts. Exposes encode/decode packet + payload helpers and the
/// protocol version. (Stream adapters live in their own types — see spec §3.)
/// </summary>
public static class EngineIoParser
{
    // see https://en.wikipedia.org/wiki/Delimiter#ASCII_delimited_text
    internal static readonly string Separator = ((char)30).ToString();

    /// <summary>Engine.io protocol version (mirrors `export const protocol = 4`).</summary>
    public const int Protocol = 4;

    /// <summary>Encodes multiple packets into a single separator-delimited payload string.</summary>
    public static string EncodePayload(IReadOnlyList<Packet> packets)
    {
        var encoded = new string[packets.Count];
        for (int i = 0; i < packets.Count; i++)
        {
            // force base64 encoding for binary packets: supportsBinary=false
            string captured = string.Empty;
            EncodePacket.Encode(packets[i], false, r => captured = r.AsString()!);
            encoded[i] = captured;
        }
        return string.Join(Separator, encoded);
    }

    /// <summary>Decodes a payload, stopping at the first error packet (mirrors JS break).</summary>
    public static IReadOnlyList<Packet> DecodePayload(string encodedPayload, BinaryType binaryType = BinaryType.NodeBuffer)
    {
        var parts = encodedPayload.Split(Separator);
        var packets = new List<Packet>(parts.Length);
        foreach (var part in parts)
        {
            var decoded = DecodePacket.Decode(new RawData(part), binaryType);
            packets.Add(decoded);
            if (decoded.Type == PacketType.Error) break;
        }
        return packets;
    }
}
```

- [ ] **Step 4: Run, verify pass**

Run: `dotnet test` → all PASS.

- [ ] **Step 5: Commit**

```
git add -A
git commit -m "feat(parser): EncodePayload/DecodePayload + protocol constant"
```

---

## Task 6: `PacketEncoderStream` (push/pull adapter) — ✅ DONE

**Files:**
- Create: `src/.../PacketEncoderStream.cs`
- Test: `tests/.../PacketEncoderStreamTests.cs`

- [ ] **Step 1: Write failing tests (mirrors index.ts + node.ts encoder cases)**

`tests/.../PacketEncoderStreamTests.cs`:
```csharp
using SharpSocketIO.EngineIo.Parser.Commons;
using Xunit;

namespace SharpSocketIO.EngineIo.Parser.Tests;

public class PacketEncoderStreamTests
{
    [Fact]
    public void Encodes_a_plaintext_packet()
    {
        var stream = new PacketEncoderStream();
        stream.Write(new Packet(PacketType.Message, new RawData("1€")));
        Assert.Equal(new byte[] { 5 }, stream.ReadChunk());
        Assert.Equal(new byte[] { 52, 49, 226, 130, 172 }, stream.ReadChunk());
    }

    [Fact]
    public void Encodes_a_binary_packet_byte_array()
    {
        var stream = new PacketEncoderStream();
        var data = new byte[] { 1, 2, 3 };
        stream.Write(new Packet(PacketType.Message, new RawData(data)));
        Assert.Equal(new byte[] { 131 }, stream.ReadChunk()); // 0x03 | 0x80
        Assert.Equal(data, stream.ReadChunk());
    }

    [Fact]
    public void Encodes_a_binary_packet_arraybuffer()
    {
        var stream = new PacketEncoderStream();
        stream.Write(new Packet(PacketType.Message, new RawData(new ArrayBuffer(new byte[] { 1, 2, 3 }))));
        Assert.Equal(new byte[] { 131 }, stream.ReadChunk());
        Assert.Equal(new byte[] { 1, 2, 3 }, stream.ReadChunk());
    }

    [Fact]
    public void Encodes_a_uint16array_view_as_bytes()
    {
        var stream = new PacketEncoderStream();
        // JS Uint16Array.from([1, 2, 257]) → little-endian bytes 01 00 02 00 01 01
        var bytes = new byte[] { 1, 0, 2, 0, 1, 1 };
        stream.Write(new Packet(PacketType.Message, new RawData(bytes)));
        Assert.Equal(new byte[] { 134 }, stream.ReadChunk()); // 6 | 0x80
        Assert.Equal(bytes, stream.ReadChunk());
    }

    [Fact]
    public void Encodes_medium_packet_with_16bit_length()
    {
        var stream = new PacketEncoderStream();
        var data = new byte[12345];
        stream.Write(new Packet(PacketType.Message, new RawData(data)));
        Assert.Equal(new byte[] { 254, 48, 57 }, stream.ReadChunk()); // 126 | 0x80, big-endian 12345
        Assert.Equal(data, stream.ReadChunk());
    }

    [Fact]
    public void Encodes_big_packet_with_64bit_length()
    {
        var stream = new PacketEncoderStream();
        var data = new byte[123456789];
        stream.Write(new Packet(PacketType.Message, new RawData(data)));
        Assert.Equal(new byte[] { 255, 0, 0, 0, 0, 7, 91, 205, 21 }, stream.ReadChunk());
        Assert.Equal(data, stream.ReadChunk());
    }
}
```

- [ ] **Step 2: Run, verify fail**

- [ ] **Step 3: Implement `PacketEncoderStream`**

`PacketEncoderStream.cs`:
```csharp
using System;
using System.Collections.Generic;
using System.Text;
using SharpSocketIO.EngineIo.Parser.Commons;

namespace SharpSocketIO.EngineIo.Parser;

/// <summary>
/// Port of createPacketEncoderStream (lib/index.ts). Push/pull adapter:
/// each Write(packet) enqueues a header chunk then a payload chunk, in order,
/// exactly as the JS TransformStream would. ReadChunk pulls them one at a time.
/// </summary>
public sealed class PacketEncoderStream
{
    private readonly Queue<byte[]> _chunks = new();

    public void Write(Packet packet)
    {
        EncodePacket.EncodeToBinary(packet, encoded =>
        {
            var bytes = encoded.Kind == RawDataKind.ByteArray
                ? encoded.AsByteArray()!
                : encoded.Kind == RawDataKind.ArrayBuffer
                    ? encoded.AsArrayBuffer()!.Value.ToArray()
                    : throw new InvalidOperationException("EncodeToBinary must yield bytes");

            long payloadLength = bytes.Length;
            byte[] header;
            if (payloadLength < 126)
            {
                header = new byte[1];
                header[0] = (byte)payloadLength;
            }
            else if (payloadLength < 65536)
            {
                header = new byte[3];
                header[0] = 126;
                header[1] = (byte)((payloadLength >> 8) & 0xff);
                header[2] = (byte)(payloadLength & 0xff);
            }
            else
            {
                header = new byte[9];
                header[0] = 127;
                ulong v = (ulong)payloadLength;
                for (int i = 0; i < 8; i++)
                    header[8 - i] = (byte)((v >> (8 * i)) & 0xff);
            }
            // first bit indicates binary (1) vs plain text (0)
            bool isBinary = packet.Data is { Kind: RawDataKind.ByteArray or RawDataKind.ArrayBuffer };
            if (isBinary) header[0] |= 0x80;

            _chunks.Enqueue(header);
            _chunks.Enqueue(bytes);
        });
    }

    public byte[] ReadChunk() => _chunks.Dequeue();

    public bool HasChunks => _chunks.Count > 0;
}
```

- [ ] **Step 4: Run, verify pass**

Run: `dotnet test` → all PASS.

- [ ] **Step 5: Commit**

```
git add -A
git commit -m "feat(parser): PacketEncoderStream push/pull adapter"
```

---

## Task 7: `PacketDecoderStream` (state machine) — ✅ DONE

**Files:**
- Create: `src/.../PacketDecoderStream.cs`
- Test: `tests/.../PacketDecoderStreamTests.cs`

- [ ] **Step 1: Write failing tests (mirrors index.ts + node.ts decoder cases)**

`tests/.../PacketDecoderStreamTests.cs`:
```csharp
using System.Linq;
using SharpSocketIO.EngineIo.Parser.Commons;
using Xunit;

namespace SharpSocketIO.EngineIo.Parser.Tests;

public class PacketDecoderStreamTests
{
    [Fact]
    public void Decodes_a_plaintext_packet()
    {
        var stream = new PacketDecoderStream(1_000_000, BinaryType.ArrayBuffer);
        stream.Write(new byte[] { 5 });
        stream.Write(new byte[] { 52, 49, 226, 130, 172 });
        Assert.True(stream.TryRead(out var packet));
        Assert.Equal(new Packet(PacketType.Message, new RawData("1€")), packet);
    }

    [Fact]
    public void Decodes_plaintext_byte_by_byte()
    {
        var stream = new PacketDecoderStream(1_000_000, BinaryType.ArrayBuffer);
        stream.Write(new byte[] { 5 });
        stream.Write(new byte[] { 52 });
        stream.Write(new byte[] { 49 });
        stream.Write(new byte[] { 226 });
        stream.Write(new byte[] { 130 });
        stream.Write(new byte[] { 172 });
        stream.Write(new byte[] { 1 });
        stream.Write(new byte[] { 50 }); // ping
        stream.Write(new byte[] { 1 });
        stream.Write(new byte[] { 51 }); // pong

        Assert.True(stream.TryRead(out var first));
        Assert.Equal(new Packet(PacketType.Message, new RawData("1€")), first);
        Assert.True(stream.TryRead(out var ping));
        Assert.Equal(new Packet(PacketType.Ping), ping);
        Assert.True(stream.TryRead(out var pong));
        Assert.Equal(new Packet(PacketType.Pong), pong);
    }

    [Fact]
    public void Decodes_plaintext_all_bytes_at_once()
    {
        var stream = new PacketDecoderStream(1_000_000, BinaryType.ArrayBuffer);
        stream.Write(new byte[] { 5, 52, 49, 226, 130, 172, 1, 50, 1, 51 });
        Assert.True(stream.TryRead(out var first));
        Assert.Equal(new Packet(PacketType.Message, new RawData("1€")), first);
        Assert.True(stream.TryRead(out var ping));
        Assert.Equal(new Packet(PacketType.Ping), ping);
        Assert.True(stream.TryRead(out var pong));
        Assert.Equal(new Packet(PacketType.Pong), pong);
    }

    [Fact]
    public void Decodes_a_binary_arraybuffer()
    {
        var stream = new PacketDecoderStream(1_000_000, BinaryType.ArrayBuffer);
        stream.Write(new byte[] { 131, 1, 2, 3 });
        Assert.True(stream.TryRead(out var packet));
        Assert.Equal(PacketType.Message, packet.Type);
        Assert.Equal(RawDataKind.ArrayBuffer, packet.Data!.Value.Kind);
        Assert.True(packet.Data!.Value.AsArrayBuffer()!.Value.BytesEqual(new ArrayBuffer(new byte[] { 1, 2, 3 })));
    }

    [Fact]
    public void Decodes_a_binary_nodebuffer()
    {
        var stream = new PacketDecoderStream(1_000_000, BinaryType.NodeBuffer);
        stream.Write(new byte[] { 131, 1, 2, 3 });
        Assert.True(stream.TryRead(out var packet));
        Assert.Equal(PacketType.Message, packet.Type);
        Assert.Equal(new byte[] { 1, 2, 3 }, packet.Data!.Value.AsByteArray());
    }

    [Fact]
    public void Decodes_a_binary_medium_packet()
    {
        var stream = new PacketDecoderStream(1_000_000, BinaryType.ArrayBuffer);
        var payload = new byte[12345];
        stream.Write(new byte[] { 254 });
        stream.Write(new byte[] { 48, 57 });
        stream.Write(payload);
        Assert.True(stream.TryRead(out var packet));
        Assert.Equal(PacketType.Message, packet.Type);
        Assert.True(packet.Data!.Value.AsArrayBuffer()!.Value.BytesEqual(new ArrayBuffer(payload)));
    }

    [Fact]
    public void Returns_error_when_payload_exceeds_max()
    {
        var stream = new PacketDecoderStream(10, BinaryType.ArrayBuffer);
        stream.Write(new byte[] { 11 });
        Assert.True(stream.TryRead(out var packet));
        Assert.Equal(ErrorPacket.Instance, packet);
    }

    [Fact]
    public void Returns_error_when_payload_length_is_zero()
    {
        var stream = new PacketDecoderStream(1_000_000, BinaryType.ArrayBuffer);
        stream.Write(new byte[] { 0 });
        Assert.True(stream.TryRead(out var packet));
        Assert.Equal(ErrorPacket.Instance, packet);
    }

    [Fact]
    public void Returns_error_when_length_exceeds_max_safe_integer()
    {
        var stream = new PacketDecoderStream(1_000_000, BinaryType.ArrayBuffer);
        stream.Write(new byte[] { 255, 1, 0, 0, 0, 0, 0, 0, 0, 0 });
        Assert.True(stream.TryRead(out var packet));
        Assert.Equal(ErrorPacket.Instance, packet);
    }
}
```

- [ ] **Step 2: Run, verify fail**

- [ ] **Step 3: Implement `PacketDecoderStream`**

`PacketDecoderStream.cs`:
```csharp
using System;
using System.Collections.Generic;
using System.Text;
using SharpSocketIO.EngineIo.Parser.Commons;

namespace SharpSocketIO.EngineIo.Parser;

/// <summary>
/// Port of createPacketDecoderStream (lib/index.ts). Push/pull adapter with the
/// same READ_HEADER / READ_EXTENDED_LENGTH_16 / READ_EXTENDED_LENGTH_64 /
/// READ_PAYLOAD state machine and the same guards (len==0 → error,
/// len &gt; maxPayload → error, 64-bit high-word &gt; 2^(53-32)-1 → error).
/// </summary>
public sealed class PacketDecoderStream
{
    private enum State { ReadHeader, ReadExtendedLength16, ReadExtendedLength64, ReadPayload }

    private readonly long _maxPayload;
    private readonly BinaryType _binaryType;
    private readonly List<byte[]> _chunks = new();
    private readonly Queue<Packet> _output = new();
    private State _state = State.ReadHeader;
    private long _expectedLength = -1;
    private bool _isBinary;

    public PacketDecoderStream(long maxPayload, BinaryType binaryType)
    {
        _maxPayload = maxPayload;
        _binaryType = binaryType;
    }

    public void Write(byte[] chunk)
    {
        _chunks.Add(chunk);
        while (true)
        {
            if (_state == State.ReadHeader)
            {
                if (TotalLength() < 1) break;
                var header = ConcatChunks(1);
                _isBinary = (header[0] & 0x80) == 0x80;
                _expectedLength = header[0] & 0x7f;
                if (_expectedLength < 126) _state = State.ReadPayload;
                else if (_expectedLength == 126) _state = State.ReadExtendedLength16;
                else _state = State.ReadExtendedLength64;
            }
            else if (_state == State.ReadExtendedLength16)
            {
                if (TotalLength() < 2) break;
                var h = ConcatChunks(2);
                _expectedLength = ((long)h[0] << 8) | h[1];
                _state = State.ReadPayload;
            }
            else if (_state == State.ReadExtendedLength64)
            {
                if (TotalLength() < 8) break;
                var h = ConcatChunks(8);
                // high 32 bits
                long n = ((long)h[0] << 24) | ((long)h[1] << 16) | ((long)h[2] << 8) | h[3];
                if (n > (long)Math.Pow(2, 53 - 32) - 1)
                {
                    _output.Enqueue(ErrorPacket.Instance);
                    break;
                }
                long low = ((long)h[4] << 24) | ((long)h[5] << 16) | ((long)h[6] << 8) | h[7];
                _expectedLength = n * (long)Math.Pow(2, 32) + low;
                _state = State.ReadPayload;
            }
            else // ReadPayload
            {
                if (TotalLength() < _expectedLength) break;
                var data = ConcatChunks((int)_expectedLength);
                RawData encoded;
                if (_isBinary)
                {
                    encoded = new RawData(data);
                }
                else
                {
                    encoded = new RawData(Encoding.UTF8.GetString(data));
                }
                _output.Enqueue(DecodePacket.Decode(encoded, _binaryType));
                _state = State.ReadHeader;
            }

            if (_expectedLength == 0 || _expectedLength > _maxPayload)
            {
                _output.Enqueue(ErrorPacket.Instance);
                break;
            }
        }
    }

    public bool TryRead(out Packet packet) => _output.TryDequeue(out packet!);

    private int TotalLength()
    {
        int total = 0;
        for (int i = 0; i < _chunks.Count; i++) total += _chunks[i].Length;
        return total;
    }

    // Mirrors JS concatChunks: pulls `size` bytes from the head of _chunks,
    // slicing the head chunk if it contributes more than remains.
    private byte[] ConcatChunks(int size)
    {
        var buffer = new byte[size];
        int j = 0;
        for (int i = 0; i < size; i++)
        {
            buffer[i] = _chunks[0][j++];
            if (j == _chunks[0].Length)
            {
                _chunks.RemoveAt(0);
                j = 0;
            }
        }
        if (_chunks.Count > 0 && j < _chunks[0].Length)
        {
            _chunks[0] = _chunks[0].Substring(j);
        }
        return buffer;
    }
}

internal static class ByteArrayExtensions
{
    public static byte[] Substring(this byte[] src, int start)
    {
        var dst = new byte[src.Length - start];
        Array.Copy(src, start, dst, 0, dst.Length);
        return dst;
    }
}
```

- [ ] **Step 4: Run, verify pass**

Run: `dotnet test` → all PASS.

- [ ] **Step 5: Commit**

```
git add -A
git commit -m "feat(parser): PacketDecoderStream state-machine push/pull adapter"
```

---

## Task 8: Port remaining test cases 1:1 (index.ts / node.ts full coverage) — ✅ DONE

This task fills any gaps not covered by Tasks 2–7's unit tests so every `it()` in upstream `index.ts` and `node.ts` has a 1:1 ported test. The encoder/decoder stream tests already live in Tasks 6–7; here we ensure the *non-stream* payload/single-packet cases from `index.ts` and `node.ts` are all present (most are in Tasks 3–5).

**Files:**
- Modify/Create: `tests/.../IndexTests.cs`, `tests/.../NodeTests.cs`, `tests/.../BrowserTests.cs`, `tests/.../TestUtil.cs`

- [ ] **Step 1: Add `TestUtil.cs`**

```csharp
using SharpSocketIO.EngineIo.Parser.Commons;
using Xunit;

namespace SharpSocketIO.EngineIo.Parser.Tests;

internal static class TestUtil
{
    // Port of test/util.ts areArraysEqual + createArrayBuffer.
    public static bool AreArraysEqual(ArrayBuffer x, ArrayBuffer y) => x.BytesEqual(y);
    public static bool AreArraysEqual(byte[] x, byte[] y)
    {
        if (x.Length != y.Length) return false;
        for (int i = 0; i < x.Length; i++) if (x[i] != y[i]) return false;
        return true;
    }

    public static ArrayBuffer CreateArrayBuffer(params int[] bytes)
    {
        var b = new byte[bytes.Length];
        for (int i = 0; i < bytes.Length; i++) b[i] = (byte)bytes[i];
        return new ArrayBuffer(b);
    }
}
```

- [ ] **Step 2: Write `IndexTests.cs` aggregating index.ts cases (single packet, payload, stream → thin wrappers around the focused test classes, kept here so the file structure mirrors upstream 1:1)**

(Cases already covered by `EncodePacketTests`/`DecodePacketTests`/`PayloadTests`/stream tests are *not* duplicated; this file documents the 1:1 mapping and adds the `BrowserTests` ArrayBuffer cases from `browser.ts`.)

`tests/.../BrowserTests.cs`:
```csharp
using SharpSocketIO.EngineIo.Parser.Commons;
using Xunit;

namespace SharpSocketIO.EngineIo.Parser.Tests;

// Port of test/browser.ts — ArrayBuffer cases only. Blob has no .NET primitive
// (design spec §6.1); the four Blob-only upstream tests are intentionally omitted.
public class BrowserTests
{
    [Fact]
    public void Encodes_and_decodes_an_arraybuffer()
    {
        var data = TestUtil.CreateArrayBuffer(1, 2, 3, 4);
        var packet = new Packet(PacketType.Message, new RawData(data));
        RawData encoded = default!;
        EncodePacket.Encode(packet, true, r => encoded = r);
        Assert.True(TestUtil.AreArraysEqual(data, encoded.AsArrayBuffer()!.Value));
        var decoded = DecodePacket.Decode(encoded, BinaryType.ArrayBuffer);
        Assert.Equal(PacketType.Message, decoded.Type);
        Assert.True(TestUtil.AreArraysEqual(data, decoded.Data!.Value.AsArrayBuffer()!.Value));
    }

    [Fact]
    public void Encodes_and_decodes_an_arraybuffer_as_base64()
    {
        var data = TestUtil.CreateArrayBuffer(1, 2, 3, 4);
        var packet = new Packet(PacketType.Message, new RawData(data));
        RawData encoded = default!;
        EncodePacket.Encode(packet, false, r => encoded = r);
        Assert.Equal("bAQIDBA==", encoded.AsString());
        var decoded = DecodePacket.Decode(encoded, BinaryType.ArrayBuffer);
        Assert.Equal(PacketType.Message, decoded.Type);
        Assert.True(TestUtil.AreArraysEqual(data, decoded.Data!.Value.AsArrayBuffer()!.Value));
    }

    [Fact]
    public void Encodes_a_string_plus_arraybuffer_payload()
    {
        var packets = new[]
        {
            new Packet(PacketType.Message, new RawData("test")),
            new Packet(PacketType.Message, new RawData(TestUtil.CreateArrayBuffer(1, 2, 3, 4))),
        };
        var payload = EngineIoParser.EncodePayload(packets);
        Assert.Equal("4test" + ((char)30) + "bAQIDBA==", payload);
        // equality: data is an ArrayBuffer; EncodePayload forces base64 so decoded
        // data is byte[]/ArrayBuffer depending on binaryType
        var decoded = EngineIoParser.DecodePayload(payload, BinaryType.ArrayBuffer);
        Assert.Equal(2, decoded.Count);
        Assert.Equal(packets[0], decoded[0]);
        Assert.Equal(PacketType.Message, decoded[1].Type);
        Assert.True(TestUtil.AreArraysEqual(TestUtil.CreateArrayBuffer(1, 2, 3, 4),
            decoded[1].Data!.Value.AsArrayBuffer()!.Value));
    }

    [Fact]
    public void Encodes_a_string_plus_zero_length_arraybuffer_payload()
    {
        var packets = new[]
        {
            new Packet(PacketType.Message, new RawData("test")),
            new Packet(PacketType.Message, new RawData(TestUtil.CreateArrayBuffer())),
        };
        var payload = EngineIoParser.EncodePayload(packets);
        Assert.Equal("4test" + ((char)30) + "b", payload);
        var decoded = EngineIoParser.DecodePayload(payload, BinaryType.ArrayBuffer);
        Assert.Equal(packets, decoded);
    }
}
```

- [ ] **Step 3: Run full suite**

Run: `dotnet test`
Expected: all tests across all TFMs PASS.

- [ ] **Step 4: Commit**

```
git add -A
git commit -m "test(parser): port remaining browser/node payload + single-packet cases"
```

---

## Task 9: Final verification & wiring — ✅ DONE

45/45 tests pass in Release across net8.0, net9.0, net10.0; library also compiles
for netstandard2.1. Wire-format golden assertions all hold. Branch
`feature/engine-io-parser-port` ready for review/merge; next cycle =
`socket.io-parser`.

- [ ] **Step 1: Clean rebuild + test across all TFMs**

Run: `dotnet test -c Release`
Expected: all tests PASS on net8.0, net9.0, net10.0.

- [ ] **Step 2: Spot-check wire-format compat assertions held**

Confirm these exact strings appear/hold in passing tests:
- `"0\x1e1\x1e2probe\x1e3probe\x1e4test"` payload
- `"bAQIDBA=="` base64 packet
- `"4test\x1ebAQIDBA=="` string+buffer payload
- `"bMTIzNAECAwQ="` NOT required (Blob only) — skip
- header bytes `{254, 48, 57}` (medium) and `{255, 0, 0, 0, 0, 7, 91, 205, 21}` (big)
- error packet on `0`-length, on `> maxPayload`, on >MAX_SAFE_INTEGER

- [ ] **Step 3: Commit any cleanup, leave branch ready for review**

```
git add -A
git commit -m "chore(parser): final verification across TFMs" --allow-empty
```

---

## Self-review

**Spec coverage:** spec §1 (DoD 1–6) → Tasks 1, 2–7, 8, 9 respectively. §3 type mapping → Task 2. §4 behavior contract → Tasks 3, 4, 5, 6, 7 each. §6.1 Blob omission → BrowserTests comment + omitted cases. §6.2 ArrayBuffer modeling → ArrayBuffer.cs (Task 2). §6.5 TransformStream→adapters → Tasks 6, 7. No spec section uncovered.

**Placeholder scan:** none — all steps carry full code/commands/expected output.

**Type consistency:** `EncodePacket.Encode` / `EncodeToBinary` (Task 3) names used identically in Tasks 5, 6. `DecodePacket.Decode` (Task 4) used in Tasks 5, 7. `EngineIoParser.EncodePayload/DecodePayload/Protocol` (Task 5) used in Task 8. `PacketEncoderStream`/`PacketDecoderStream` (Tasks 6, 7). `RawData`, `ArrayBuffer`, `Packet`, `PacketType`, `BinaryType` consistent throughout. `Substring` extension on `byte[]` is isolated in Task 7 and used only there.

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-06-18-engine-io-parser-port.md`.
**Inline execution** is the appropriate mode here (single-developer TDD, fast red→green loops, no parallelizable independent subtasks). Proceeding inline via the executing-plans discipline.
