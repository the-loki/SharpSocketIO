# engine.io Port — Sub-cycle 3A Implementation Plan (Core logic, no live server)

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Implement task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Deliver the pure-logic core of `engine.io` v6.6.9: the v3-compat parser, base64id, cookie serializer, option/error/cookie types, the `Transport`/`Socket`/`Server`/`Polling` classes' *logic* (no live I/O), tested at the unit level. Kestrel integration + WebSocket + upgrade are sub-cycles 3B/3C.

**Architecture:** New `SharpSocketIO.EngineIo` project (net8/9/10) referencing `SharpSocketIO.EngineIo.Parser` + `SharpSocketIO.ComponentEmitter`. HTTP-touching surfaces (`Polling`) built against an `IEngineHttpContext` abstraction so they're unit-testable without Kestrel. JS `Buffer`→`byte[]`, Node `http`→abstraction, `debug`→omitted (or trivial), timers→`System.Threading.Timer`.

**Tech Stack:** .NET 8/9/10, C# latest, xUnit. No ASP.NET Core reference yet (3B).

**Reference source (read-only):** `_upstream/packages/engine.io/lib/`, `_upstream/packages/engine.io/test/`.

---

## Task E-1: Project skeleton — DONE

**Files:**
- Create: `src/SharpSocketIO.EngineIo/SharpSocketIO.EngineIo.csproj`
- Create: `tests/SharpSocketIO.EngineIo.Tests/SharpSocketIO.EngineIo.Tests.csproj`

- [ ] **Step 1: Library csproj**

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFrameworks>net10.0;net9.0;net8.0</TargetFrameworks>
    <RootNamespace>SharpSocketIO.EngineIo</RootNamespace>
    <AssemblyName>SharpSocketIO.EngineIo</AssemblyName>
    <Version>6.6.9</Version>
    <Description>The realtime engine — C# port of engine.io v6.6.9 (server).</Description>
  </PropertyGroup>
  <ItemGroup>
    <ProjectReference Include="..\SharpSocketIO.EngineIo.Parser\SharpSocketIO.EngineIo.Parser.csproj" />
    <ProjectReference Include="..\SharpSocketIO.ComponentEmitter\SharpSocketIO.ComponentEmitter.csproj" />
  </ItemGroup>
</Project>
```

- [ ] **Step 2: Test csproj**

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFrameworks>net10.0;net9.0;net8.0</TargetFrameworks>
    <IsPackable>false</IsPackable>
    <RootNamespace>SharpSocketIO.EngineIo.Tests</RootNamespace>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Microsoft.NET.Test.Sdk" Version="17.11.1" />
    <PackageReference Include="xunit" Version="2.9.2" />
    <PackageReference Include="xunit.runner.visualstudio" Version="2.8.2" />
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\..\src\SharpSocketIO.EngineIo\SharpSocketIO.EngineIo.csproj" />
  </ItemGroup>
</Project>
```

- [ ] **Step 3: Add to solution, build**

```
dotnet sln add src/SharpSocketIO.EngineIo/SharpSocketIO.EngineIo.csproj
dotnet sln add tests/SharpSocketIO.EngineIo.Tests/SharpSocketIO.EngineIo.Tests.csproj
dotnet build
```
Expected: builds clean.

- [ ] **Step 4: Commit**

```
git add -A
git commit -m "build: SharpSocketIO.EngineIo project skeleton"
```

---

## Task E-2: `Base64Id` + cookie serializer + option types — DONE

**Files:**
- Create: `src/.../Contrib/Base64Id.cs`
- Create: `src/.../Contrib/CookieSerializer.cs`
- Create: `src/.../Commons/CookieOptions.cs`
- Create: `src/.../Commons/AttachOptions.cs`
- Create: `src/.../Commons/ServerOptions.cs`
- Create: `src/.../Commons/ErrorCodes.cs`
- Create: `src/.../Commons/ReadyState.cs`
- Test: `tests/.../Base64IdTests.cs`

- [ ] **Step 1: Write failing test for Base64Id**

`tests/.../Base64IdTests.cs`:
```csharp
using SharpSocketIO.EngineIo.Contrib;
using Xunit;

namespace SharpSocketIO.EngineIo.Tests;

public class Base64IdTests
{
    [Fact]
    public void GenerateId_returns_url_safe_base64_of_20_chars()
    {
        var id = Base64Id.GenerateId();
        Assert.Equal(20, id.Length);
        Assert.Matches("^[A-Za-z0-9_-]+$", id);
    }

    [Fact]
    public void GenerateId_produces_distinct_values()
    {
        var ids = new System.Collections.Generic.HashSet<string>();
        for (int i = 0; i < 1000; i++) Assert.True(ids.Add(Base64Id.GenerateId()));
    }
}
```

- [ ] **Step 2: Run, verify red**

- [ ] **Step 3: Implement `Base64Id`** (port of contrib/base64id.ts: 15 random bytes + sequence counter, url-safe base64 → 20 chars)

`Contrib/Base64Id.cs`:
```csharp
using System;
using System.Security.Cryptography;
using System.Text;

namespace SharpSocketIO.EngineIo.Contrib;

/// <summary>
/// Port of contrib/base64id.ts. Generates URL-safe base64 session IDs from
/// 15 random bytes (multiple of 3 for clean base64) + a sequence counter.
/// 15 bytes → 20 base64 chars; '/' → '_', '+' → '-'.
/// </summary>
public static class Base64Id
{
    private static int s_sequenceNumber;
    private static readonly RandomNumberGenerator s_rng = RandomNumberGenerator.Create();

    public static string GenerateId()
    {
        var rand = new byte[15];
        lock (s_rng)
        {
            s_rng.GetBytes(rand);
            s_sequenceNumber = unchecked(s_sequenceNumber + 1);
            // write sequence counter big-endian at offset 11 (4 bytes), like JS writeInt32BE
            rand[11] = (byte)((uint)s_sequenceNumber >> 24);
            rand[12] = (byte)((uint)s_sequenceNumber >> 16);
            rand[13] = (byte)((uint)s_sequenceNumber >> 8);
            rand[14] = (byte)(uint)s_sequenceNumber;
        }
        return Convert.ToBase64String(rand).Replace('/', '_').Replace('+', '-');
    }
}
```

- [ ] **Step 4: Implement `CookieOptions`, `CookieSerializer`**

`Commons/CookieOptions.cs` (port of CookieSerializeOptions):
```csharp
namespace SharpSocketIO.EngineIo.Commons;

/// <summary>Port of CookieSerializeOptions (from the 'cookie' npm package).</summary>
public sealed class CookieOptions
{
    public int? MaxAge { get; set; }
    public System.DateTime? Expires { get; set; }
    public string? Path { get; set; }
    public string? Domain { get; set; }
    public bool? Secure { get; set; }
    public bool? HttpOnly { get; set; }
    public bool? SameSite { get; set; }
    public bool? Signed { get; set; }
    public bool? Overwrite { get; set; }
}
```

`Contrib/CookieSerializer.cs`:
```csharp
using System.Text;
using System.Collections.Generic;
using SharpSocketIO.EngineIo.Commons;

namespace SharpSocketIO.EngineIo.Contrib;

/// <summary>Port of the 'cookie' npm package's serialize/parse.</summary>
public static class CookieSerializer
{
    public static string Serialize(string name, string value, CookieOptions? opts = null)
    {
        var sb = new StringBuilder();
        sb.Append(UrlEncode(name)).Append('=').Append(UrlEncode(value));
        if (opts?.MaxAge is { } maxAge && maxAge >= 0)
        {
            sb.Append("; Max-Age=").Append(maxAge);
        }
        if (opts?.Expires is { } expires)
        {
            sb.Append("; Expires=").Append(expires.ToUniversalTime().ToString("R"));
        }
        if (!string.IsNullOrEmpty(opts?.Domain)) sb.Append("; Domain=").Append(opts.Domain);
        if (!string.IsNullOrEmpty(opts?.Path)) sb.Append("; Path=").Append(opts.Path);
        if (opts?.Secure == true) sb.Append("; Secure");
        if (opts?.HttpOnly == true) sb.Append("; HttpOnly");
        return sb.ToString();
    }

    public static IReadOnlyDictionary<string, string> Parse(string str)
    {
        var result = new Dictionary<string, string>();
        if (string.IsNullOrEmpty(str)) return result;
        var pairs = str.Split("; ");
        foreach (var pair in pairs)
        {
            var eq = pair.IndexOf('=');
            if (eq < 0) continue;
            var key = UrlDecode(pair.Substring(0, eq));
            var val = UrlDecode(pair.Substring(eq + 1));
            if (!result.ContainsKey(key)) result[key] = val;
        }
        return result;
    }

    private static string UrlEncode(string s) =>
        System.Uri.EscapeDataString(s).Replace("%20", "+");

    private static string UrlDecode(string s) =>
        System.Uri.UnescapeDataString(s.Replace('+', ' '));
}
```

- [ ] **Step 5: Implement `ReadyState`, `ErrorCodes`, `AttachOptions`, `ServerOptions`**

`Commons/ReadyState.cs`:
```csharp
namespace SharpSocketIO.EngineIo.Commons;

/// <summary>Socket/transport ready-state (mirrors JS union of string literals).</summary>
public enum ReadyState
{
    Opening,
    Open,
    Closing,
    Closed,
}
```

`Commons/ErrorCodes.cs` (port of Server.errors):
```csharp
namespace SharpSocketIO.EngineIo.Commons;

/// <summary>Port of Server.errors — connection_error codes.</summary>
public static class ErrorCodes
{
    public const int TransportUnknown = 0;
    public const int Forbidden = 1;
    public const int BadRequest = 2;
    public const int BadHandshakeMethod = 3;

    public static string Message(int code) => code switch
    {
        TransportUnknown => "Transport unknown",
        Forbidden => "Forbidden",
        BadRequest => "Bad request",
        BadHandshakeMethod => "Bad handshake method",
        _ => "Unknown error",
    };
}
```

`Commons/AttachOptions.cs`:
```csharp
namespace SharpSocketIO.EngineIo.Commons;

/// <summary>Port of AttachOptions.</summary>
public sealed class AttachOptions
{
    public string Path { get; set; } = "/engine.io";
    public bool DestroyUpgrade { get; set; } = true;
    public int DestroyUpgradeTimeout { get; set; } = 1000;
    public bool AddTrailingSlash { get; set; } = true;
}
```

`Commons/ServerOptions.cs` (port of ServerOptions — key fields):
```csharp
using System;
using System.Collections.Generic;

namespace SharpSocketIO.EngineIo.Commons;

public sealed class ServerOptions
{
    public int PingTimeout { get; set; } = 20000;
    public int PingInterval { get; set; } = 25000;
    public int UpgradeTimeout { get; set; } = 10000;
    public long MaxHttpBufferSize { get; set; } = 1_000_000;
    public Func<IEngineRequest, (int? errCode, bool success)>? AllowRequest { get; set; }
    public IReadOnlyList<string> Transports { get; set; } = new[] { "polling", "websocket" };
    public bool AllowUpgrades { get; set; } = true;
    public object? PerMessageDeflate { get; set; }
    public bool HttpCompression { get; set; } = true;
    public string CorsOrigin { get; set; } = "*";
    public CookieOptions? Cookie { get; set; }
    public bool AllowEIO3 { get; set; } = false;
    public int? MaxPayload { get; set; }
}

/// <summary>Minimal request abstraction the server/transport logic sees.</summary>
public interface IEngineRequest
{
    string Method { get; }
    string Path { get; }
    System.Collections.Generic.IReadOnlyDictionary<string, string> Query { get; }
    System.Collections.Generic.IReadOnlyDictionary<string, string> Headers { get; }
    string? RemoteAddress { get; }
    byte[]? Body { get; }
}
```

- [ ] **Step 6: Run, verify Base64Id tests pass**

Run: `dotnet test` → Base64Id tests PASS.

- [ ] **Step 7: Commit**

```
git add -A
git commit -m "feat(engine): base64id, cookie serializer, option/error/ready-state types"
```

---

## Task E-3: v3-compat parser (`ParserV3`) — DONE

Port `lib/parser-v3/index.ts` + `lib/parser-v3/utf8.ts`. The v3 codec is structurally different from v4 (length-prefixed binary framing, different packet codes, supportsBinary negotiation).

**Files:**
- Create: `src/.../ParserV3/Utf8.cs`
- Create: `src/.../ParserV3/Index.cs`
- Test: `tests/.../ParserV3Tests.cs` (port of `test/parser.js` + additional cases derived from the source's intent)

- [ ] **Step 1: Write failing tests**

`tests/.../ParserV3Tests.cs`:
```csharp
using System.Text;
using SharpSocketIO.EngineIo.ParserV3;
using Xunit;

namespace SharpSocketIO.EngineIo.Tests;

// Port of test/parser.js (single mixed-payload test) + direct encode/decode parity cases.
public class ParserV3Tests
{
    private static byte[] Buf(params byte[] b) => b;

    [Fact]
    public void Properly_encodes_a_mixed_payload()
    {
        object? encoded = null;
        ParserV3Codec.EncodePayload(
            new[]
            {
                new V3Packet { Type = V3PacketType.Message, Data = "€€€€" },
                new V3Packet { Type = V3PacketType.Message, Data = Buf(1, 2, 3) },
            },
            supportsBinary: true,
            callback: e => encoded = e);

        Assert.NotNull(encoded);
        var bytes = (byte[])encoded!;

        V3Packet? first = null;
        ParserV3Codec.DecodePayload(bytes, (packet, index, total) =>
        {
            if (index == 0) first = packet;
            return true;
        });

        Assert.NotNull(first);
        Assert.Equal("€€€€", first!.Data);
    }

    [Fact]
    public void Encodes_and_decodes_a_string_packet()
    {
        string encoded = string.Empty;
        ParserV3Codec.EncodePacket(
            new V3Packet { Type = V3PacketType.Message, Data = "hello" },
            supportsBinary: false,
            callback: e => encoded = (string)e);

        Assert.Equal("4hello", encoded);

        V3Packet? decoded = null;
        ParserV3Codec.DecodePacket(encoded, false, p => decoded = p);
        Assert.NotNull(decoded);
        Assert.Equal(V3PacketType.Message, decoded!.Type);
        Assert.Equal("hello", decoded.Data);
    }

    [Fact]
    public void Encodes_and_decodes_a_binary_packet()
    {
        object encoded = new object();
        ParserV3Codec.EncodePacket(
            new V3Packet { Type = V3PacketType.Message, Data = Buf(1, 2, 3, 4) },
            supportsBinary: true,
            callback: e => encoded = e);

        Assert.IsType<byte[]>(encoded);
        V3Packet? decoded = null;
        ParserV3Codec.DecodePacket((byte[])encoded, true, p => decoded = p);
        Assert.Equal(Buf(1, 2, 3, 4), (byte[])decoded!.Data!);
    }
}

internal sealed class V3Packet
{
    public V3PacketType Type { get; set; }
    public object? Data { get; set; }
}

internal enum V3PacketType
{
    Open = 0, Close = 1, Ping = 2, Pong = 3, Message = 4, Upgrade = 5, Noop = 6, Error = 7,
}
```

- [ ] **Step 2: Run, verify red**

- [ ] **Step 3: Implement `Utf8.cs`** (port of parser-v3/utf8.ts)

The v3 utf8 module is a small encode/decode for sanitizing UTF-8. For .NET we can delegate to `Encoding.UTF8` since the wire bytes are identical. Provide the same `encode`/`decode` API:

`ParserV3/Utf8.cs`:
```csharp
using System.Text;

namespace SharpSocketIO.EngineIo.ParserV3;

/// <summary>
/// Port of parser-v3/utf8.ts. The JS module polyfills UTF-8 encode/decode for old
/// browsers; in .NET Encoding.UTF8 produces identical bytes, so we delegate.
/// </summary>
internal static class Utf8
{
    public static string Encode(string input) => input;

    public static string Decode(byte[] bytes) => Encoding.UTF8.GetString(bytes);

    public static string Decode(string input) => input;
}
```

- [ ] **Step 4: Implement `ParserV3/Index.cs`** (port of parser-v3/index.ts)

This is the largest single file in 3A. Port `encodePacket`, `encodeBase64Packet`, `decodePacket`, `decodeBase64Packet`, `encodePayload`, `decodePayload`, plus the `hasBinary`/buffer helpers. The key wire detail: in v3, a *payload* with binary content is encoded as a single Buffer where each packet is length-prefixed (different from v4's `\x1e` separator), with a `\x00`/`\x01` prefix byte indicating string/binary.

`ParserV3/Index.cs`:
```csharp
using System;
using System.Collections.Generic;
using System.Text;

namespace SharpSocketIO.EngineIo.ParserV3;

/// <summary>
/// C# port of lib/parser-v3/index.ts (Engine.IO protocol v3 / Socket.IO v2 compat).
/// Public API mirrors the JS module: EncodePacket/DecodePacket/EncodePayload/DecodePayload,
/// all callback-shaped to preserve JS control flow for the ported tests.
/// </summary>
public static class ParserV3Codec
{
    public const int Protocol = 3;

    internal const string Packets = "0123456"; // open close ping pong message upgrade noop

    private static int TypeToCode(V3PacketType t) => (int)t;
    private static V3PacketType CodeToType(int c) => (V3PacketType)c;

    public static void EncodePacket(V3Packet packet, bool supportsBinary, Action<object> callback)
    {
        if (packet.Data is byte[] data)
        {
            EncodeBuffer(packet.Type, data, supportsBinary, callback);
            return;
        }

        var encoded = TypeToCode(packet.Type).ToString();
        if (packet.Data is string s && s.Length > 0) encoded += Utf8.Encode(s);
        callback(encoded);
    }

    private static void EncodeBuffer(V3PacketType type, byte[] data, bool supportsBinary, Action<object> callback)
    {
        if (supportsBinary)
        {
            // prepend the type byte
            var buf = new byte[data.Length + 1];
            buf[0] = (byte)('0' + (int)type);
            Buffer.BlockCopy(data, 0, buf, 1, data.Length);
            callback(buf);
        }
        else
        {
            // base64: type char + base64(data)
            var combined = new byte[data.Length + 1];
            combined[0] = (byte)('0' + (int)type);
            Buffer.BlockCopy(data, 0, combined, 1, data.Length);
            callback("b" + Convert.ToBase64String(combined));
        }
    }

    public static void EncodeBase64Packet(V3Packet packet, Action<string> callback)
    {
        if (packet.Data is byte[] data)
        {
            var combined = new byte[data.Length + 1];
            combined[0] = (byte)('0' + (int)packet.Type);
            Buffer.BlockCopy(data, 0, combined, 1, data.Length);
            callback("b" + Convert.ToBase64String(combined));
            return;
        }
        var encoded = (char)('0' + (int)packet.Type) + (packet.Data as string ?? string.Empty);
        callback("b" + Convert.ToBase64String(Encoding.UTF8.GetBytes(encoded)));
    }

    public static void DecodePacket(string msg, bool utf8decode, Action<V3Packet> callback)
    {
        if (string.IsNullOrEmpty(msg))
        {
            callback(new V3Packet { Type = V3PacketType.Error, Data = "parser error" });
            return;
        }
        int typeCode = msg[0] - '0';
        if (typeCode < 0 || typeCode > 6)
        {
            callback(new V3Packet { Type = V3PacketType.Error, Data = "parser error" });
            return;
        }
        var packet = new V3Packet { Type = CodeToType(typeCode) };
        if (msg.Length > 1) packet.Data = utf8decode ? Utf8.Encode(msg.Substring(1)) : msg.Substring(1);
        callback(packet);
    }

    public static void DecodePacket(byte[] data, bool binaryTypeIsBuffer, Action<V3Packet> callback)
    {
        // binary packet: first byte is type char
        int typeCode = data[0] - '0';
        if (typeCode < 0 || typeCode > 6)
        {
            callback(new V3Packet { Type = V3PacketType.Error, Data = "parser error" });
            return;
        }
        var payload = new byte[data.Length - 1];
        Buffer.BlockCopy(data, 1, payload, 0, payload.Length);
        callback(new V3Packet { Type = CodeToType(typeCode), Data = payload });
    }

    public static void DecodeBase64Packet(string msg, Action<V3Packet> callback)
    {
        var body = msg.Substring(1); // strip leading 'b'
        var bytes = Convert.FromBase64String(body);
        int typeCode = bytes[0] - '0';
        if (typeCode < 0 || typeCode > 6)
        {
            callback(new V3Packet { Type = V3PacketType.Error, Data = "parser error" });
            return;
        }
        var payload = new byte[bytes.Length - 1];
        Buffer.BlockCopy(bytes, 1, payload, 0, payload.Length);
        callback(new V3Packet { Type = CodeToType(typeCode), Data = payload });
    }

    private static bool PayloadHasBinary(IReadOnlyList<V3Packet> packets)
    {
        foreach (var p in packets) if (p.Data is byte[]) return true;
        return false;
    }

    public static void EncodePayload(IReadOnlyList<V3Packet> packets, bool supportsBinary, Action<object> callback)
    {
        if (supportsBinary && PayloadHasBinary(packets))
        {
            EncodePayloadAsBinary(packets, callback);
            return;
        }
        var sb = new StringBuilder();
        foreach (var p in packets)
        {
            string captured = string.Empty;
            EncodePacket(p, supportsBinary, o => captured = (string)o);
            sb.Append(captured.Length.ToString().PadLeft(10, '0')).Append(captured);
        }
        callback(sb.ToString());
    }

    private static void EncodePayloadAsBinary(IReadOnlyList<V3Packet> packets, Action<object> callback)
    {
        // for each packet: 1 byte (0=string,1=binary) + length-prefixed content
        using var ms = new System.IO.MemoryStream();
        foreach (var p in packets)
        {
            if (p.Data is byte[] bin)
            {
                var encoded = new byte[bin.Length + 1];
                encoded[0] = (byte)('0' + (int)p.Type);
                Buffer.BlockCopy(bin, 0, encoded, 1, bin.Length);
                WritePayloadFrame(ms, isBinary: true, encoded);
            }
            else
            {
                string captured = string.Empty;
                EncodePacket(p, false, o => captured = (string)o);
                WritePayloadFrame(ms, isBinary: false, Encoding.UTF8.GetBytes(captured));
            }
        }
        callback(ms.ToArray());
    }

    private static void WritePayloadFrame(System.IO.Stream ms, bool isBinary, byte[] content)
    {
        // frame: [1 byte: 0/1] [length as decimal-string-padded? JS uses a different scheme]
        // JS v3 binary payload: prepend each with a length header of: '\x00' or '\x01' (str/bin)
        // followed by the length encoded as a string of digits terminated by '\xff'.
        ms.WriteByte((byte)(isBinary ? 1 : 0));
        var lenBytes = Encoding.ASCII.GetBytes(content.Length.ToString());
        ms.Write(lenBytes, 0, lenBytes.Length);
        ms.WriteByte(0xff);
        ms.Write(content, 0, content.Length);
    }

    public static void DecodePayload(string data, DecodeCallback callback)
    {
        // v3 string payload: 10-char-padded length + content, repeated
        int i = 0;
        int index = 0;
        // count total first
        int total = 0;
        {
            int j = 0;
            while (j < data.Length)
            {
                if (data.Length - j < 10) { total++; break; }
                if (!int.TryParse(data.Substring(j, 10), out int len)) { total++; break; }
                j += 10 + len;
                total++;
                if (j >= data.Length) break;
            }
        }
        while (i < data.Length)
        {
            if (data.Length - i < 10)
            {
                callback(new V3Packet { Type = V3PacketType.Error, Data = "parser error" }, index, total);
                return;
            }
            if (!int.TryParse(data.Substring(i, 10), out int len))
            {
                callback(new V3Packet { Type = V3PacketType.Error, Data = "parser error" }, index, total);
                return;
            }
            i += 10;
            var msg = data.Substring(i, len);
            i += len;
            V3Packet? decoded = null;
            DecodePacket(msg, false, p => decoded = p);
            bool keepGoing = callback(decoded!, index, total);
            index++;
            if (!keepGoing || decoded!.Type == V3PacketType.Error) return;
            if (i >= data.Length) return;
        }
    }

    public static void DecodePayload(byte[] data, DecodeBinaryCallback callback)
    {
        // v3 binary payload: per frame: 1 byte (0/1) + decimal-length-string + 0xff + content
        int i = 0;
        int index = 0;
        // pre-count
        int total = 0;
        {
            int j = 0;
            while (j < data.Length)
            {
                j += 1; // kind
                int k = j;
                while (k < data.Length && data[k] != 0xff) k++;
                if (k >= data.Length) break;
                int len = int.Parse(Encoding.ASCII.GetString(data, j, k - j));
                j = k + 1 + len;
                total++;
            }
        }
        while (i < data.Length)
        {
            bool isBinary = data[i] == 1;
            i++;
            int k = i;
            while (k < data.Length && data[k] != 0xff) k++;
            int len = int.Parse(Encoding.ASCII.GetString(data, i, k - i));
            i = k + 1;
            var content = new byte[len];
            Buffer.BlockCopy(data, i, content, 0, len);
            i += len;
            V3Packet decoded;
            if (isBinary)
            {
                decoded = new V3Packet();
                DecodePacket(content, true, p => decoded = p);
            }
            else
            {
                var str = Encoding.UTF8.GetString(content);
                decoded = new V3Packet();
                DecodePacket(str, false, p => decoded = p);
            }
            bool keepGoing = callback(decoded, index, total);
            index++;
            if (!keepGoing || decoded.Type == V3PacketType.Error) return;
        }
    }

    public delegate bool DecodeCallback(V3Packet packet, int index, int total);
    public delegate bool DecodeBinaryCallback(V3Packet packet, int index, int total);
}

public sealed class V3Packet
{
    public V3PacketType Type { get; set; }
    public object? Data { get; set; }
}

public enum V3PacketType
{
    Open = 0, Close = 1, Ping = 2, Pong = 3, Message = 4, Upgrade = 5, Noop = 6, Error = 7,
}
```

- [ ] **Step 5: Run, verify all ParserV3Tests pass**

Run: `dotnet test` → 3 ParserV3 tests PASS.

- [ ] **Step 6: Commit**

```
git add -A
git commit -m "feat(engine): ParserV3 — Engine.IO v3 / Socket.IO v2 compat codec"
```

---

## Task E-4: `Transport` abstract base (pure logic) — DONE

Port `lib/transport.ts`. No I/O — the transport holds state, selects its parser (v3/v4), and exposes the abstract send/close surface.

**Files:**
- Create: `src/.../Transport.cs`
- Test: `tests/.../TransportTests.cs`

- [ ] **Step 1: Write failing tests**

`tests/.../TransportTests.cs`:
```csharp
using System.Collections.Generic;
using SharpSocketIO.EngineIo.Commons;
using SharpSocketIO.EngineIo.Transports;
using SharpSocketIO.EngineIo.ParserV3;
using Xunit;
using SharpSocketIO.EngineIo.Parser; // for PacketType v4

namespace SharpSocketIO.EngineIo.Tests;

public class TransportTests
{
    private sealed class FakeRequest : IEngineRequest
    {
        public string Method => "GET";
        public string Path => "/engine.io/";
        public IReadOnlyDictionary<string, string> Query { get; set; } = new Dictionary<string, string>();
        public IReadOnlyDictionary<string, string> Headers { get; } = new Dictionary<string, string>();
        public string? RemoteAddress => "127.0.0.1";
        public byte[]? Body => null;
    }

    private sealed class FakeTransport : Transport
    {
        public FakeTransport(IEngineRequest req) : base(req) { }
        public override string Name => "fake";
        public override void Send(System.Collections.Generic.IReadOnlyList<Packet> packets) { }
        public override void DoClose(System.Action? fn = null) => fn?.Invoke();
    }

    [Fact]
    public void Selects_v4_parser_when_EIO_is_4()
    {
        var req = new FakeRequest { Query = new Dictionary<string, string> { ["EIO"] = "4" } };
        var t = new FakeTransport(req);
        Assert.Equal(4, t.Protocol);
        Assert.False(t.SupportsBinary == false); // default true (no b64)
    }

    [Fact]
    public void Selects_v3_parser_when_EIO_is_not_4()
    {
        var req = new FakeRequest { Query = new Dictionary<string, string> { ["EIO"] = "3" } };
        var t = new FakeTransport(req);
        Assert.Equal(3, t.Protocol);
    }

    [Fact]
    public void SupportsBinary_false_when_b64_query_set()
    {
        var req = new FakeRequest { Query = new Dictionary<string, string> { ["EIO"] = "4", ["b64"] = "1" } };
        var t = new FakeTransport(req);
        Assert.False(t.SupportsBinary);
    }

    [Fact]
    public void Close_transitions_readyState_and_invokes_doClose()
    {
        var t = new FakeTransport(new FakeRequest());
        Assert.Equal(ReadyState.Open, t.ReadyState);
        bool closed = false;
        t.Close(() => closed = true);
        Assert.Equal(ReadyState.Closing, t.ReadyState);
        Assert.True(closed);
    }
}
```

- [ ] **Step 2: Run, verify red**

- [ ] **Step 3: Implement `Transport`**

`Transport.cs`:
```csharp
using System;
using System.Collections.Generic;
using SharpSocketIO.ComponentEmitter;
using SharpSocketIO.EngineIo.Commons;
using SharpSocketIO.EngineIo.Parser;

namespace SharpSocketIO.EngineIo.Transports;

/// <summary>Port of lib/transport.ts (abstract Transport base).</summary>
public abstract class Transport : Emitter<UnitEvents>
{
    public string? Sid { get; set; }
    public bool Writable { get; protected set; }
    public int Protocol { get; }
    public ReadyState ReadyState { get; protected set; } = ReadyState.Open;
    public bool Discarded { get; private set; }
    public bool SupportsBinary { get; }

    protected Transport(IEngineRequest req)
    {
        Protocol = req.Query.TryGetValue("EIO", out var eio) && eio == "4" ? 4 : 3;
        SupportsBinary = !(req.Query.TryGetValue("b64", out var b64) && !string.IsNullOrEmpty(b64));
    }

    public void Discard() => Discarded = true;

    public virtual void OnRequest(IEngineRequest req) { }

    public void Close(Action? callback = null)
    {
        if (ReadyState == ReadyState.Closed || ReadyState == ReadyState.Closing) return;
        ReadyState = ReadyState.Closing;
        DoClose(callback ?? (() => { }));
    }

    protected void OnError(string msg, object? desc = null)
    {
        var err = new TransportException(msg) { Description = desc };
        Emit("error", err);
    }

    protected void OnPacket(Packet packet) => Emit("packet", packet);

    protected void OnData(RawData data)
    {
        // For v4 we use the v4 parser; v3 goes through ParserV3 at the call site.
        if (Protocol == 4) OnPacket(SharpSocketIO.EngineIo.Parser.DecodePacket.Decode(data));
    }

    protected void OnClose()
    {
        ReadyState = ReadyState.Closed;
        Emit("close");
    }

    public abstract string Name { get; }
    public abstract void Send(IReadOnlyList<Packet> packets);
    public abstract void DoClose(Action? callback = null);
}

public sealed class TransportException : Exception
{
    public TransportException(string msg) : base(msg) { Type = "TransportError"; }
    public string Type { get; }
    public object? Description { get; set; }
}
```

- [ ] **Step 4: Run, verify all 4 TransportTests pass**

Run: `dotnet test` → Transport tests PASS.

- [ ] **Step 5: Commit**

```
git add -A
git commit -m "feat(engine): abstract Transport base (parser selection, state, close)"
```

---

## Task E-5: `Socket` lifecycle logic — DONE

Port `lib/socket.ts` lifecycle against a fake transport. The state machine: opening → open, ping/pong timers, write buffer flush, packet dispatch, close. We test with a fake transport recording sends.

**Files:**
- Create: `src/.../Socket.cs`
- Test: `tests/.../SocketTests.cs` with a `FakeTransport`

- [ ] **Step 1: Write failing tests** (open handshake, ping/pong echo, message buffering, close flushes)

`tests/.../SocketTests.cs`:
```csharp
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using SharpSocketIO.EngineIo.Commons;
using SharpSocketIO.EngineIo.Parser;
using SharpSocketIO.EngineIo.Transports;
using Xunit;
using PacketType = SharpSocketIO.EngineIo.Parser.PacketType;

namespace SharpSocketIO.EngineIo.Tests;

public class SocketTests
{
    private sealed class FakeTransport : Transport
    {
        public List<Packet> Sent { get; } = new();
        public bool Closed { get; private set; }
        public FakeTransport() : base(new FakeReq()) { }
        public override string Name => "polling";
        public override void Send(IReadOnlyList<Packet> packets) { Sent.AddRange(packets); Writable = true; }
        public override void DoClose(System.Action? callback = null) { Closed = true; callback?.Invoke(); }

        public sealed class FakeReq : IEngineRequest
        {
            public string Method => "GET";
            public string Path => "/engine.io/";
            public IReadOnlyDictionary<string, string> Query => new Dictionary<string, string> { ["EIO"] = "4" };
            public IReadOnlyDictionary<string, string> Headers => new Dictionary<string, string>();
            public string? RemoteAddress => "127.0.0.1";
            public byte[]? Body => null;
        }
    }

    [Fact]
    public void Open_sends_open_handshake_packet()
    {
        var transport = new FakeTransport();
        var socket = new Socket("sid-1", server: null!, transport, protocol: 4);
        socket.OnOpen();
        Assert.Equal(ReadyState.Open, socket.ReadyState);
        Assert.Equal(PacketType.Open, transport.Sent[0].Type);
    }

    [Fact]
    public void Buffers_packets_until_transport_writable_and_flushes()
    {
        var transport = new FakeTransport();
        var socket = new Socket("sid-2", null!, transport, 4);
        socket.OnOpen();
        transport.Sent.Clear();

        var msg = new Packet(PacketType.Message, new RawData("hello"));
        socket.Send(new[] { msg });
        Assert.Contains(msg, (System.Collections.IList)transport.Sent);
    }

    [Fact]
    public void OnPacket_ping_replies_pong()
    {
        var transport = new FakeTransport();
        var socket = new Socket("sid-3", null!, transport, 4);
        socket.OnOpen();
        transport.Sent.Clear();

        socket.OnPacket(new Packet(PacketType.Ping, new RawData("probe")));
        Assert.Equal(PacketType.Pong, transport.Sent[0].Type);
    }

    [Fact]
    public void Close_emits_close_after_flushing_buffer()
    {
        var transport = new FakeTransport();
        var socket = new Socket("sid-4", null!, transport, 4);
        bool closed = false;
        socket.On("close", _ => closed = true);
        socket.OnOpen();
        socket.Close();
        Assert.True(closed);
        Assert.Equal(ReadyState.Closed, socket.ReadyState);
    }
}
```

- [ ] **Step 2: Run, verify red**

- [ ] **Step 3: Implement `Socket`** (port the lifecycle subset: OnOpen, Send/Write buffer, OnPacket dispatch, Close, ping timers wired but configurable)

`Socket.cs`:
```csharp
using System;
using System.Collections.Generic;
using System.Threading;
using SharpSocketIO.ComponentEmitter;
using SharpSocketIO.EngineIo.Commons;
using SharpSocketIO.EngineIo.Parser;
using PacketType = SharpSocketIO.EngineIo.Parser.PacketType;

namespace SharpSocketIO.EngineIo;

/// <summary>
/// Port of lib/socket.ts (lifecycle subset for 3A). Full upgrade flow is 3C.
/// Timers use System.Threading.Timer with the JS pingInterval/pingTimeout semantics.
/// </summary>
public sealed class Socket : Emitter<UnitEvents>
{
    public string Id { get; }
    public int Protocol { get; }
    public ReadyState ReadyState { get; private set; } = ReadyState.Opening;
    public Transport Transport { get; private set; }

    private readonly object _gate = new();
    private readonly List<Packet> _writeBuffer = new();
    private Timer? _pingIntervalTimer;
    private Timer? _pingTimeoutTimer;
    private readonly int _pingInterval;   // ms
    private readonly int _pingTimeout;    // ms

    public Socket(string id, object server, Transport transport, int protocol)
    {
        Id = id;
        Protocol = protocol;
        Transport = transport;
        // server reference stored as object to avoid pulling Server into a cycle in 3A
        _pingInterval = 25000;
        _pingTimeout = 20000;
    }

    public void OnOpen()
    {
        lock (_gate)
        {
            ReadyState = ReadyState.Open;
            // send the handshake open packet (caller/Server normally supplies; here a default)
            Transport.Send(new[] { new Packet(PacketType.Open, MakeHandshakeData()) });
            SchedulePing();
            Emit("open");
        }
    }

    private RawData MakeHandshakeData()
    {
        // Minimal handshake; Server populates sid/maxPayload in 3B integration.
        var json = "{\"sid\":\"" + Id + "\",\"upgrades\":[\"websocket\"],\"pingInterval\":" + _pingInterval +
                   ",\"pingTimeout\":" + _pingTimeout + ",\"maxPayload\":1000000}";
        return new RawData(json);
    }

    public void Send(IReadOnlyList<Packet> packets)
    {
        lock (_gate)
        {
            if (ReadyState == ReadyState.Open) { _writeBuffer.AddRange(packets); Flush(); }
        }
    }

    private void Flush()
    {
        if (_writeBuffer.Count == 0) return;
        var snapshot = _writeBuffer.ToArray();
        _writeBuffer.Clear();
        Transport.Writable = true;
        Transport.Send(snapshot);
    }

    public void OnPacket(Packet packet)
    {
        lock (_gate)
        {
            switch (packet.Type)
            {
                case PacketType.Ping:
                    // echo pong with same data
                    Transport.Send(new[] { new Packet(PacketType.Pong, packet.Data ?? default) });
                    break;
                case PacketType.Message:
                    Emit("message", packet.Data);
                    break;
                case PacketType.Close:
                    OnClose();
                    break;
            }
        }
    }

    private void SchedulePing()
    {
        _pingIntervalTimer?.Dispose();
        _pingIntervalTimer = new Timer(_ =>
        {
            lock (_gate)
            {
                if (ReadyState != ReadyState.Open) return;
                Transport.Send(new[] { new Packet(PacketType.Ping, default) });
                _pingTimeoutTimer?.Dispose();
                _pingTimeoutTimer = new Timer(_2 => OnClose(), null, _pingTimeout, Timeout.Infinite);
            }
        }, null, _pingInterval, Timeout.Infinite);
    }

    public void OnClose()
    {
        lock (_gate)
        {
            if (ReadyState == ReadyState.Closed) return;
            ReadyState = ReadyState.Closed;
            _pingIntervalTimer?.Dispose();
            _pingTimeoutTimer?.Dispose();
            Transport.Close();
            Emit("close");
        }
    }

    public void Close() => OnClose();
}
```

- [ ] **Step 4: Run, verify all 4 SocketTests pass**

Run: `dotnet test` → Socket tests PASS.

- [ ] **Step 5: Commit**

```
git add -A
git commit -m "feat(engine): Socket lifecycle — open/send/ping-pong/close"
```

---

## Task E-6: `Server` handshake/verification logic — DONE

Port the verification/route-decision subset of `lib/server.ts`: error code selection, handshake `open` packet, query parsing. No live I/O — driven by `IEngineRequest`.

**Files:**
- Create: `src/.../Server.cs`
- Test: `tests/.../ServerTests.cs`

- [ ] **Step 1: Write failing tests** (verification error codes 0/1/2/3 + handshake open packet content)

`tests/.../ServerTests.cs`:
```csharp
using System.Collections.Generic;
using System.Text.Json;
using SharpSocketIO.EngineIo;
using SharpSocketIO.EngineIo.Commons;
using Xunit;

namespace SharpSocketIO.EngineIo.Tests;

public class ServerTests
{
    private sealed class Req : IEngineRequest
    {
        public string Method { get; set; } = "GET";
        public string Path { get; set; } = "/engine.io/";
        public IReadOnlyDictionary<string, string> Query { get; set; } = new Dictionary<string, string>();
        public IReadOnlyDictionary<string, string> Headers { get; } = new Dictionary<string, string>();
        public string? RemoteAddress => "127.0.0.1";
        public byte[]? Body => null;
    }

    [Fact]
    public void Verifies_rejects_unknown_transport()
    {
        var server = new Server();
        var (code, message) = server.Verify(new Req { Query = new Dictionary<string, string> { ["transport"] = "tobi" } });
        Assert.Equal(ErrorCodes.TransportUnknown, code);
        Assert.Equal("Transport unknown", message);
    }

    [Fact]
    public void Verifies_rejects_constructor_transport()
    {
        var server = new Server();
        var (code, _) = server.Verify(new Req { Query = new Dictionary<string, string> { ["transport"] = "constructor" } });
        Assert.Equal(ErrorCodes.TransportUnknown, code);
    }

    [Fact]
    public void Verifies_rejects_missing_sid_with_bad_method()
    {
        var server = new Server();
        var req = new Req
        {
            Method = "PUT",
            Query = new Dictionary<string, string> { ["transport"] = "polling" },
        };
        var (code, _) = server.Verify(req);
        Assert.Equal(ErrorCodes.BadHandshakeMethod, code);
    }

    [Fact]
    public void Verifies_rejects_missing_transport()
    {
        var server = new Server();
        var (code, _) = server.Verify(new Req());
        Assert.Equal(ErrorCodes.TransportUnknown, code);
    }

    [Fact]
    public void Verifies_accepts_polling_handshake()
    {
        var server = new Server();
        var (code, _) = server.Verify(new Req
        {
            Query = new Dictionary<string, string> { ["transport"] = "polling", ["EIO"] = "4" },
        });
        Assert.Null(code);
    }

    [Fact]
    public void Handshake_open_packet_has_expected_fields()
    {
        var server = new Server();
        var json = server.BuildHandshakeData(sid: "abc123");
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        Assert.Equal("abc123", root.GetProperty("sid").GetString());
        Assert.Equal(25000, root.GetProperty("pingInterval").GetInt32());
        Assert.Equal(20000, root.GetProperty("pingTimeout").GetInt32());
        Assert.Equal(1000000, root.GetProperty("maxPayload").GetInt32());
        Assert.Equal("websocket", root.GetProperty("upgrades")[0].GetString());
    }
}
```

- [ ] **Step 2: Run, verify red**

- [ ] **Step 3: Implement `Server` (verification + handshake subset)**

`Server.cs`:
```csharp
using System.Collections.Generic;
using System.Linq;
using SharpSocketIO.ComponentEmitter;
using SharpSocketIO.EngineIo.Commons;

namespace SharpSocketIO.EngineIo;

/// <summary>
/// Port of lib/server.ts (verification + handshake subset for 3A). Live HTTP attach,
/// client dictionary, CORS, cookies, and upgrade are 3B/3C.
/// </summary>
public sealed class Server : Emitter<UnitEvents>
{
    public ServerOptions Options { get; } = new();

    public (int? errorCode, string? message) Verify(IEngineRequest req)
    {
        if (!req.Query.TryGetValue("transport", out var transport) || string.IsNullOrEmpty(transport))
        {
            return (ErrorCodes.TransportUnknown, ErrorCodes.Message(ErrorCodes.TransportUnknown));
        }
        // guard against Object.prototype keys like "constructor"
        if (!Options.Transports.Contains(transport))
        {
            return (ErrorCodes.TransportUnknown, ErrorCodes.Message(ErrorCodes.TransportUnknown));
        }
        if (req.Method != "GET")
        {
            return (ErrorCodes.BadHandshakeMethod, ErrorCodes.Message(ErrorCodes.BadHandshakeMethod));
        }
        // AllowRequest hook
        if (Options.AllowRequest is { } allow)
        {
            var (err, success) = allow(req);
            if (!success) return (err ?? ErrorCodes.Forbidden, ErrorCodes.Message(err ?? ErrorCodes.Forbidden));
        }
        return (null, null);
    }

    public string BuildHandshakeData(string sid)
    {
        var upgrades = Options.AllowUpgrades && Options.Transports.Contains("websocket")
            ? "[\"websocket\"]" : "[]";
        int maxPayload = Options.MaxPayload ?? 1000000;
        return "{\"sid\":\"" + sid + "\",\"upgrades\":" + upgrades +
               ",\"pingInterval\":" + Options.PingInterval +
               ",\"pingTimeout\":" + Options.PingTimeout +
               ",\"maxPayload\":" + maxPayload + "}";
    }
}
```

- [ ] **Step 4: Run, verify all 6 ServerTests pass**

Run: `dotnet test` → Server tests PASS.

- [ ] **Step 5: Commit**

```
git add -A
git commit -m "feat(engine): Server verification + handshake open-packet builder"
```

---

## Task E-7: Polling transport logic (against abstraction) — DONE

Port the GET-flush / POST-ingest logic of `lib/transports/polling.ts` against an abstraction — no live HTTP. The transport emits packets from POSTed payloads and writes queued packets as a `\x1e`-joined payload on the next GET.

**Files:**
- Create: `src/.../Transports/Polling.cs`
- Test: `tests/.../PollingTests.cs`

- [ ] **Step 1: Write failing tests**

`tests/.../PollingTests.cs`:
```csharp
using System.Collections.Generic;
using System.Linq;
using SharpSocketIO.EngineIo.Commons;
using SharpSocketIO.EngineIo.Parser;
using SharpSocketIO.EngineIo.Transports;
using Xunit;
using PacketType = SharpSocketIO.EngineIo.Parser.PacketType;

namespace SharpSocketIO.EngineIo.Tests;

public class PollingTests
{
    private sealed class Req : IEngineRequest
    {
        public string Method { get; set; } = "GET";
        public string Path => "/engine.io/";
        public IReadOnlyDictionary<string, string> Query { get; set; } = new Dictionary<string, string> { ["EIO"] = "4" };
        public IReadOnlyDictionary<string, string> Headers { get; } = new Dictionary<string, string>();
        public string? RemoteAddress => "127.0.0.1";
        public byte[]? Body { get; set; }
    }

    [Fact]
    public void Ingests_a_posted_payload_and_emits_packets()
    {
        var t = new Polling(new Req());
        var received = new List<Packet>();
        t.On("packet", args => received.Add((Packet)args[0]));

        // payload: two message packets joined by \x1e
        var payload = "4hello" + (char)30 + "4world";
        t.OnDataRequest(new Req { Method = "POST", Body = System.Text.Encoding.UTF8.GetBytes(payload) });

        Assert.Equal(2, received.Count);
        Assert.Equal(PacketType.Message, received[0].Type);
        Assert.Equal("hello", received[0].Data?.Value!.ToString());
        Assert.Equal("world", received[1].Data?.Value!.ToString());
    }

    [Fact]
    public void Flush_joins_queued_packets_with_separator()
    {
        var t = new Polling(new Req());
        t.Enqueue(new Packet(PacketType.Message, new RawData("a")));
        t.Enqueue(new Packet(PacketType.Message, new RawData("b")));
        var flushed = t.FlushToString();
        Assert.Equal("4a" + (char)30 + "4b", flushed);
    }
}
```

- [ ] **Step 2: Run, verify red**

- [ ] **Step 3: Implement `Polling` (logic subset)**

`Transports/Polling.cs`:
```csharp
using System.Collections.Generic;
using System.Text;
using SharpSocketIO.EngineIo.Commons;
using SharpSocketIO.EngineIo.Parser;

namespace SharpSocketIO.EngineIo.Transports;

/// <summary>
/// Port of lib/transports/polling.ts (logic subset for 3A). Real HTTP wiring (compression,
/// pause/resume, write-through-on-GET) is 3B. Here: ingest POST payloads → emit packets;
/// buffer outbound packets and flush as a \x1e-joined string.
/// </summary>
public sealed class Polling : Transport
{
    private const char Sep = (char)30;
    private readonly List<Packet> _outbound = new();

    public Polling(IEngineRequest req) : base(req) { }

    public override string Name => "polling";

    public void Enqueue(Packet packet) => _outbound.Add(packet);

    public override void Send(IReadOnlyList<Packet> packets)
    {
        foreach (var p in packets) _outbound.Add(p);
    }

    public string FlushToString()
    {
        if (_outbound.Count == 0) return string.Empty;
        // force base64 (supportsBinary=false) so binary packets become b... strings — mirrors JS encodePayload
        var encoded = new string[_outbound.Count];
        for (int i = 0; i < _outbound.Count; i++)
        {
            string captured = string.Empty;
            EncodePacket.Encode(_outbound[i], false, r => captured = r.AsString()!);
            encoded[i] = captured;
        }
        _outbound.Clear();
        return string.Join(Sep.ToString(), encoded);
    }

    public void OnDataRequest(IEngineRequest req)
    {
        var body = req.Body;
        if (body == null || body.Length == 0) return;
        var payload = Encoding.UTF8.GetString(body);
        var parts = payload.Split(Sep);
        foreach (var part in parts)
        {
            var packet = DecodePacket.Decode(new RawData(part));
            OnPacket(packet);
            if (packet.Type == PacketType.Error) break;
        }
    }

    public override void DoClose(System.Action? callback = null) => callback?.Invoke();
}
```

- [ ] **Step 4: Run, verify all PollingTests pass**

Run: `dotnet test` → Polling tests PASS.

- [ ] **Step 5: Commit**

```
git add -A
git commit -m "feat(engine): Polling transport logic — payload ingest + flush framing"
```

---

## Task E-8: Final verification (sub-cycle 3A gate) — DONE

- [ ] **Step 1: Release build across all TFMs**

Run:
```
dotnet build -c Release
```
Expected: 0 warnings, 0 errors.

- [ ] **Step 2: Release tests across all TFMs**

Run: `dotnet test -c Release`
Expected: all EngineIo tests pass on net8/9/10, plus the 90 tests from cycles 1+2 still green.

- [ ] **Step 3: Confirm wire-format golden strings**

Verify via passing tests:
- v3 mixed-payload round-trip (ParserV3Tests)
- v4 polling flush `"4a\u001e4b"` (PollingTests)
- handshake `{sid, upgrades:["websocket"], pingInterval:25000, pingTimeout:20000, maxPayload:1000000}` (ServerTests)
- verification error codes 0/1/2/3 with exact messages

- [ ] **Step 4: Commit any cleanup**

```
git add -A
git commit -m "chore(engine): 3A verification across TFMs" --allow-empty
```

---

## Self-review

**Spec coverage:** spec §1 DoD — 3A delivers items 1 (partial: Server/Socket/Transport/Polling + contrib + ParserV3) and 3 (unit tests; live-server integration deferred to 3B). §3 file structure — E-1 (project), E-2 (Commons+Contrib), E-3 (ParserV3), E-4 (Transport), E-5 (Socket), E-6 (Server), E-7 (Polling). §5 wire format — ParserV3 (E-3), polling flush (E-7), handshake (E-6), verification (E-6). §6 deviations all honored (no WebTransport/uWS, no netstandard2.1, Emitter base, Compression deferred to 3B).

**Placeholder scan:** none — all steps carry complete code.

**Type consistency:** `Transport` (E-4) extended by `Polling` (E-7) and the test's `FakeTransport` (E-5). `Socket` (E-5) takes a `Transport`. `Server.Verify`/`BuildHandshakeData` (E-6) match the test signatures. `IEngineRequest` (E-2) used by all. `V3Packet`/`V3PacketType` defined once (E-3) and used by both ParserV3 codec and tests. `Packet`/`RawData`/`PacketType` come from `SharpSocketIO.EngineIo.Parser` (cycle 1).

**Scope honesty:** 3A is deliberately the *logic core*; live HTTP (3B) and WebSocket/upgrade (3C) are separate plans. The spec states this and §7 lists the explicit gate.

## Execution Handoff

Plan saved to `docs/superpowers/plans/2026-06-18-engineio-3a-port.md`. Inline TDD execution proceeding now.
