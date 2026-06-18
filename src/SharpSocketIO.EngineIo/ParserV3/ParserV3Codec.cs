using System;
using System.Collections.Generic;
using System.IO;
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

    public static void EncodePacket(V3Packet packet, bool supportsBinary, Action<object> callback)
    {
        if (packet.Data is byte[] data)
        {
            EncodeBuffer(packet.Type, data, supportsBinary, callback);
            return;
        }

        var encoded = ((int)packet.Type).ToString();
        if (packet.Data is string s && s.Length > 0) encoded += Utf8.Encode(s);
        callback(encoded);
    }

    private static void EncodeBuffer(V3PacketType type, byte[] data, bool supportsBinary, Action<object> callback)
    {
        if (supportsBinary)
        {
            var buf = new byte[data.Length + 1];
            buf[0] = (byte)('0' + (int)type);
            Buffer.BlockCopy(data, 0, buf, 1, data.Length);
            callback(buf);
        }
        else
        {
            var combined = new byte[data.Length + 1];
            combined[0] = (byte)('0' + (int)type);
            Buffer.BlockCopy(data, 0, combined, 1, data.Length);
            callback("b" + Convert.ToBase64String(combined));
        }
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
        var packet = new V3Packet { Type = (V3PacketType)typeCode };
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
        callback(new V3Packet { Type = (V3PacketType)typeCode, Data = payload });
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
        callback(new V3Packet { Type = (V3PacketType)typeCode, Data = payload });
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
        // v3 binary payload framing: per packet, 1 byte kind (0=string,1=binary) +
        // ASCII decimal length + 0xff terminator + content bytes.
        using var ms = new MemoryStream();
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

    private static void WritePayloadFrame(Stream ms, bool isBinary, byte[] content)
    {
        ms.WriteByte((byte)(isBinary ? 1 : 0));
        var lenBytes = Encoding.ASCII.GetBytes(content.Length.ToString());
        ms.Write(lenBytes, 0, lenBytes.Length);
        ms.WriteByte(0xff);
        ms.Write(content, 0, content.Length);
    }

    public delegate bool DecodeCallback(V3Packet packet, int index, int total);

    public delegate bool DecodeBinaryCallback(V3Packet packet, int index, int total);

    public static void DecodePayload(string data, DecodeCallback callback)
    {
        // v3 string payload: 10-char-padded length + content, repeated
        int total = CountStringFrames(data);
        int i = 0;
        int index = 0;
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

    private static int CountStringFrames(string data)
    {
        int total = 0;
        int j = 0;
        while (j < data.Length)
        {
            if (data.Length - j < 10) { total++; break; }
            if (!int.TryParse(data.Substring(j, 10), out int len)) { total++; break; }
            j += 10 + len;
            total++;
            if (j >= data.Length) break;
        }
        return total;
    }

    public static void DecodePayload(byte[] data, DecodeBinaryCallback callback)
    {
        int total = CountBinaryFrames(data);
        int i = 0;
        int index = 0;
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
            V3Packet decoded = new V3Packet();
            if (isBinary)
            {
                DecodePacket(content, true, p => decoded = p);
            }
            else
            {
                var str = Encoding.UTF8.GetString(content);
                DecodePacket(str, false, p => decoded = p);
            }
            bool keepGoing = callback(decoded, index, total);
            index++;
            if (!keepGoing || decoded.Type == V3PacketType.Error) return;
        }
    }

    private static int CountBinaryFrames(byte[] data)
    {
        int total = 0;
        int j = 0;
        while (j < data.Length)
        {
            j += 1; // kind byte
            int k = j;
            while (k < data.Length && data[k] != 0xff) k++;
            if (k >= data.Length) break;
            int len = int.Parse(Encoding.ASCII.GetString(data, j, k - j));
            j = k + 1 + len;
            total++;
        }
        return total;
    }
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
