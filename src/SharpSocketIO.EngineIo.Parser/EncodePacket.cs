using System;
using System.Text;
using SharpSocketIO.EngineIo.Parser.Commons;
using SharpSocketIO.EngineIo.Parser.Contrib;

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
                callback(new RawData("b" + Base64ArrayBuffer.Encode(buf, off, len)));
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
