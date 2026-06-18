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
        char typeChar = s[0];
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
