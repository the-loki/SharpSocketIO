using System.Collections.Generic;
using SharpSocketIO.EngineIo.Parser.Commons;

namespace SharpSocketIO.EngineIo.Parser;

/// <summary>
/// Port of lib/index.ts (payload portion). Exposes encode/decode packet + payload
/// helpers and the protocol version. (Stream adapters live in their own types —
/// see design spec §3.)
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
