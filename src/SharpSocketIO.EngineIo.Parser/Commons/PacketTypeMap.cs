using System.Collections.Generic;

namespace SharpSocketIO.EngineIo.Parser.Commons;

/// <summary>
/// Port of PACKET_TYPES / PACKET_TYPES_REVERSE from lib/commons.ts.
/// Single source of truth: derived from the PacketType enum so wire codes can't drift.
/// Note: Error has no on-wire code (it is synthesised locally on parse failure).
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
