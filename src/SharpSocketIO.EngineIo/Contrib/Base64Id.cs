using System;
using System.Security.Cryptography;

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
            uint s = (uint)s_sequenceNumber;
            rand[11] = (byte)(s >> 24);
            rand[12] = (byte)(s >> 16);
            rand[13] = (byte)(s >> 8);
            rand[14] = (byte)s;
        }
        return Convert.ToBase64String(rand).Replace('/', '_').Replace('+', '-');
    }
}
