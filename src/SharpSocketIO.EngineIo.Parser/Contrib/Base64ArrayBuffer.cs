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
            if (p < bufferLength) bytes[p++] = (byte)((e1 << 2) | (e2 >> 4));
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
