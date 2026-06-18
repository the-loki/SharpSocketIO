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
