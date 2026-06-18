using System.Collections.Generic;
using System.Text;
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
        if (!string.IsNullOrEmpty(opts?.SameSite)) sb.Append("; SameSite=").Append(Capitalize(opts.SameSite));
        return sb.ToString();
    }

    private static string Capitalize(string? s) => s switch
    {
        "lax" or "Lax" => "Lax",
        "strict" or "Strict" => "Strict",
        "none" or "None" => "None",
        _ => s!,
    };

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
