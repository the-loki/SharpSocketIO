using System.Collections.Generic;
using System.Text;

namespace SharpSocketIO.EngineIo.Client.Contrib;

/// <summary>Port of contrib/parseqs.ts — encode/decode query string maps.</summary>
public static class Parseqs
{
    public static string Encode(IReadOnlyDictionary<string, string> obj)
    {
        var sb = new StringBuilder();
        foreach (var kv in obj)
        {
            if (sb.Length > 0) sb.Append('&');
            sb.Append(System.Uri.EscapeDataString(kv.Key)).Append('=').Append(System.Uri.EscapeDataString(kv.Value ?? ""));
        }
        return sb.ToString();
    }

    public static IReadOnlyDictionary<string, string> Decode(string qs)
    {
        var result = new Dictionary<string, string>();
        if (string.IsNullOrEmpty(qs)) return result;
        foreach (var pair in qs.Split('&'))
        {
            var eq = pair.IndexOf('=');
            string key, val;
            if (eq < 0) { key = System.Uri.UnescapeDataString(pair); val = ""; }
            else
            {
                key = System.Uri.UnescapeDataString(pair.Substring(0, eq));
                val = System.Uri.UnescapeDataString(pair.Substring(eq + 1));
            }
            result[key] = val;
        }
        return result;
    }
}
