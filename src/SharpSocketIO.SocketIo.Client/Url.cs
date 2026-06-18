using System.Collections.Generic;

namespace SharpSocketIO.SocketIo.Client;

/// <summary>Port of lib/url.ts — parse a socket.io URL into host/path/query/source/secure.</summary>
public sealed class Url
{
    public string Source { get; private set; } = "";
    public string Host { get; private set; } = "";
    public string Path { get; private set; } = "/socket.io";
    public bool Secure { get; private set; }
    public Dictionary<string, string> Query { get; private set; } = new();

    public static Url Parse(string uri)
    {
        var result = new Url { Source = uri };
        if (string.IsNullOrEmpty(uri)) return result;

        // protocol
        var parsed = uri;
        bool secure = false;
        if (parsed.StartsWith("https://") || parsed.StartsWith("wss://"))
        {
            secure = true;
            parsed = parsed.Substring(parsed.IndexOf("://") + 3);
        }
        else if (parsed.StartsWith("http://") || parsed.StartsWith("ws://"))
        {
            parsed = parsed.Substring(parsed.IndexOf("://") + 3);
        }
        result.Secure = secure;

        // host (up to first / or ?)
        int pathStart = parsed.IndexOfAny(new[] { '/', '?' });
        string host;
        if (pathStart < 0) { host = parsed; parsed = ""; }
        else { host = parsed.Substring(0, pathStart); parsed = parsed.Substring(pathStart); }
        result.Host = host;

        // path + query
        int q = parsed.IndexOf('?');
        if (q < 0)
        {
            if (parsed.Length > 0) result.Path = parsed;
        }
        else
        {
            if (q > 0) result.Path = parsed.Substring(0, q);
            var queryStr = parsed.Substring(q + 1);
            foreach (var pair in queryStr.Split('&'))
            {
                var eq = pair.IndexOf('=');
                if (eq < 0) result.Query[System.Uri.UnescapeDataString(pair)] = "";
                else result.Query[System.Uri.UnescapeDataString(pair.Substring(0, eq))] =
                    System.Uri.UnescapeDataString(pair.Substring(eq + 1));
            }
        }
        return result;
    }
}
