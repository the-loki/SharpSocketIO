using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace SharpSocketIO.EngineIo.Client.Contrib;

/// <summary>
/// Port of contrib/parseuri.ts. Parses ws/wss/http/https URIs (and bare hosts)
/// into components mirroring the JS parse() output.
/// </summary>
public sealed class EngineIoUri
{
    private static readonly Regex Re = new Regex(
        @"^(?:(?![^:@\/?#]+:[^:@\/]*@)(http|https|ws|wss):\/\/)?((?:(([^:@\/?#]*)(?::([^:@\/?#]*))?)?@)?((?:[a-f0-9]{0,4}:){2,7}[a-f0-9]{0,4}|[^:\/?#]*)(?::(\d*))?)(((\/(?:[^?#](?![^?#\/]*\.[^?#\/.]+(?:[?#]|$)))*\/?)?([^?#\/]*))(?:\?([^#]*))?(?:#(.*))?)",
        RegexOptions.IgnoreCase);

    public string Source { get; private set; } = "";
    public string Protocol { get; private set; } = "";
    public string Authority { get; private set; } = "";
    public string UserInfo { get; private set; } = "";
    public string User { get; private set; } = "";
    public string Password { get; private set; } = "";
    public string Host { get; private set; } = "";
    public string Port { get; private set; } = "";
    public string Relative { get; private set; } = "";
    public string Path { get; private set; } = "";
    public string Directory { get; private set; } = "";
    public string File { get; private set; } = "";
    public string Query { get; private set; } = "";
    public string Anchor { get; private set; } = "";
    public bool Ipv6Uri { get; private set; }
    public IReadOnlyList<string> PathNames { get; private set; } = Array.Empty<string>();
    public IReadOnlyDictionary<string, string> QueryKey { get; private set; } = new Dictionary<string, string>();

    public bool Secure => Protocol == "https" || Protocol == "wss";

    public static EngineIoUri Parse(string str)
    {
        if (str.Length > 8000) throw new ArgumentException("URI too long");

        var src = str;
        int b = str.IndexOf('['), e = str.IndexOf(']');
        if (b != -1 && e != -1)
        {
            str = str.Substring(0, b) + str.Substring(b, e - b).Replace(":", ";") + str.Substring(e);
        }

        var m = Re.Match(str ?? "");
        string[] parts = { "source", "protocol", "authority", "userInfo", "user", "password", "host", "port", "relative", "path", "directory", "file", "query", "anchor" };
        var values = new Dictionary<string, string>();
        for (int i = parts.Length - 1; i >= 0; i--)
        {
            values[parts[i]] = i < m.Groups.Count ? m.Groups[i].Value : "";
        }

        var uri = new EngineIoUri
        {
            Source = src,
            Protocol = values["protocol"],
            Authority = values["authority"],
            UserInfo = values["userInfo"],
            User = values["user"],
            Password = values["password"],
            Host = values["host"],
            Port = values["port"],
            Relative = values["relative"],
            Path = values["path"],
            Directory = values["directory"],
            File = values["file"],
            Query = values["query"],
            Anchor = values["anchor"],
        };

        if (b != -1 && e != -1)
        {
            uri.Source = src;
            uri.Host = uri.Host.Substring(1, uri.Host.Length - 2).Replace(";", ":");
            uri.Authority = uri.Authority.Replace("[", "").Replace("]", "").Replace(";", ":");
            uri.Ipv6Uri = true;
        }

        uri.PathNames = ComputePathNames(uri.Path);
        uri.QueryKey = ComputeQueryKey(uri.Query);
        return uri;
    }

    private static IReadOnlyList<string> ComputePathNames(string path)
    {
        var regx = new Regex("/{2,9}");
        var names = new List<string>(regx.Replace(path, "/").Split('/'));
        if (path.Length > 0 && path[0] == '/' || path.Length == 0) names.RemoveAt(0);
        if (path.Length > 0 && path[path.Length - 1] == '/') names.RemoveAt(names.Count - 1);
        return names;
    }

    private static IReadOnlyDictionary<string, string> ComputeQueryKey(string query)
    {
        var data = new Dictionary<string, string>();
        var regx = new Regex(@"(?:^|&)([^&=]*)=?([^&]*)");
        foreach (Match m in regx.Matches(query))
        {
            if (!string.IsNullOrEmpty(m.Groups[1].Value)) data[m.Groups[1].Value] = m.Groups[2].Value;
        }
        return data;
    }
}
