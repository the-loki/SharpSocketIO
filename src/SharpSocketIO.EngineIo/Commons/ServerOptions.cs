using System;
using System.Collections.Generic;

namespace SharpSocketIO.EngineIo.Commons;

public sealed class ServerOptions
{
    public int PingTimeout { get; set; } = 20000;
    public int PingInterval { get; set; } = 25000;
    public int UpgradeTimeout { get; set; } = 10000;
    public long MaxHttpBufferSize { get; set; } = 1_000_000;
    public Func<IEngineRequest, (int? errCode, bool success)>? AllowRequest { get; set; }
    public IReadOnlyList<string> Transports { get; set; } = new[] { "polling", "websocket" };
    public bool AllowUpgrades { get; set; } = true;
    public object? PerMessageDeflate { get; set; }
    public bool HttpCompression { get; set; } = true;
    public string CorsOrigin { get; set; } = "*";
    public Func<string>? GenerateId { get; set; }
    /// <summary>Cookie config: null (no cookie) or a config object.</summary>
    public CookieConfig? CookieConfig { get; set; }
    public bool AllowEIO3 { get; set; } = false;
    public int? MaxPayload { get; set; }
}

/// <summary>Cookie configuration (port of the 'cookie' option in engine.io).</summary>
public sealed class CookieConfig
{
    public string? Name { get; set; }
    public string? Path { get; set; }
    public bool? HttpOnly { get; set; }
    public string? SameSite { get; set; } // "lax"/"strict"/"none"
    public int? MaxAge { get; set; }
}

public interface IEngineRequest
{
    string Method { get; }
    string Path { get; }
    IReadOnlyDictionary<string, string> Query { get; }
    IReadOnlyDictionary<string, string> Headers { get; }
    string? RemoteAddress { get; }
    byte[]? Body { get; }
}
