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
    public CookieOptions? Cookie { get; set; }
    public bool AllowEIO3 { get; set; } = false;
    public int? MaxPayload { get; set; }
}

/// <summary>Minimal request abstraction the server/transport logic sees (3A).</summary>
public interface IEngineRequest
{
    string Method { get; }
    string Path { get; }
    IReadOnlyDictionary<string, string> Query { get; }
    IReadOnlyDictionary<string, string> Headers { get; }
    string? RemoteAddress { get; }
    byte[]? Body { get; }
}
