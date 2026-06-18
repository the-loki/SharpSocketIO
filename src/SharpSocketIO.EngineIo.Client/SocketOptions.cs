using System.Collections.Generic;

namespace SharpSocketIO.EngineIo.Client;

/// <summary>Port of SocketOptions from lib/socket.ts (key fields).</summary>
public sealed class SocketOptions
{
    public string? Host { get; set; }
    public string? Hostname { get; set; }
    public bool Secure { get; set; }
    public string? Port { get; set; }
    public Dictionary<string, string> Query { get; set; } = new();
    public bool Upgrade { get; set; } = true;
    public bool ForceBase64 { get; set; }
    public string TimestampParam { get; set; } = "t";
    public bool TimestampRequests { get; set; }
    public IReadOnlyList<string> Transports { get; set; } = new[] { "polling", "websocket" };
    public string Path { get; set; } = "/engine.io";
    public bool RememberUpgrade { get; set; }
    public bool Reconnection { get; set; } = true;
    public int ReconnectionAttempts { get; set; } = int.MaxValue;
    public int ReconnectionDelay { get; set; } = 1000;
    public int ReconnectionDelayMax { get; set; } = 5000;
    public bool WithCredentials { get; set; }
    public IReadOnlyDictionary<string, string> ExtraHeaders { get; set; } = new Dictionary<string, string>();
    public int Timeout { get; set; } = 20000;
}
