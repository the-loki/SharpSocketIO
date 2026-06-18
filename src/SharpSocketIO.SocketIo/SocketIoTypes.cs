using System.Collections.Generic;

namespace SharpSocketIO.SocketIo;

/// <summary>Handshake details sent to the client on connect.</summary>
public sealed class Handshake
{
    public string Id { get; set; } = "";
    public Dictionary<string, string> Headers { get; set; } = new();
    public string? RemoteAddress { get; set; }
    public string? Url { get; set; }
    public string? Origin { get; set; }
    public Dictionary<string, string> Query { get; set; } = new();
    public string Issued { get; set; } = "";
    public object? Auth { get; set; }
    public Dictionary<string, string> Time { get; set; } = new();
    public bool Secure { get; set; }
}

/// <summary>The reasons a socket disconnects (port of socket-types.ts DisconnectReason).</summary>
public static class DisconnectReasons
{
    public const string TransportError = "transport error";
    public const string TransportClose = "transport close";
    public const string ForcedClose = "forced close";
    public const string PingTimeout = "ping timeout";
    public const string ParseError = "parse error";
    public const string ClientNamespaceDisconnect = "client namespace disconnect";
    public const string ServerNamespaceDisconnect = "server namespace disconnect";
    public const string ServerShuttingDown = "server shutting down";
}

/// <summary>Subset of ServerOptions relevant to the logic layer.</summary>
public sealed class ServerOptions
{
    public string Path { get; set; } = "/socket.io";
    public bool ServeClient { get; set; } = true;
    public int PingTimeout { get; set; } = 20000;
    public int PingInterval { get; set; } = 25000;
    public int ConnectTimeout { get; set; } = 45000;
    public IReadOnlyList<string> Transports { get; set; } = new[] { "polling", "websocket" };
    public bool AllowUpgrades { get; set; } = true;
    public bool CleanupEmptyChildNamespaces { get; set; } = true;
}
