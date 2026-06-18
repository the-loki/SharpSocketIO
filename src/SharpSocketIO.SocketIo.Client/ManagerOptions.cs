using System.Collections.Generic;

namespace SharpSocketIO.SocketIo.Client;

/// <summary>Port of ManagerOptions.</summary>
public sealed class ManagerOptions
{
    public bool ForceNew { get; set; }
    public bool Multiplex { get; set; } = true;
    public string Path { get; set; } = "/socket.io";
    public bool Reconnection { get; set; } = true;
    public int ReconnectionAttempts { get; set; } = int.MaxValue;
    public int ReconnectionDelay { get; set; } = 1000;
    public int ReconnectionDelayMax { get; set; } = 5000;
    public double RandomizationFactor { get; set; } = 0.5;
    public int Timeout { get; set; } = 20000;
    public bool AutoConnect { get; set; } = true;
    public IReadOnlyList<string> Transports { get; set; } = new[] { "polling", "websocket" };
}
