namespace SharpSocketIO.SocketIo.Parser.Commons;

/// <summary>Port of RESERVED_EVENTS — event names with special meaning.</summary>
public static class ReservedEvents
{
    public static readonly string[] All =
    {
        "connect",
        "connect_error",
        "disconnect",
        "disconnecting",
        "newListener",
        "removeListener",
    };

    public static bool Contains(string name) => System.Array.IndexOf(All, name) >= 0;
}
