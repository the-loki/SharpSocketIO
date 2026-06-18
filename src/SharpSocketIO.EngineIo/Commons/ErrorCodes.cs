namespace SharpSocketIO.EngineIo.Commons;

/// <summary>Port of Server.errors — connection_error codes.</summary>
public static class ErrorCodes
{
    public const int TransportUnknown = 0;
    public const int Forbidden = 1;
    public const int BadRequest = 2;
    public const int BadHandshakeMethod = 3;

    public static string Message(int code) => code switch
    {
        TransportUnknown => "Transport unknown",
        Forbidden => "Forbidden",
        BadRequest => "Bad request",
        BadHandshakeMethod => "Bad handshake method",
        _ => "Unknown error",
    };
}
