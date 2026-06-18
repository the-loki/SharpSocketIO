using SharpSocketIO.ComponentEmitter;
using SharpSocketIO.EngineIo.Commons;

namespace SharpSocketIO.EngineIo;

/// <summary>
/// Port of lib/server.ts (verification + handshake subset for 3A). Live HTTP attach,
/// client dictionary, CORS, cookies, and upgrade are 3B/3C.
/// </summary>
public sealed class Server : Emitter<UnitEvents>
{
    public ServerOptions Options { get; } = new();

    public (int? errorCode, string? message) Verify(IEngineRequest req)
    {
        if (!req.Query.TryGetValue("transport", out var transport) || string.IsNullOrEmpty(transport))
        {
            return (ErrorCodes.TransportUnknown, ErrorCodes.Message(ErrorCodes.TransportUnknown));
        }
        // guard against Object.prototype keys like "constructor"
        if (!Options.Transports.Contains(transport))
        {
            return (ErrorCodes.TransportUnknown, ErrorCodes.Message(ErrorCodes.TransportUnknown));
        }
        if (req.Method != "GET")
        {
            return (ErrorCodes.BadHandshakeMethod, ErrorCodes.Message(ErrorCodes.BadHandshakeMethod));
        }
        // AllowRequest hook
        if (Options.AllowRequest is { } allow)
        {
            var (err, success) = allow(req);
            if (!success) return (err ?? ErrorCodes.Forbidden, ErrorCodes.Message(err ?? ErrorCodes.Forbidden));
        }
        return (null, null);
    }

    public string BuildHandshakeData(string sid)
    {
        var upgrades = Options.AllowUpgrades && Options.Transports.Contains("websocket")
            ? "[\"websocket\"]" : "[]";
        int maxPayload = Options.MaxPayload ?? 1000000;
        return "{\"sid\":\"" + sid + "\",\"upgrades\":" + upgrades +
               ",\"pingInterval\":" + Options.PingInterval +
               ",\"pingTimeout\":" + Options.PingTimeout +
               ",\"maxPayload\":" + maxPayload + "}";
    }
}
