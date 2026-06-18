using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using SharpSocketIO.ComponentEmitter;
using SharpSocketIO.EngineIo.Commons;
using SharpSocketIO.EngineIo.Contrib;
using SharpSocketIO.EngineIo.Http;
using SharpSocketIO.EngineIo.Transports;

namespace SharpSocketIO.EngineIo;

/// <summary>
/// Port of lib/server.ts (3A verification + handshake; 3B adds live HTTP request
/// handling, client registry, cookie writing). WebSocket upgrade is 3C.
/// </summary>
public sealed class Server : Emitter<UnitEvents>
{
    public ServerOptions Options { get; } = new();
    public ConcurrentDictionary<string, Socket> Clients { get; } = new();
    public int ClientsCount { get; private set; }

    public (int? errorCode, string? message) Verify(IEngineRequest req)
    {
        if (!req.Query.TryGetValue("transport", out var transport) || string.IsNullOrEmpty(transport))
            return (ErrorCodes.TransportUnknown, ErrorCodes.Message(ErrorCodes.TransportUnknown));
        if (!Options.Transports.Contains(transport))
            return (ErrorCodes.TransportUnknown, ErrorCodes.Message(ErrorCodes.TransportUnknown));
        if (req.Method != "GET")
            return (ErrorCodes.BadHandshakeMethod, ErrorCodes.Message(ErrorCodes.BadHandshakeMethod));
        if (Options.AllowRequest is { } allow)
        {
            var (err, success) = allow(req);
            if (!success) return (err ?? ErrorCodes.Forbidden, ErrorCodes.Message(err ?? ErrorCodes.Forbidden));
        }
        return (null, null);
    }

    public string BuildHandshakeData(string sid) => BuildHandshakeData(sid, "polling");

    /// <summary>
    /// Builds the open packet's JSON data. Upgrades advertised depend on the client's
    /// current transport: a polling client may upgrade to websocket; a websocket client
    /// has no further upgrades. Mirrors JS server.upgrades(transport).
    /// </summary>
    public string BuildHandshakeData(string sid, string clientTransport)
    {
        string upgrades;
        if (!Options.AllowUpgrades)
        {
            upgrades = "[]";
        }
        else if (clientTransport == "polling" && Options.Transports.Contains("websocket"))
        {
            upgrades = "[\"websocket\"]";
        }
        else
        {
            upgrades = "[]";
        }
        // Advertise the actual HTTP body size limit the server enforces (mirrors upstream: maxHttpBufferSize)
        long maxPayload = Options.MaxPayload ?? Options.MaxHttpBufferSize;
        return "{\"sid\":\"" + sid + "\",\"upgrades\":" + upgrades +
               ",\"pingInterval\":" + Options.PingInterval +
               ",\"pingTimeout\":" + Options.PingTimeout +
               ",\"maxPayload\":" + maxPayload + "}";
    }

    public string GenerateId() => Options.GenerateId?.Invoke() ?? Base64Id.GenerateId();

    public Socket RegisterSocket(string id, Socket socket)
    {
        Clients[id] = socket;
        ClientsCount++;
        socket.On("close", _ =>
        {
            ((IDictionary<string, Socket>)Clients).Remove(id);
            ClientsCount--;
        });
        Emit("connection", socket);
        return socket;
    }

    /// <summary>Builds the Set-Cookie value for the handshake, or null if cookies disabled.</summary>
    public string? BuildCookieValue(string sid)
    {
        var c = Options.CookieConfig;
        if (c == null) return null;
        var name = string.IsNullOrEmpty(c.Name) ? "io" : c.Name!;
        var opts = new Commons.CookieOptions
        {
            Path = c.Path ?? "/",
            HttpOnly = c.HttpOnly ?? true,
            SameSite = c.SameSite,
        };
        return CookieSerializer.Serialize(name, sid, opts);
    }

    /// <summary>Handles a handshake (no sid) request.</summary>
    public async Task HandleHandshakeAsync(HttpContext ctx, HttpContextEngineRequest req)
    {
        var (code, message) = Verify(req);
        if (code.HasValue)
        {
            await WriteErrorAsync(ctx, code.Value, message!);
            Emit("connection_error", new { req, code = code.Value, message });
            return;
        }
        if (req.Query.TryGetValue("EIO", out var eio) && eio != "4" && !Options.AllowEIO3)
        {
            await WriteErrorAsync(ctx, ErrorCodes.BadRequest, "Unsupported protocol version");
            return;
        }
        string id = GenerateId();
        var cookie = BuildCookieValue(id);
        if (cookie != null) ctx.Response.Headers["Set-Cookie"] = cookie;

        var hasJsonp = req.Query.TryGetValue("j", out var _) && !string.IsNullOrEmpty(req.Query["j"]);
        var transport = hasJsonp
            ? new JsonpPolling(req, Options.MaxHttpBufferSize, Options.HttpCompression)
            : new PollingHttp(req, Options.MaxHttpBufferSize, Options.HttpCompression);
        var socket = new Socket(id, this, transport, protocol: 4);
        RegisterSocket(id, socket);

        // open packet is enqueued by Socket.OnOpen; respond to the GET with it.
        socket.OnOpen();
        if (ctx.Request.Method == "GET")
        {
            await transport.HandleGetAsync();
        }
    }

    /// <summary>Handles a request carrying an existing sid (subsequent polls / data POSTs).</summary>
    public async Task HandlePollingAsync(HttpContext ctx, HttpContextEngineRequest req, string sid)
    {
        if (!Clients.TryGetValue(sid, out var socket))
        {
            await WriteErrorAsync(ctx, ErrorCodes.TransportUnknown, "Session ID unknown");
            return;
        }
        if (socket.Transport is not PollingHttp transport)
        {
            await WriteErrorAsync(ctx, ErrorCodes.BadRequest, "Transport mismatch");
            return;
        }
        transport.BindRequest(req);
        if (ctx.Request.Method == "POST") await transport.HandlePostAsync();
        else await transport.HandleGetAsync();
    }

    private static async Task WriteErrorAsync(HttpContext ctx, int code, string message)
    {
        ctx.Response.StatusCode = 400;
        ctx.Response.ContentType = "application/json";
        var body = "{\"code\":" + code + ",\"message\":\"" + message + "\"}";
        await ctx.Response.WriteAsync(body);
    }

    /// <summary>Handles a WebSocket upgrade request: upgrade an existing polling session, or ws-only handshake.</summary>
    public async Task HandleWebSocketUpgradeAsync(HttpContext ctx, HttpContextEngineRequest req, string? sid)
    {
        if (!ctx.WebSockets.IsWebSocketRequest)
        {
            ctx.Response.StatusCode = 400;
            return;
        }
        var ws = await ctx.WebSockets.AcceptWebSocketAsync();

        if (!string.IsNullOrEmpty(sid) && Clients.TryGetValue(sid, out var existing))
        {
            // Reject concurrent upgrade attempts (upstream: if client.upgrading || client.upgraded → close ws)
            if (existing.Upgraded || existing.IsUpgrading)
            {
                try { await ws.CloseAsync(System.Net.WebSockets.WebSocketCloseStatus.NormalClosure, "already upgraded", System.Threading.CancellationToken.None); } catch { }
                return;
            }
            // upgrade existing polling session
            var upgradeTransport = new WebSocketTransport(req, ws, Options.MaxHttpBufferSize);
            // force a polling cycle (send noop) for a fast probe
            if (existing.Transport is PollingHttp polling)
            {
                polling.EnqueueAndFlush(new[]
                {
                    new SharpSocketIO.EngineIo.Parser.Commons.Packet(
                        SharpSocketIO.EngineIo.Parser.Commons.PacketType.Noop)
                });
            }
            existing.MaybeUpgrade(upgradeTransport, Options.UpgradeTimeout);
            await upgradeTransport.RunReceiveLoopAsync();
            return;
        }

        if (!string.IsNullOrEmpty(sid))
        {
            ctx.Response.StatusCode = 400;
            return;
        }

        // ws-only fresh handshake
        var (code, message) = Verify(req);
        if (code.HasValue)
        {
            try { await ws.CloseAsync(System.Net.WebSockets.WebSocketCloseStatus.NormalClosure, message, System.Threading.CancellationToken.None); } catch { }
            return;
        }
        if (req.Query.TryGetValue("EIO", out var eio) && eio != "4" && !Options.AllowEIO3)
        {
            try { await ws.CloseAsync(System.Net.WebSockets.WebSocketCloseStatus.NormalClosure, "Unsupported protocol version", System.Threading.CancellationToken.None); } catch { }
            return;
        }
        string id = GenerateId();
        var cookie = BuildCookieValue(id);
        if (cookie != null) ctx.Response.Headers["Set-Cookie"] = cookie;

        var transport = new WebSocketTransport(req, ws, Options.MaxHttpBufferSize);
        var socket = new Socket(id, this, transport, protocol: 4);
        RegisterSocket(id, socket);
        await socket.OnOpenAsync();
        await transport.RunReceiveLoopAsync();
    }
}
