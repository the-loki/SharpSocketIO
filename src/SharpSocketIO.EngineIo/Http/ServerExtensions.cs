using System;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using SharpSocketIO.EngineIo.Commons;

namespace SharpSocketIO.EngineIo.Http;

/// <summary>
/// ASP.NET Core integration for engine.io. Attach mounts a middleware on an
/// IApplicationBuilder (WebApplication satisfies this) that serves the engine.io
/// polling surface at the configured path. Port of lib/engine.ts attach().
/// </summary>
public static class ServerExtensions
{
    public static Server Attach(this Server engine, IApplicationBuilder app, AttachOptions? opts = null)
    {
        opts ??= new AttachOptions();
        string matchPath = opts.Path;
        bool addTrailing = opts.AddTrailingSlash;

        app.Use(async (ctx, next) =>
        {
            var path = ctx.Request.Path.Value ?? "/";
            bool matches = path == matchPath
                           || path == matchPath + "/"
                           || (addTrailing && path == matchPath + "/");
            if (!matches)
            {
                await next();
                return;
            }

            ApplyCors(ctx, engine.Options.CorsOrigin);

            if (ctx.Request.Method == "OPTIONS")
            {
                ctx.Response.StatusCode = 204;
                return;
            }

            var req = new HttpContextEngineRequest(ctx);

            if (req.Query.TryGetValue("sid", out var sid) && !string.IsNullOrEmpty(sid))
            {
                await engine.HandlePollingAsync(ctx, req, sid);
                return;
            }

            await engine.HandleHandshakeAsync(ctx, req);
        });

        return engine;
    }

    private static void ApplyCors(HttpContext ctx, string origin)
    {
        if (string.IsNullOrEmpty(origin)) return;
        ctx.Response.Headers["Access-Control-Allow-Origin"] = origin;
        ctx.Response.Headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS";
        ctx.Response.Headers["Access-Control-Allow-Headers"] = "Content-Type";
        if (origin != "*") ctx.Response.Headers["Access-Control-Allow-Credentials"] = "true";
    }
}
