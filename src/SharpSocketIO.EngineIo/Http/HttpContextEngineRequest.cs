using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.AspNetCore.Http;
using SharpSocketIO.EngineIo.Commons;

namespace SharpSocketIO.EngineIo.Http;

/// <summary>
/// Adapts an ASP.NET Core HttpContext to the IEngineRequest abstraction the
/// engine.io logic core consumes. Body is read lazily on first access (POST).
/// </summary>
public sealed class HttpContextEngineRequest : IEngineRequest
{
    private readonly HttpContext _ctx;
    private byte[]? _body;

    public HttpContextEngineRequest(HttpContext ctx) { _ctx = ctx; }

    public string Method => _ctx.Request.Method;
    public string Path => _ctx.Request.Path.Value ?? "/";

    public IReadOnlyDictionary<string, string> Query
    {
        get
        {
            var d = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var kv in _ctx.Request.Query) d[kv.Key] = kv.Value.ToString();
            return d;
        }
    }

    public IReadOnlyDictionary<string, string> Headers
    {
        get
        {
            var d = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var h in _ctx.Request.Headers) d[h.Key] = h.Value.ToString();
            return d;
        }
    }

    public string? RemoteAddress => _ctx.Connection.RemoteIpAddress?.ToString();

    public byte[]? Body
    {
        get
        {
            if (_body == null)
            {
                using var ms = new MemoryStream();
                _ctx.Request.Body.CopyTo(ms);
                _body = ms.ToArray();
            }
            return _body;
        }
    }

    public HttpContext HttpContext => _ctx;
}
