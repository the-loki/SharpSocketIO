using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using SharpSocketIO.EngineIo.Commons;

namespace SharpSocketIO.EngineIo.Http;

/// <summary>
/// Adapts an ASP.NET Core HttpContext to the IEngineRequest abstraction the
/// engine.io logic core consumes. Body is read lazily; use ReadBodyAsync first
/// for POST (TestHost disallows synchronous IO by default).
/// </summary>
public sealed class HttpContextEngineRequest : IEngineRequest
{
    private readonly HttpContext _ctx;
    private byte[]? _body;
    private bool _bodyRead;

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

    public byte[]? Body => _body;

    /// <summary>Reads the request body asynchronously and caches it. Call before accessing Body on POST.</summary>
    public async Task ReadBodyAsync()
    {
        if (_bodyRead) return;
        _ctx.Request.EnableBuffering();
        using var ms = new MemoryStream();
        await _ctx.Request.Body.CopyToAsync(ms);
        _body = ms.ToArray();
        _bodyRead = true;
        try { _ctx.Request.Body.Position = 0; } catch { }
    }

    public HttpContext HttpContext => _ctx;
}
