using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using SharpSocketIO.EngineIo.Commons;
using SharpSocketIO.EngineIo.Http;
using SharpSocketIO.EngineIo.Parser.Commons;
using EioParser = SharpSocketIO.EngineIo.Parser;

namespace SharpSocketIO.EngineIo.Transports;

/// <summary>
/// HTTP-aware Polling transport (3B). GET holds the response open via a TCS until
/// packets are flushed; POST reads the body and emits packets. Compression + JSONP
/// wrapping handled by subclasses / collaborators. Mirrors lib/transports/polling.ts.
/// </summary>
public class PollingHttp : Polling
{
    public const int DefaultCompressionThreshold = 1024;

    private HttpContext _ctx;
    private HttpContextEngineRequest? _boundReq;
    private readonly long _maxHttpBufferSize;
    private readonly bool _httpCompression;
    private readonly int _compressionThreshold;
    private TaskCompletionSource<string>? _pollWait;
    private bool _responseWritten;

    public PollingHttp(HttpContextEngineRequest req, long maxHttpBufferSize, bool httpCompression)
        : base(req)
    {
        _ctx = req.HttpContext;
        _boundReq = req;
        _maxHttpBufferSize = maxHttpBufferSize == 0 ? 1_000_000 : maxHttpBufferSize;
        _httpCompression = httpCompression;
        _compressionThreshold = DefaultCompressionThreshold;
    }

    public HttpContext HttpContext => _ctx;

    /// <summary>Rebinds the current HTTP context on each new request (polling serves many requests per session).</summary>
    public void BindRequest(HttpContextEngineRequest req)
    {
        _ctx = req.HttpContext;
        _boundReq = req;
    }

    /// <summary>Handle a polling GET. If outbound buffer non-empty, flush; else hold until FlushToWaiter.</summary>
    public async Task HandleGetAsync()
    {
        var immediate = FlushToString();
        if (!string.IsNullOrEmpty(immediate))
        {
            await WritePayloadAsync(immediate);
            return;
        }
        _pollWait = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var payload = await _pollWait.Task;
        await WritePayloadAsync(payload);
    }

    /// <summary>Flush queued packets; if a poll is waiting, complete it. Returns true if delivered.</summary>
    public bool FlushToWaiter()
    {
        var flushed = FlushToString();
        if (string.IsNullOrEmpty(flushed)) return false;
        if (_pollWait != null)
        {
            _pollWait.TrySetResult(flushed);
            _pollWait = null;
            return true;
        }
        return false;
    }

    /// <summary>POST data ingest: read body, enforce maxHttpBufferSize, decode, emit packets, reply ok.</summary>
    public async Task HandlePostAsync()
    {
        if (_boundReq == null) throw new InvalidOperationException("no bound request");
        await _boundReq.ReadBodyAsync();
        var bodyBytes = _boundReq.Body ?? Array.Empty<byte>();
        if (bodyBytes.Length > _maxHttpBufferSize)
        {
            _ctx.Response.StatusCode = 413;
            return;
        }
        OnDataRequest(_boundReq);
        _ctx.Response.StatusCode = 200;
        _ctx.Response.ContentType = "text/html";
        _ctx.Response.ContentLength = 2;
        await _ctx.Response.WriteAsync("ok");
    }

    public void EnqueueAndFlush(IReadOnlyList<Packet> packets)
    {
        foreach (var p in packets) Enqueue(p);
        FlushToWaiter();
    }

    protected virtual async Task WritePayloadAsync(string payload)
    {
        var bytes = Encoding.UTF8.GetBytes(payload);
        string? encoding = null;
        if (_httpCompression && bytes.Length >= _compressionThreshold)
        {
            encoding = NegotiateEncoding(_ctx.Request.Headers["Accept-Encoding"].ToString());
            if (encoding != null) bytes = Compress(bytes, encoding);
        }

        _ctx.Response.StatusCode = 200;
        _ctx.Response.ContentType = ResponseContentType;
        if (encoding != null) _ctx.Response.Headers["Content-Encoding"] = encoding;
        _ctx.Response.ContentLength = bytes.Length;
        await _ctx.Response.Body.WriteAsync(bytes);
        _responseWritten = true;
    }

    protected virtual string ResponseContentType => "text/plain; charset=UTF-8";

    public bool ResponseWritten => _responseWritten;

    private static string? NegotiateEncoding(string acceptEncoding)
    {
        if (string.IsNullOrEmpty(acceptEncoding)) return null;
        if (acceptEncoding.Contains("gzip", StringComparison.OrdinalIgnoreCase)) return "gzip";
        if (acceptEncoding.Contains("deflate", StringComparison.OrdinalIgnoreCase)) return "deflate";
        return null;
    }

    private static byte[] Compress(byte[] data, string encoding)
    {
        using var ms = new MemoryStream();
        if (encoding == "gzip")
        {
            using var gz = new System.IO.Compression.GZipStream(ms, System.IO.Compression.CompressionLevel.Optimal, leaveOpen: true);
            gz.Write(data, 0, data.Length);
        }
        else
        {
            using var df = new System.IO.Compression.DeflateStream(ms, System.IO.Compression.CompressionLevel.Optimal, leaveOpen: true);
            df.Write(data, 0, data.Length);
        }
        return ms.ToArray();
    }
}
