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

    /// <summary>
    /// Override of Send: enqueue then, if a poll GET is currently waiting, wake it up so the
    /// queued packets are delivered on the in-flight long-poll response immediately.
    /// </summary>
    public override void Send(IReadOnlyList<Packet> packets)
    {
        foreach (var p in packets) Enqueue(p);
        FlushToWaiter();
    }

    /// <summary>Handle a polling GET. If outbound buffer non-empty, flush; else hold until FlushToWaiter.</summary>
    public async Task HandleGetAsync()
    {
        // Capture THIS request's context: the long-poll may be completed later by a POST
        // arriving on a different HttpContext, and we must write the payload to THIS response.
        var getCtx = _ctx;
        var immediate = FlushToString();
        if (!string.IsNullOrEmpty(immediate))
        {
            await WritePayloadAsync(getCtx, immediate);
            return;
        }
        _pollWait = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var payload = await _pollWait.Task;
        await WritePayloadAsync(getCtx, payload);
    }

    /// <summary>Flush queued packets; if a poll is waiting, complete it. Returns true if delivered.</summary>
    public bool FlushToWaiter()
    {
        if (_pollWait == null) return false; // don't drain the buffer if nobody is waiting
        var flushed = FlushToString();
        if (string.IsNullOrEmpty(flushed)) return false;
        _pollWait.TrySetResult(flushed);
        _pollWait = null;
        return true;
    }

    /// <summary>POST data ingest: read body, enforce maxHttpBufferSize, decode, emit packets, reply ok.</summary>
    public async Task HandlePostAsync()
    {
        if (_boundReq == null) throw new InvalidOperationException("no bound request");
        await _boundReq.ReadBodyAsync();
        var bodyBytes = _boundReq.Body ?? Array.Empty<byte>();
        LastPostBodyLength = bodyBytes.Length;
        LastRequestContentLength = (long?)_ctx.Request.ContentLength ?? -1;
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

    public int LastPostBodyLength { get; private set; } = -1;
    public long LastRequestContentLength { get; private set; } = -1;

    public void EnqueueAndFlush(IReadOnlyList<Packet> packets)
    {
        foreach (var p in packets) Enqueue(p);
        FlushToWaiter();
    }

    protected virtual async Task WritePayloadAsync(HttpContext ctx, string payload)
    {
        var bytes = Encoding.UTF8.GetBytes(payload);
        string? encoding = null;
        if (_httpCompression && bytes.Length >= _compressionThreshold)
        {
            encoding = NegotiateEncoding(ctx.Request.Headers["Accept-Encoding"].ToString());
            if (encoding != null) bytes = Compress(bytes, encoding);
        }

        ctx.Response.StatusCode = 200;
        ctx.Response.ContentType = ResponseContentType;
        if (encoding != null) ctx.Response.Headers["Content-Encoding"] = encoding;
        ctx.Response.ContentLength = bytes.Length;
        await ctx.Response.Body.WriteAsync(bytes);
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
