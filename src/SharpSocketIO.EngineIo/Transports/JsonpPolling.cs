using System.Text.Json;
using System.Threading.Tasks;
using SharpSocketIO.EngineIo.Http;

namespace SharpSocketIO.EngineIo.Transports;

/// <summary>
/// Port of lib/transports/polling-jsonp.ts. Wraps polling GET responses as a JSONP
/// callback: ___eio[&lt;j&gt;](&lt;json-string&gt;); with Content-Type text/javascript.
/// POST ingest unwraps the `d` form field (handled at the middleware level for 3B).
/// </summary>
public sealed class JsonpPolling : PollingHttp
{
    private readonly string _head;
    private const string Foot = ");";

    public JsonpPolling(HttpContextEngineRequest req, long maxHttpBufferSize, bool httpCompression)
        : base(req, maxHttpBufferSize, httpCompression)
    {
        var j = req.Query.TryGetValue("j", out var jv) ? jv : "";
        foreach (var c in j) if (!char.IsDigit(c)) { j = ""; break; }
        _head = "___eio[" + j + "](";
    }

    protected override string ResponseContentType => "text/javascript; charset=UTF-8";

    protected override async Task WritePayloadAsync(Microsoft.AspNetCore.Http.HttpContext ctx, string payload)
    {
        // JSON-stringify the payload (escape U+2028/U+2029 which are invalid in JS string literals)
        var js = JsonSerializer.Serialize(payload).Replace("\u2028", "\\u2028").Replace("\u2029", "\\u2029");
        var wrapped = _head + js + Foot;
        await base.WritePayloadAsync(ctx, wrapped);
    }
}
