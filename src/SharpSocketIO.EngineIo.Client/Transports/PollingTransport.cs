using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SharpSocketIO.EngineIo.Client.Contrib;
using SharpSocketIO.EngineIo.Parser;
using SharpSocketIO.EngineIo.Parser.Commons;
using EioParser = SharpSocketIO.EngineIo.Parser;

namespace SharpSocketIO.EngineIo.Client.Transports;

/// <summary>
/// Port of lib/transports/polling.ts + polling-xhr.ts (HttpClient-based). GET = long-poll
/// (returns queued payloads), POST = send. Payloads are \x1e-joined; binary base64-forced.
/// </summary>
public sealed class PollingTransport : Transport
{
    private const char Sep = (char)30;
    private readonly HttpClient _http;
    private readonly HttpClient _postHttp;
    private readonly string _baseUrl;
    private readonly CancellationTokenSource _cts = new();
    private Task? _pollLoopTask;
    private bool _polling;
    private int _pollIndex;

    public PollingTransport(SocketOptions opts) : base(opts)
    {
        var scheme = opts.Secure ? "https" : "http";
        var host = opts.Hostname ?? "localhost";
        string port = "";
        if (!string.IsNullOrEmpty(opts.Port) &&
            !((opts.Secure && opts.Port == "443") || (!opts.Secure && opts.Port == "80")))
        {
            port = ":" + opts.Port;
        }
        _baseUrl = $"{scheme}://{host}{port}{opts.Path}";
        var handler = new HttpClientHandler();
        if (opts.WithCredentials) handler.UseCookies = true;
        _http = new HttpClient(handler) { Timeout = Timeout.InfiniteTimeSpan };
        _http.DefaultRequestVersion = new Version(1, 1);
        _http.DefaultVersionPolicy = HttpVersionPolicy.RequestVersionOrLower;
        // Separate client for POST: the long-poll GET holds a connection open, so POSTs
        // need their own connection pool to avoid head-of-line blocking.
        var postHandler = new HttpClientHandler();
        if (opts.WithCredentials) postHandler.UseCookies = true;
        _postHttp = new HttpClient(postHandler);
    }

    public override string Name => "polling";

    public HttpClient HttpClient => _http;

    public int WriteCallCount { get; private set; }
    public int PostSuccessCount { get; private set; }
    public string? LastPostError { get; private set; }

    protected override void DoOpen()
    {
        _polling = true;
        ReadyState = TransportState.Opening;
        // Open fires once the first poll succeeds (mirrors JS: the polling GET itself opens the transport).
        _ = Task.Run(async () =>
        {
            try
            {
                await PollOnceAsync(first: true);
                _pollLoopTask = PollLoopAsync();
            }
            catch (Exception ex)
            {
                OnError("poll error", ex.Message);
                OnClose(new CloseDetails { Description = "poll error", Context = ex.Message });
            }
        });
    }

    protected override void DoClose()
    {
        _polling = false;
        _cts.Cancel();
        _ = PostPayloadAsync(EncodePackets(new[] { new Packet(PacketType.Close) }));
    }

    protected override void Write(IReadOnlyList<Packet> packets)
    {
        WriteCallCount++;
        var payload = EncodePackets(packets);
        LastEncodedPayload = payload;
        _ = PostPayloadAsync(payload);
    }

    public string? LastEncodedPayload { get; private set; }

    private string EncodePackets(IReadOnlyList<Packet> packets)
    {
        var encoded = new string[packets.Count];
        for (int i = 0; i < packets.Count; i++)
        {
            string captured = string.Empty;
            EioParser.EncodePacket.Encode(packets[i], false, r => captured = r.AsString()!);
            encoded[i] = captured;
        }
        return string.Join(Sep.ToString(), encoded);
    }

    private async Task PostPayloadAsync(string payload)
    {
        try
        {
            var uri = BuildUri();
            using var content = new StringContent(payload, Encoding.UTF8, "text/plain");
            using var resp = await _postHttp.PostAsync(uri, content, _cts.Token);
            LastPostResponseStatus = (int)resp.StatusCode;
            LastPostResponseBody = await resp.Content.ReadAsStringAsync();
            PostSuccessCount++;
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            LastPostError = ex.Message;
            OnError("xhr post error", ex.Message);
        }
    }

    public int LastPostResponseStatus { get; private set; }
    public string? LastPostResponseBody { get; private set; }

    private async Task PollOnceAsync(bool first)
    {
        var uri = BuildUri();
        using var resp = await _http.GetAsync(uri, HttpCompletionOption.ResponseContentRead, _cts.Token);
        var body = await resp.Content.ReadAsStringAsync();
        LastPollBody = body;
        // On the first poll, mark the transport open BEFORE delivering packets, so any
        // synchronous send from the socket's "open" handler is accepted (not discarded).
        if (first && ReadyState != TransportState.Open) OnOpen();
        if (!string.IsNullOrEmpty(body))
        {
            foreach (var part in body.Split(Sep)) OnData(new RawData(part));
        }
        EmitReserved("pollComplete");
    }

    public string? LastPollBody { get; private set; }

    public int PollLoopIterations { get; private set; }

    private async Task PollLoopAsync()
    {
        while (_polling && !_cts.IsCancellationRequested)
        {
            try
            {
                PollLoopIterations++;
                await PollOnceAsync(first: false);
            }
            catch (OperationCanceledException) { return; }
            catch (Exception ex)
            {
                OnError("xhr poll error", ex.Message);
                OnClose(new CloseDetails { Description = "poll error", Context = ex.Message });
                return;
            }
        }
    }

    private string BuildUri()
    {
        var query = new Dictionary<string, string>(Query)
        {
            ["EIO"] = "4",
            ["transport"] = "polling",
        };
        if (Opts.TimestampRequests) query[Opts.TimestampParam] = (_pollIndex++).ToString();
        return _baseUrl + "?" + Parseqs.Encode(query);
    }

    public override void Pause(Action onPause) => onPause();
}
