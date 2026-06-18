using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SharpSocketIO.EngineIo;
using SharpSocketIO.EngineIo.Commons;
using SharpSocketIO.EngineIo.Http;

namespace SharpSocketIO.EngineIo.Tests.Commons;

/// <summary>
/// HttpClient-driven test driver for engine.io HTTP round-trips. Spins up a TestServer
/// (in-process Kestrel) attached to an engine.io Server and exposes the polling
/// GET/POST surface. NOT the engine.io-client package — a minimal driver sufficient
/// to exercise the server's polling wire format.
/// </summary>
public sealed class TestDriver : IAsyncDisposable
{
    private readonly IHost _host;
    private readonly HttpClient _client;
    public Server Engine { get; }

    private TestDriver(IHost host, HttpClient client, Server engine, TestServer server)
    {
        _host = host; _client = client; Engine = engine; TestServer = server;
    }

    public static async Task<TestDriver> StartAsync(Action<ServerOptions>? configure = null)
    {
        var engine = new Server();
        configure?.Invoke(engine.Options);

        var builder = Host.CreateDefaultBuilder()
            .ConfigureWebHost(web =>
            {
                web.UseTestServer();
                web.Configure(app => engine.Attach(app));
            });
        var host = builder.Build();
        await host.StartAsync();

        var server = host.GetTestServer();
        var client = server.CreateClient();
        client.BaseAddress = new Uri("http://localhost/");
        return new TestDriver(host, client, engine, server);
    }

    /// <summary>Starts a real Kestrel server on an ephemeral port (for WebSocket tests).</summary>
    public static async Task<TestDriver> StartRealAsync(Action<ServerOptions>? configure = null)
    {
        var engine = new Server();
        configure?.Invoke(engine.Options);

        var builder = Host.CreateDefaultBuilder()
            .ConfigureWebHost(web =>
            {
                web.UseKestrel();
                web.UseUrls("http://127.0.0.1:0");
                web.Configure(app => engine.Attach(app));
            });
        var host = builder.Build();
        await host.StartAsync();

        var serverFeature = host.Services.GetRequiredService<Microsoft.AspNetCore.Hosting.Server.IServer>();
        var address = serverFeature.Features.Get<IServerAddressesFeature>()!.Addresses.First();
        var baseUri = new Uri(address);
        var client = new HttpClient { BaseAddress = baseUri };
        return new TestDriver(host, client, engine, server: null!);
    }

    public Uri RealBaseAddress => _client.BaseAddress!;

    public async Task<(int status, string body, IReadOnlyDictionary<string, string> headers)> GetAsync(
        string pathAndQuery, string? acceptEncoding = null)
    {
        using var req = new HttpRequestMessage(HttpMethod.Get, pathAndQuery);
        if (acceptEncoding != null) req.Headers.AcceptEncoding.ParseAdd(acceptEncoding);
        using var resp = await _client.SendAsync(req);
        var body = await resp.Content.ReadAsStringAsync();
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var h in resp.Headers) headers[h.Key] = string.Join(",", h.Value);
        foreach (var h in resp.Content.Headers) headers[h.Key] = string.Join(",", h.Value);
        return ((int)resp.StatusCode, body, headers);
    }

    public async Task<(int status, string body)> PostAsync(string pathAndQuery, string payload, string contentType = "text/plain")
    {
        var content = new StringContent(payload, Encoding.UTF8, contentType);
        using var resp = await _client.PostAsync(pathAndQuery, content);
        return ((int)resp.StatusCode, await resp.Content.ReadAsStringAsync());
    }

    public async ValueTask DisposeAsync()
    {
        _client.Dispose();
        if (_host is IAsyncDisposable ad) await ad.DisposeAsync();
        else _host.Dispose();
    }

    public Uri BaseUri => _client.BaseAddress!;

    /// <summary>The in-process TestServer — for WebSocket clients (CreateWebSocketClient).</summary>
    public TestServer TestServer { get; }
}
