using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SharpSocketIO.EngineIo;
using SharpSocketIO.EngineIo.Http;
using Xunit;

namespace SharpSocketIO.EngineIo.Client.Tests;

/// <summary>
/// Minimal isolation test: POST a known body via HttpClient to a fresh engine.io server
/// after a polling handshake, and assert the server received the body. Rules out
/// transport-level body-loss vs server-side body-read issues.
/// </summary>
public class PostBodyIsolationTests
{
    private static async Task<string> StartServerAsync()
    {
        var engine = new Server();
        engine.Options.CookieConfig = null;
        var builder = Host.CreateDefaultBuilder()
            .ConfigureWebHost(web =>
            {
                web.UseKestrel();
                web.UseUrls("http://127.0.0.1:0");
                web.Configure(app => engine.Attach(app));
            });
        var host = builder.Build();
        await host.StartAsync();
        var sf = host.Services.GetRequiredService<IServer>();
        return sf.Features.Get<IServerAddressesFeature>()!.Addresses.First();
    }

    [Fact]
    public async Task Raw_HttpClient_POST_body_is_received_by_server()
    {
        var baseAddress = await StartServerAsync();
        using var http = new HttpClient();

        // 1. handshake GET to obtain a sid
        var hsResp = await http.GetAsync(baseAddress + "/engine.io/?EIO=4&transport=polling");
        var hsBody = await hsResp.Content.ReadAsStringAsync();
        var sidMatch = System.Text.RegularExpressions.Regex.Match(hsBody, "\"sid\":\"([^\"]+)\"");
        Assert.True(sidMatch.Success, $"no sid in handshake body: {hsBody}");
        var sid = sidMatch.Groups[1].Value;

        // 2. POST a known body with that sid
        var payload = "4hello-world";
        using var content = new StringContent(payload, Encoding.UTF8, "text/plain");
        var postResp = await http.PostAsync(baseAddress + $"/engine.io/?EIO=4&transport=polling&sid={sid}", content);
        var postBody = await postResp.Content.ReadAsStringAsync();
        Assert.Equal(System.Net.HttpStatusCode.OK, postResp.StatusCode);
        Assert.Equal("ok", postBody);
    }
}
