using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SharpSocketIO.EngineIo;
using SharpSocketIO.EngineIo.Http;
using SharpSocketIO.EngineIo.Parser.Commons;
using Xunit;

namespace SharpSocketIO.EngineIo.Client.Tests;

public class WebSocketIntegrationTests
{
    private static async Task<(IHost host, string baseAddress, Server engine)> StartServerAsync(
        System.Action<SharpSocketIO.EngineIo.Commons.ServerOptions>? configure = null)
    {
        var engine = new Server();
        engine.Options.CookieConfig = null;
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
        var sf = host.Services.GetRequiredService<IServer>();
        var address = sf.Features.Get<IServerAddressesFeature>()!.Addresses.First();
        return (host, address, engine);
    }

    [Fact]
    public async Task Connects_ws_only()
    {
        var (host, baseAddress, engine) = await StartServerAsync(o => o.Transports = new[] { "websocket" });
        try
        {
            var client = new EngineIoClientSocket(baseAddress, new SocketOptions { Transports = new[] { "websocket" } });
            var openTcs = new TaskCompletionSource<bool>();
            client.On("open", _ => openTcs.TrySetResult(true));
            await client.OpenAsync();
            var opened = await Task.WhenAny(openTcs.Task, Task.Delay(5000));
            Assert.True(opened == openTcs.Task, "did not open ws-only within 5s");
            Assert.NotNull(client.Id);
            Assert.Equal("websocket", client.Transport?.Name);
            client.Close();
        }
        finally { await host.StopAsync(); host.Dispose(); }
    }

    [Fact]
    public async Task Upgrades_from_polling_to_websocket()
    {
        var (host, baseAddress, engine) = await StartServerAsync();
        try
        {
            var client = new EngineIoClientSocket(baseAddress);
            var upgradeTcs = new TaskCompletionSource<bool>();
            client.On("upgrade", _ => upgradeTcs.TrySetResult(true));
            await client.OpenAsync();
            var upgraded = await Task.WhenAny(upgradeTcs.Task, Task.Delay(15000));
            Assert.True(upgraded == upgradeTcs.Task, "did not upgrade within 8s");
            Assert.True(client.Upgraded);
            Assert.Equal("websocket", client.Transport?.Name);
            client.Close();
        }
        finally { await host.StopAsync(); host.Dispose(); }
    }

    [Fact]
    public async Task Sends_message_over_websocket_after_upgrade()
    {
        var (host, baseAddress, engine) = await StartServerAsync();
        try
        {
            string? receivedOnServer = null;
            string? receivedOnClient = null;
            var doneTcs = new TaskCompletionSource<bool>();

            engine.On("connection", args =>
            {
                var srvSocket = (Socket)args[0];
                srvSocket.On("message", m =>
                {
                    if (m is object[] arr && arr.Length > 0 && arr[0] is RawData rd)
                    {
                        receivedOnServer = rd.AsString();
                        srvSocket.Send(new[] { new Packet(PacketType.Message, new RawData("echo:" + receivedOnServer)) });
                    }
                });
            });

            var client = new EngineIoClientSocket(baseAddress);
            client.On("upgrade", _ => client.Send("hello-via-ws"));
            client.On("message", args =>
            {
                if (args[0] is RawData rd && rd.AsString() == "echo:hello-via-ws")
                {
                    receivedOnClient = rd.AsString();
                    doneTcs.TrySetResult(true);
                }
            });
            await client.OpenAsync();
            var done = await Task.WhenAny(doneTcs.Task, Task.Delay(15000));
            Assert.True(done == doneTcs.Task, "did not round-trip over ws within 8s");
            Assert.Equal("hello-via-ws", receivedOnServer);
            Assert.Equal("echo:hello-via-ws", receivedOnClient);
            client.Close();
        }
        finally { await host.StopAsync(); host.Dispose(); }
    }
}
