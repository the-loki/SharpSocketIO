using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SharpSocketIO.EngineIo;
using SharpSocketIO.EngineIo.Parser.Commons;
using Xunit;
using PacketType = SharpSocketIO.EngineIo.Parser.Commons.PacketType;

namespace SharpSocketIO.EngineIo.Client.Tests;

public class BinaryIntegrationTests
{
    private static async Task<(IHost host, string baseAddress, Server engine)> StartServerAsync()
    {
        var engine = new Server();
        engine.Options.CookieConfig = null;
        var builder = Host.CreateDefaultBuilder()
            .ConfigureWebHost(web =>
            {
                web.UseKestrel();
                web.UseUrls("http://127.0.0.1:0");
                web.Configure(app => SharpSocketIO.EngineIo.Http.ServerExtensions.Attach(engine, app));
            });
        var host = builder.Build();
        await host.StartAsync();
        var sf = host.Services.GetRequiredService<IServer>();
        return (host, sf.Features.Get<IServerAddressesFeature>()!.Addresses.First(), engine);
    }

    [Fact]
    public async Task Binary_round_trips_over_polling()
    {
        var (host, baseAddress, engine) = await StartServerAsync();
        try
        {
            byte[] sent = { 1, 2, 3, 4, 5 };
            byte[]? received = null;
            var doneTcs = new TaskCompletionSource<bool>();

            engine.On("connection", args =>
            {
                var srvSocket = (Socket)args[0];
                srvSocket.On("message", m =>
                {
                    if (m is object[] arr && arr.Length > 0 && arr[0] is RawData rd && rd.IsBinary)
                    {
                        var echo = rd.AsByteArray()!;
                        srvSocket.Send(new[] { new Packet(PacketType.Message, new RawData(echo)) });
                    }
                });
            });

            var client = new EngineIoClientSocket(baseAddress, new SocketOptions { Upgrade = false });
            client.On("open", _ => client.Send(sent));
            client.On("message", args =>
            {
                if (args[0] is RawData rd && rd.IsBinary)
                {
                    received = rd.AsByteArray();
                    doneTcs.TrySetResult(true);
                }
            });
            await client.OpenAsync();
            var done = await Task.WhenAny(doneTcs.Task, Task.Delay(15000));
            Assert.True(done == doneTcs.Task, "binary polling round-trip did not complete");
            Assert.Equal(sent, received);
            client.Close();
        }
        finally { await host.StopAsync(); host.Dispose(); }
    }

    [Fact]
    public async Task Binary_round_trips_over_websocket()
    {
        var (host, baseAddress, engine) = await StartServerAsync();
        try
        {
            byte[] sent = { 10, 20, 30, 40, 50, 60 };
            byte[]? received = null;
            var doneTcs = new TaskCompletionSource<bool>();

            engine.On("connection", args =>
            {
                var srvSocket = (Socket)args[0];
                srvSocket.On("message", m =>
                {
                    if (m is object[] arr && arr.Length > 0 && arr[0] is RawData rd && rd.IsBinary)
                    {
                        srvSocket.Send(new[] { new Packet(PacketType.Message, new RawData(rd.AsByteArray()!)) });
                    }
                });
            });

            var client = new EngineIoClientSocket(baseAddress);
            client.On("upgrade", _ => client.Send(sent));
            client.On("message", args =>
            {
                if (args[0] is RawData rd && rd.IsBinary)
                {
                    received = rd.AsByteArray();
                    doneTcs.TrySetResult(true);
                }
            });
            await client.OpenAsync();
            var done = await Task.WhenAny(doneTcs.Task, Task.Delay(15000));
            Assert.True(done == doneTcs.Task, "binary ws round-trip did not complete");
            Assert.Equal(sent, received);
            client.Close();
        }
        finally { await host.StopAsync(); host.Dispose(); }
    }

    [Fact]
    public async Task ForceBase64_round_trips_binary_over_polling()
    {
        var (host, baseAddress, engine) = await StartServerAsync();
        try
        {
            byte[] sent = { 7, 8, 9 };
            byte[]? received = null;
            var doneTcs = new TaskCompletionSource<bool>();

            engine.On("connection", args =>
            {
                var srvSocket = (Socket)args[0];
                srvSocket.On("message", m =>
                {
                    if (m is object[] arr && arr.Length > 0 && arr[0] is RawData rd && rd.IsBinary)
                    {
                        srvSocket.Send(new[] { new Packet(PacketType.Message, new RawData(rd.AsByteArray()!)) });
                    }
                });
            });

            var client = new EngineIoClientSocket(baseAddress, new SocketOptions { Upgrade = false, ForceBase64 = true });
            client.On("open", _ => client.Send(sent));
            client.On("message", args =>
            {
                if (args[0] is RawData rd && rd.IsBinary)
                {
                    received = rd.AsByteArray();
                    doneTcs.TrySetResult(true);
                }
            });
            await client.OpenAsync();
            var done = await Task.WhenAny(doneTcs.Task, Task.Delay(15000));
            Assert.True(done == doneTcs.Task, "force-base64 round-trip did not complete");
            Assert.Equal(sent, received);
            client.Close();
        }
        finally { await host.StopAsync(); host.Dispose(); }
    }
}
