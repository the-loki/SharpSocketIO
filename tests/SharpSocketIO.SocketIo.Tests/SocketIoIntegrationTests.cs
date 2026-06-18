using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SharpSocketIO.EngineIo;
using SharpSocketIO.EngineIo.Client;
using SharpSocketIO.EngineIo.Http;
using SharpSocketIO.EngineIo.Parser.Commons;
using Xunit;

namespace SharpSocketIO.SocketIo.Tests;

public class SocketIoIntegrationTests
{
    private static async Task<(IHost host, string baseAddress, Server io, EngineIo.Server engine)> StartAsync()
    {
        var engine = new EngineIo.Server();
        engine.Options.CookieConfig = null;
        var io = new Server();
        io.Attach(engine);
        var builder = Host.CreateDefaultBuilder()
            .ConfigureWebHost(web =>
            {
                web.UseKestrel();
                web.UseUrls("http://127.0.0.1:0");
                web.Configure(app => ServerExtensions.Attach(engine, app));
            });
        var host = builder.Build();
        await host.StartAsync();
        var sf = host.Services.GetRequiredService<IServer>();
        return (host, sf.Features.Get<IServerAddressesFeature>()!.Addresses.First(), io, engine);
    }

    private static async Task OpenAsync(EngineIoClientSocket transport)
    {
        var openTcs = new TaskCompletionSource<bool>();
        transport.On("open", _ => openTcs.TrySetResult(true));
        await transport.OpenAsync();
        await Task.WhenAny(openTcs.Task, Task.Delay(15000));
    }

    [Fact]
    public async Task Socket_io_client_connects_to_default_namespace()
    {
        var (host, baseAddress, io, engine) = await StartAsync();
        try
        {
            bool connectionFired = false;
            io.Of("/").On("connection", _ => connectionFired = true);

            var transport = new EngineIoClientSocket(baseAddress, new EngineIo.Client.SocketOptions { Upgrade = false });
            await OpenAsync(transport);
            transport.Send("0"); // socket.io CONNECT packet for "/"

            await Task.Delay(800);
            Assert.True(connectionFired, "socket.io connection event did not fire");
            transport.Close();
        }
        finally { await host.StopAsync(); host.Dispose(); }
    }

    [Fact]
    public async Task Server_emits_event_reaches_client()
    {
        var (host, baseAddress, io, engine) = await StartAsync();
        try
        {
            var transport = new EngineIoClientSocket(baseAddress, new EngineIo.Client.SocketOptions { Upgrade = false });
            await OpenAsync(transport);

            string? receivedEvent = null;
            transport.On("message", args =>
            {
                if (args[0] is RawData rd)
                {
                    var text = rd.AsString() ?? "";
                    if (text.StartsWith("2")) receivedEvent = text; // socket.io EVENT packet
                }
            });

            io.Of("/").On("connection", args =>
            {
                var s = (Socket)args[0];
                s.Emit("hello", "world");
            });
            transport.Send("0"); // CONNECT
            await Task.Delay(1000);
            Assert.NotNull(receivedEvent);
            Assert.Contains("hello", receivedEvent!);
            Assert.Contains("world", receivedEvent!);
            transport.Close();
        }
        finally { await host.StopAsync(); host.Dispose(); }
    }

    [Fact]
    public async Task Client_emits_event_reaches_server_handler()
    {
        var (host, baseAddress, io, engine) = await StartAsync();
        try
        {
            string? receivedEvent = null;
            object[]? receivedArgs = null;
            io.Of("/").On("connection", args =>
            {
                var s = (Socket)args[0];
                s.On("client-msg", m =>
                {
                    receivedEvent = "client-msg";
                    receivedArgs = m;
                });
            });

            var transport = new EngineIoClientSocket(baseAddress, new EngineIo.Client.SocketOptions { Upgrade = false });
            await OpenAsync(transport);
            transport.Send("0"); // CONNECT
            await Task.Delay(500);
            // client emits a socket.io EVENT: 2["client-msg","hi"]
            transport.Send("2[\"client-msg\",\"hi\"]");
            await Task.Delay(800);
            Assert.Equal("client-msg", receivedEvent);
            Assert.NotNull(receivedArgs);
            Assert.Equal("hi", receivedArgs![0]);
            transport.Close();
        }
        finally { await host.StopAsync(); host.Dispose(); }
    }
}
