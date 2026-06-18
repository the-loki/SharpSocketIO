using global::SharpSocketIO.SocketIoClient;
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
using EngineIoServerExtensions = SharpSocketIO.EngineIo.Http.ServerExtensions;

namespace SharpSocketIO.Tests.SocketIoClient;

public class IntegrationTests
{
    private static async Task<(IHost host, string baseAddress)> StartServerAsync(
        System.Action<SharpSocketIO.SocketIo.Server>? onReady = null)
    {
        var engine = new EngineIo.Server();
        engine.Options.CookieConfig = null;
        
        var io = new SharpSocketIO.SocketIo.Server();
        io.Attach(engine);
        onReady?.Invoke(io);
        var builder = Host.CreateDefaultBuilder()
            .ConfigureWebHost(web =>
            {
                web.UseKestrel();
                web.UseUrls("http://127.0.0.1:0");
                web.Configure(app => EngineIoServerExtensions.Attach(engine, app, new SharpSocketIO.EngineIo.Commons.AttachOptions { Path = "/socket.io" }));
            });
        var host = builder.Build();
        await host.StartAsync();
        var sf = host.Services.GetRequiredService<IServer>();
        return (host, sf.Features.Get<IServerAddressesFeature>()!.Addresses.First());
    }

    [Fact]
    public async Task Socket_io_client_connects_and_receives_connect_event()
    {
        var (host, baseAddress) = await StartServerAsync();
        try
        {
            var manager = new Manager(baseAddress, new ManagerOptions
            {
                Path = "/socket.io",
                AutoConnect = false,
                Reconnection = false,
            });
            var socket = manager.Socket("/");
            var connectTcs = new TaskCompletionSource<bool>();
            socket.On("connect", _ => connectTcs.TrySetResult(true));

            await manager.OpenAsync();
            await socket.ConnectAsync();
            var connected = await Task.WhenAny(connectTcs.Task, Task.Delay(15000));
            Assert.True(connected == connectTcs.Task, "did not connect within 15s");
            Assert.True(socket.Connected);
            manager.DisconnectAll();
        }
        finally { await host.StopAsync(); host.Dispose(); }
    }

    [Fact]
    public async Task Client_emits_event_with_ack_and_receives_reply()
    {
        var (host, baseAddress) = await StartServerAsync(io =>
        {
            io.Of("/").On("connection", args =>
            {
                var s = (SharpSocketIO.SocketIo.Socket)args[0];
                s.On("ping", m =>
                {
                    // reply with an ACK (m is object[]; the AckCallback is the last arg)
                    if (m is object[] arr && arr.Length > 0 && arr[arr.Length - 1] is SharpSocketIO.SocketIo.AckCallback ack)
                    {
                        ack.Send("pong");
                    }
                });
            });
        });
        try
        {
            var manager = new Manager(baseAddress, new ManagerOptions
            {
                Path = "/socket.io", AutoConnect = false, Reconnection = false,
            });
            var socket = manager.Socket("/");
            var connectTcs = new TaskCompletionSource<bool>();
            socket.On("connect", _ => connectTcs.TrySetResult(true));
            await manager.OpenAsync();
            await socket.ConnectAsync();
            await Task.WhenAny(connectTcs.Task, Task.Delay(15000));

            string? ackReply = null;
            var ackTcs = new TaskCompletionSource<bool>();
            socket.EmitWithAck("ping", data =>
            {
                if (data.Length > 0) ackReply = data[0]?.ToString();
                ackTcs.TrySetResult(true);
            });
            var done = await Task.WhenAny(ackTcs.Task, Task.Delay(15000));
            Assert.True(done == ackTcs.Task, "did not receive ack within 15s");
            Assert.Equal("pong", ackReply);
            manager.DisconnectAll();
        }
        finally { await host.StopAsync(); host.Dispose(); }
    }

    [Fact]
    public async Task Server_emits_event_reaches_client_handler()
    {
        var (host, baseAddress) = await StartServerAsync(io =>
        {
            io.Of("/").On("connection", args =>
            {
                var s = (SharpSocketIO.SocketIo.Socket)args[0];
                s.Emit("welcome", "client");
            });
        });
        try
        {
            var manager = new Manager(baseAddress, new ManagerOptions
            {
                Path = "/socket.io", AutoConnect = false, Reconnection = false,
            });
            var socket = manager.Socket("/");
            string? gotEvent = null;
            object[]? gotArgs = null;
            var doneTcs = new TaskCompletionSource<bool>();
            socket.On("connect", _ => { });
            socket.On("welcome", args =>
            {
                gotEvent = "welcome";
                gotArgs = args;
                doneTcs.TrySetResult(true);
            });
            await manager.OpenAsync();
            await socket.ConnectAsync();
            var done = await Task.WhenAny(doneTcs.Task, Task.Delay(15000));
            Assert.True(done == doneTcs.Task, "did not receive welcome within 15s");
            Assert.Equal("welcome", gotEvent);
            Assert.Equal("client", gotArgs![0]);
            manager.DisconnectAll();
        }
        finally { await host.StopAsync(); host.Dispose(); }
    }
}
