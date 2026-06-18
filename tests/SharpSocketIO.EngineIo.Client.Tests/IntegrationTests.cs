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

/// <summary>
/// End-to-end integration: the engine.io-client (polling) against a real Kestrel
/// SharpSocketIO.EngineIo server. Proves wire-format compatibility.
/// </summary>
public class IntegrationTests
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
                web.Configure(app => engine.Attach(app));
            });
        var host = builder.Build();
        await host.StartAsync();
        var sf = host.Services.GetRequiredService<IServer>();
        var address = sf.Features.Get<IServerAddressesFeature>()!.Addresses.First();
        return (host, address, engine);
    }

    [Fact]
    public async Task Client_opens_and_receives_open_packet_via_polling()
    {
        var (host, baseAddress, engine) = await StartServerAsync();
        try
        {
            var client = new EngineIoClientSocket(baseAddress);
            var openTcs = new TaskCompletionSource<bool>();
            client.On("open", _ => openTcs.TrySetResult(true));
            await client.OpenAsync();
            var opened = await Task.WhenAny(openTcs.Task, Task.Delay(5000));
            Assert.True(opened == openTcs.Task, "client did not open within 5s");
            Assert.NotNull(client.Id);
            Assert.Equal(SocketReadyState.Open, client.ReadyState);
            client.Close();
        }
        finally
        {
            await host.StopAsync();
            host.Dispose();
        }
    }

    [Fact]
    public async Task Client_send_round_trips_through_server()
    {
        var (host, baseAddress, engine) = await StartServerAsync();
        try
        {
            string? receivedOnServer = null;
            string? receivedOnClient = null;
            var pongTcs = new TaskCompletionSource<bool>();

            bool connectionFired = false;
            engine.On("connection", args =>
            {
                connectionFired = true;
                var srvSocket = (Socket)args[0];
                srvSocket.On("packet", p =>
                {
                    // diagnostic: any packet arriving at the server-side socket
                    if (p is object[] arr && arr.Length > 0 && arr[0] is Packet pkt && pkt.Type == PacketType.Message)
                    {
                        receivedOnServer = pkt.Data?.AsString();
                    }
                });
                srvSocket.On("message", m =>
                {
                    if (m is object[] arr && arr.Length > 0 && arr[0] is RawData rd && rd.AsString() == "ping-from-client")
                    {
                        receivedOnServer = rd.AsString();
                        srvSocket.Send(new[]
                        {
                            new Packet(PacketType.Message, new RawData("pong-from-server"))
                        });
                    }
                });
            });

            int clientMessageEvents = 0;
            string? lastClientMessage = null;
            var client = new EngineIoClientSocket(baseAddress, new SocketOptions { Upgrade = false });
            client.On("open", _ => client.Send("ping-from-client"));
            client.On("message", args =>
            {
                clientMessageEvents++;
                if (args[0] is RawData rd) lastClientMessage = rd.AsString();
                if (args[0] is RawData rd2 && rd2.AsString() == "pong-from-server")
                {
                    receivedOnClient = rd2.AsString();
                    pongTcs.TrySetResult(true);
                }
            });

            await client.OpenAsync();
            // give the round-trip up to 8s
            var done = await Task.WhenAny(pongTcs.Task, Task.Delay(15000));
            var pt = client.Transport as SharpSocketIO.EngineIo.Client.Transports.PollingTransport;
            var spt = engine.Clients.Values.FirstOrDefault()?.Transport as SharpSocketIO.EngineIo.Transports.PollingHttp;
            Assert.True(done == pongTcs.Task, $"did not round-trip (conn={connectionFired}, srvMsg={receivedOnServer ?? "null"}, cliMsgEvents={clientMessageEvents}, cliState={client.ReadyState}, pollIter={pt?.PollLoopIterations}, lastPollBody={pt?.LastPollBody ?? "null"}, srvBodyLen={spt?.LastPostBodyLength})");
            Assert.Equal("ping-from-client", receivedOnServer);
            Assert.Equal("pong-from-server", receivedOnClient);
            client.Close();
        }
        finally
        {
            await host.StopAsync();
            host.Dispose();
        }
    }
}
