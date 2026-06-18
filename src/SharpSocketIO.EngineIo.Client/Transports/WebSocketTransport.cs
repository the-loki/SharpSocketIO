using System;
using System.Collections.Generic;
using System.IO;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SharpSocketIO.EngineIo.Client.Contrib;
using SharpSocketIO.EngineIo.Parser;
using SharpSocketIO.EngineIo.Parser.Commons;
using EioParser = SharpSocketIO.EngineIo.Parser;

namespace SharpSocketIO.EngineIo.Client.Transports;

/// <summary>
/// Port of lib/transports/websocket.ts (client). Wraps ClientWebSocket; each engine.io
/// packet maps to one WS message (Text for string, Binary for binary). A receive loop
/// decodes incoming messages to packets.
/// </summary>
public sealed class WebSocketTransport : Transport
{
    private ClientWebSocket? _ws;
    private readonly string _baseUrl;
    private readonly CancellationTokenSource _cts = new();

    public WebSocketTransport(SocketOptions opts) : base(opts)
    {
        var scheme = opts.Secure ? "wss" : "ws";
        var host = opts.Hostname ?? "localhost";
        string port = "";
        if (!string.IsNullOrEmpty(opts.Port) &&
            !((opts.Secure && opts.Port == "443") || (!opts.Secure && opts.Port == "80")))
        {
            port = ":" + opts.Port;
        }
        _baseUrl = $"{scheme}://{host}{port}{opts.Path}";
    }

    public override string Name => "websocket";

    protected override void DoOpen()
    {
        _ = Task.Run(async () =>
        {
            try
            {
                _ws = new ClientWebSocket();
                await _ws.ConnectAsync(BuildUri(), _cts.Token);
                OnOpen();
                _ = ReceiveLoopAsync();
            }
            catch (Exception ex)
            {
                OnError("websocket error", ex.Message);
                OnClose(new CloseDetails { Description = "websocket connect error", Context = ex.Message });
            }
        });
    }

    protected override void DoClose()
    {
        _cts.Cancel();
        _ = Task.Run(async () =>
        {
            if (_ws != null && _ws.State == WebSocketState.Open)
            {
                try { await _ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "", CancellationToken.None); } catch { }
            }
            OnClose();
        });
    }

    protected override void Write(IReadOnlyList<Packet> packets) => _ = SendAsync(packets);

    private async Task SendAsync(IReadOnlyList<Packet> packets)
    {
        if (_ws == null) return;
        for (int i = 0; i < packets.Count; i++)
        {
            var packet = packets[i];
            if (packet.Data is { Kind: RawDataKind.ByteArray or RawDataKind.ArrayBuffer })
            {
                RawData encoded = default;
                EioParser.EncodePacket.Encode(packet, true, r => encoded = r);
                var bytes = encoded.Kind == RawDataKind.ByteArray ? encoded.AsByteArray()! : encoded.AsArrayBuffer()!.Value.ToArray();
                await _ws.SendAsync(bytes, WebSocketMessageType.Binary, true, _cts.Token);
            }
            else
            {
                string captured = string.Empty;
                EioParser.EncodePacket.Encode(packet, true, r => captured = r.AsString()!);
                await _ws.SendAsync(Encoding.UTF8.GetBytes(captured), WebSocketMessageType.Text, true, _cts.Token);
            }
        }
        Writable = true;
        EmitReserved("drain");
    }

    private async Task ReceiveLoopAsync()
    {
        var buffer = new byte[16 * 1024];
        try
        {
            while (!_cts.IsCancellationRequested && _ws!.State == WebSocketState.Open)
            {
                using var msg = new MemoryStream();
                WebSocketReceiveResult result;
                do
                {
                    result = await _ws.ReceiveAsync(buffer, _cts.Token);
                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        OnClose(new CloseDetails { Description = "websocket connection closed" });
                        return;
                    }
                    msg.Write(buffer, 0, result.Count);
                } while (!result.EndOfMessage);

                var data = msg.ToArray();
                if (result.MessageType == WebSocketMessageType.Binary)
                    OnData(new RawData(data));
                else
                    OnData(new RawData(Encoding.UTF8.GetString(data)));
            }
        }
        catch (OperationCanceledException) { }
        catch (WebSocketException)
        {
            OnClose(new CloseDetails { Description = "websocket connection closed" });
        }
    }

    private Uri BuildUri()
    {
        var query = new Dictionary<string, string>(Query) { ["EIO"] = "4", ["transport"] = "websocket" };
        if (!SupportsBinary) query["b64"] = "1";
        return new Uri(_baseUrl + "?" + Parseqs.Encode(query));
    }
}
