using System;
using System.Collections.Generic;
using System.IO;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SharpSocketIO.EngineIo.Commons;
using SharpSocketIO.EngineIo.Http;
using SharpSocketIO.EngineIo.Parser.Commons;
using EioParser = SharpSocketIO.EngineIo.Parser;

namespace SharpSocketIO.EngineIo.Transports;

/// <summary>
/// Port of lib/transports/websocket.ts. Wraps a System.Net.WebSockets.WebSocket;
/// each engine.io packet maps to one WS message (Text for string packets, Binary
/// for binary). A background receive loop decodes incoming messages to packets.
/// </summary>
public sealed class WebSocketTransport : Transport
{
    private readonly WebSocket _ws;
    private readonly CancellationTokenSource _cts = new();
    private readonly long _maxPayload;

    public WebSocketTransport(HttpContextEngineRequest req, WebSocket ws, long maxPayload)
        : base(req)
    {
        _ws = ws;
        _maxPayload = maxPayload > 0 ? maxPayload : 1_000_000;
        Writable = true;
    }

    public override string Name => "websocket";

    /// <summary>Starts the background receive loop. Returns when the socket closes.</summary>
    public async Task RunReceiveLoopAsync()
    {
        try
        {
            await RunReceiveLoopCoreAsync();
        }
        catch (Exception)
        {
            // never let an unobserved exception escape — it would crash the process
            try { OnClose(); } catch { }
        }
    }

    private async Task RunReceiveLoopCoreAsync()
    {
        var buffer = new byte[16 * 1024];
        while (!_cts.IsCancellationRequested && _ws.State == WebSocketState.Open)
        {
            WebSocketReceiveResult result;
            using var msg = new MemoryStream();
            try
            {
                do
                {
                    result = await _ws.ReceiveAsync(buffer, _cts.Token);
                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        OnClose();
                        return;
                    }
                    msg.Write(buffer, 0, result.Count);
                } while (!result.EndOfMessage);
            }
            catch (OperationCanceledException) { return; }
            catch (WebSocketException) { OnClose(); return; }
            catch (IOException) { OnClose(); return; }

            if (msg.Length > _maxPayload)
            {
                try { await _ws.CloseAsync(WebSocketCloseStatus.MessageTooBig, "payload too big", CancellationToken.None); } catch { }
                OnClose();
                return;
            }

            var data = msg.ToArray();
            if (result!.MessageType == WebSocketMessageType.Binary)
            {
                OnPacket(EioParser.DecodePacket.Decode(new RawData(data)));
            }
            else
            {
                var text = Encoding.UTF8.GetString(data);
                OnPacket(EioParser.DecodePacket.Decode(new RawData(text)));
            }
        }
    }

    public override void Send(IReadOnlyList<Packet> packets)
    {
        // Fire-and-forget, but observe exceptions to avoid crashing the process.
        _ = SendAsync(packets).ContinueWith(t =>
        {
            if (t.IsFaulted) OnError("write error", t.Exception?.InnerException?.Message);
        }, TaskScheduler.Default);
    }

    public async Task SendAsync(IReadOnlyList<Packet> packets)
    {
        for (int i = 0; i < packets.Count; i++)
        {
            await SendOneAsync(packets[i]);
        }
        Writable = true;
        Emit("drain");
        Emit("ready");
    }

    private async Task SendOneAsync(Packet packet)
    {
        if (packet.Data is { Kind: RawDataKind.ByteArray or RawDataKind.ArrayBuffer })
        {
            RawData encoded = default;
            EioParser.EncodePacket.Encode(packet, true, r => encoded = r);
            byte[] bytes = encoded.Kind == RawDataKind.ByteArray
                ? encoded.AsByteArray()!
                : encoded.AsArrayBuffer()!.Value.ToArray();
            await _ws.SendAsync(bytes, WebSocketMessageType.Binary, endOfMessage: true, _cts.Token);
        }
        else
        {
            string captured = string.Empty;
            EioParser.EncodePacket.Encode(packet, true, r => captured = r.AsString()!);
            var bytes = Encoding.UTF8.GetBytes(captured);
            await _ws.SendAsync(bytes, WebSocketMessageType.Text, endOfMessage: true, _cts.Token);
        }
    }

    public override void DoClose(Action? callback = null)
    {
        _ = Task.Run(async () =>
        {
            try
            {
                _cts.Cancel();
                if (_ws.State == WebSocketState.Open)
                    await _ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "closed", CancellationToken.None);
            }
            catch { }
            finally { callback?.Invoke(); }
        });
    }
}
