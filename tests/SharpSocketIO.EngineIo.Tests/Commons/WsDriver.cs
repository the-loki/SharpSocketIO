using System;
using System.IO;
using System.Net.WebSockets;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace SharpSocketIO.EngineIo.Tests.Commons;

/// <summary>
/// WebSocket client driver for the engine.io ws surface + polling→ws upgrade handshake.
/// Uses ClientWebSocket against a real Kestrel server. NOT the engine.io-client package.
/// </summary>
public sealed class WsDriver
{
    private readonly Uri _baseAddress;
    private ClientWebSocket? _ws;

    public WsDriver(Uri baseAddress)
    {
        // ws:// or http:// → ws://
        _baseAddress = new UriBuilder(baseAddress) { Scheme = "ws" }.Uri;
    }

    /// <summary>Opens a fresh ws-only session; returns the open packet text and sid.</summary>
    public async Task<(string openText, string sid)> ConnectWsOnlyAsync(
        string path = "/engine.io/?EIO=4&transport=websocket")
    {
        _ws = new ClientWebSocket();
        await _ws.ConnectAsync(new Uri(_baseAddress, path), CancellationToken.None);
        var openText = await ReceiveTextAsync();
        var sid = ExtractSid(openText);
        return (openText, sid);
    }

    /// <summary>Upgrades an existing polling session (sid) to websocket via the probe sequence.</summary>
    public async Task UpgradeAsync(string sid, string pathBase = "/engine.io/?EIO=4&transport=websocket&sid=")
    {
        _ws = new ClientWebSocket();
        await _ws.ConnectAsync(new Uri(_baseAddress, pathBase + sid), CancellationToken.None);
        await SendTextAsync("2probe");
        var pong = await ReceiveTextAsync();
        if (!pong.StartsWith("3probe"))
            throw new InvalidOperationException($"expected 3probe, got '{pong}'");
        await SendTextAsync("5");
    }

    public async Task SendTextAsync(string text)
    {
        var bytes = Encoding.UTF8.GetBytes(text);
        await _ws!.SendAsync(bytes, WebSocketMessageType.Text, endOfMessage: true, CancellationToken.None);
    }

    public async Task SendBinaryAsync(byte[] data)
    {
        await _ws!.SendAsync(data, WebSocketMessageType.Binary, endOfMessage: true, CancellationToken.None);
    }

    public async Task<string> ReceiveTextAsync(CancellationToken? ct = null)
    {
        var token = ct ?? CancellationToken.None;
        var buffer = new byte[16 * 1024];
        using var ms = new MemoryStream();
        WebSocketReceiveResult result;
        do
        {
            result = await _ws!.ReceiveAsync(buffer, token);
            if (result.MessageType == WebSocketMessageType.Close)
                throw new InvalidOperationException("ws closed");
            ms.Write(buffer, 0, result.Count);
        } while (!result.EndOfMessage);
        return Encoding.UTF8.GetString(ms.ToArray());
    }

    public async Task CloseAsync()
    {
        if (_ws != null && _ws.State == WebSocketState.Open)
        {
            try
            {
                await _ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "", CancellationToken.None);
            }
            catch (WebSocketException)
            {
                // server may have aborted the connection without completing the close handshake; that's fine
            }
        }
    }

    public static string ExtractSid(string openPacketBody)
    {
        var m = Regex.Match(openPacketBody, "\"sid\":\"([^\"]+)\"");
        return m.Success ? m.Groups[1].Value : string.Empty;
    }
}
