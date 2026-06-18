using System.Collections.Generic;
using System.Text.Json;
using SharpSocketIO.EngineIo;
using SharpSocketIO.EngineIo.Commons;
using Xunit;

namespace SharpSocketIO.EngineIo.Tests;

public class ServerTests
{
    private sealed class Req : IEngineRequest
    {
        public string Method { get; set; } = "GET";
        public string Path { get; set; } = "/engine.io/";
        public IReadOnlyDictionary<string, string> Query { get; set; } = new Dictionary<string, string>();
        public IReadOnlyDictionary<string, string> Headers { get; } = new Dictionary<string, string>();
        public string? RemoteAddress => "127.0.0.1";
        public byte[]? Body => null;
    }

    [Fact]
    public void Verifies_rejects_unknown_transport()
    {
        var server = new Server();
        var (code, message) = server.Verify(new Req { Query = new Dictionary<string, string> { ["transport"] = "tobi" } });
        Assert.Equal(ErrorCodes.TransportUnknown, code);
        Assert.Equal("Transport unknown", message);
    }

    [Fact]
    public void Verifies_rejects_constructor_transport()
    {
        var server = new Server();
        var (code, _) = server.Verify(new Req { Query = new Dictionary<string, string> { ["transport"] = "constructor" } });
        Assert.Equal(ErrorCodes.TransportUnknown, code);
    }

    [Fact]
    public void Verifies_rejects_bad_method()
    {
        var server = new Server();
        var req = new Req
        {
            Method = "PUT",
            Query = new Dictionary<string, string> { ["transport"] = "polling" },
        };
        var (code, _) = server.Verify(req);
        Assert.Equal(ErrorCodes.BadHandshakeMethod, code);
    }

    [Fact]
    public void Verifies_rejects_missing_transport()
    {
        var server = new Server();
        var (code, _) = server.Verify(new Req());
        Assert.Equal(ErrorCodes.TransportUnknown, code);
    }

    [Fact]
    public void Verifies_accepts_polling_handshake()
    {
        var server = new Server();
        var (code, _) = server.Verify(new Req
        {
            Query = new Dictionary<string, string> { ["transport"] = "polling", ["EIO"] = "4" },
        });
        Assert.Null(code);
    }

    [Fact]
    public void Verifies_calls_AllowRequest_and_rejects_when_forbidden()
    {
        var server = new Server
        {
            Options = { AllowRequest = _ => (ErrorCodes.Forbidden, false) }
        };
        var (code, message) = server.Verify(new Req
        {
            Query = new Dictionary<string, string> { ["transport"] = "polling" },
        });
        Assert.Equal(ErrorCodes.Forbidden, code);
        Assert.Equal("Forbidden", message);
    }

    [Fact]
    public void Handshake_open_packet_has_expected_fields()
    {
        var server = new Server();
        var json = server.BuildHandshakeData(sid: "abc123");
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        Assert.Equal("abc123", root.GetProperty("sid").GetString());
        Assert.Equal(25000, root.GetProperty("pingInterval").GetInt32());
        Assert.Equal(20000, root.GetProperty("pingTimeout").GetInt32());
        Assert.Equal(1000000, root.GetProperty("maxPayload").GetInt32());
        Assert.Equal("websocket", root.GetProperty("upgrades")[0].GetString());
    }

    [Fact]
    public void Handshake_open_packet_no_upgrades_when_disabled()
    {
        var server = new Server { Options = { AllowUpgrades = false } };
        var json = server.BuildHandshakeData(sid: "x");
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(0, doc.RootElement.GetProperty("upgrades").GetArrayLength());
    }
}
