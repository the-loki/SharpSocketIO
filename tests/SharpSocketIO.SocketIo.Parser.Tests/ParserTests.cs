using System;
using System.Collections.Generic;
using System.Text.Json;
using SharpSocketIO.SocketIo.Parser;
using SharpSocketIO.SocketIo.Parser.Commons;
using Xunit;

namespace SharpSocketIO.SocketIo.Parser.Tests;

public class ParserTests
{
    [Fact]
    public void Exposes_packet_types_as_numbers()
    {
        Assert.Equal(0, (int)PacketType.Connect);
        Assert.Equal(1, (int)PacketType.Disconnect);
        Assert.Equal(2, (int)PacketType.Event);
        Assert.Equal(3, (int)PacketType.Ack);
        Assert.Equal(4, (int)PacketType.ConnectError);
        Assert.Equal(5, (int)PacketType.BinaryEvent);
        Assert.Equal(6, (int)PacketType.BinaryAck);
    }

    [Fact]
    public void Protocol_is_5()
    {
        Assert.Equal(5, SocketIoParser.Protocol);
    }

    // Port of helpers.test: encode then decode, expect deep equality.
    internal static void TestRoundTrip(Packet obj)
    {
        var encoder = new Encoder();
        var encoded = encoder.Encode(obj);
        Packet? decoded = null;
        var decoder = new Decoder();
        decoder.On("decoded", args => decoded = (Packet)args[0]);
        foreach (var part in encoded) decoder.Add(part);
        Assert.NotNull(decoded);
        AssertEqual(obj, decoded!);
    }

    internal static void AssertEqual(Packet a, Packet b)
    {
        Assert.Equal(a.Type, b.Type);
        Assert.Equal(a.Nsp, b.Nsp);
        Assert.Equal(a.Id, b.Id);
        Assert.Equal(a.Attachments, b.Attachments);
        Assert.Equal(JsonSerializer.Serialize(a.Data), JsonSerializer.Serialize(b.Data));
    }

    [Fact]
    public void Encodes_connection()
    {
        TestRoundTrip(new Packet
        {
            Type = PacketType.Connect,
            Nsp = "/woot",
            Data = new Dictionary<string, object> { ["token"] = "123" },
        });
    }

    [Fact]
    public void Encodes_disconnection()
    {
        TestRoundTrip(new Packet { Type = PacketType.Disconnect, Nsp = "/woot" });
    }

    [Fact]
    public void Encodes_an_event()
    {
        TestRoundTrip(new Packet
        {
            Type = PacketType.Event,
            Data = new object[] { "a", 1, new Dictionary<string, object>() },
            Nsp = "/",
        });
    }

    [Fact]
    public void Encodes_an_event_with_integer_event_name()
    {
        TestRoundTrip(new Packet
        {
            Type = PacketType.Event,
            Data = new object[] { 1, "a", new Dictionary<string, object>() },
            Nsp = "/",
        });
    }

    [Fact]
    public void Encodes_an_event_with_ack()
    {
        TestRoundTrip(new Packet
        {
            Type = PacketType.Event,
            Data = new object[] { "a", 1, new Dictionary<string, object>() },
            Id = 1,
            Nsp = "/test",
        });
    }

    [Fact]
    public void Encodes_an_ack()
    {
        TestRoundTrip(new Packet
        {
            Type = PacketType.Ack,
            Data = new object[] { "a", 1, new Dictionary<string, object>() },
            Id = 123,
            Nsp = "/",
        });
    }

    [Fact]
    public void Encodes_a_connect_error_string()
    {
        TestRoundTrip(new Packet
        {
            Type = PacketType.ConnectError,
            Data = "Unauthorized",
            Nsp = "/",
        });
    }

    [Fact]
    public void Encodes_a_connect_error_object()
    {
        TestRoundTrip(new Packet
        {
            Type = PacketType.ConnectError,
            Data = new Dictionary<string, object> { ["message"] = "Unauthorized" },
            Nsp = "/",
        });
    }

    [Fact]
    public void Throws_on_circular_object_encode()
    {
        var a = new Dictionary<string, object>();
        a["b"] = a;
        var data = new Packet { Type = PacketType.Event, Data = a, Id = 1, Nsp = "/" };
        var encoder = new Encoder();
        Assert.ThrowsAny<Exception>(() => encoder.Encode(data));
    }

    [Fact]
    public void Decodes_bad_binary_packet_throws_illegal()
    {
        var decoder = new Decoder();
        Assert.ThrowsAny<Exception>(() => decoder.Add("5"));
    }

    [Fact]
    public void Throws_when_too_many_attachments()
    {
        var decoder = new Decoder(new DecoderOptions { MaxAttachments = 2 });
        Assert.ThrowsAny<Exception>(() => decoder.Add(
            "53-[\"hello\",{\"_placeholder\":true,\"num\":0},{\"_placeholder\":true,\"num\":1},{\"_placeholder\":true,\"num\":2}]"));
    }

    [Fact]
    public void Decodes_with_custom_reviver_function()
    {
        var decoder = new Decoder((key, value) => key == "a" && value is string s ? s.ToUpper() : value);
        Packet? got = null;
        decoder.On("decoded", args => got = (Packet)args[0]);
        decoder.Add("2[\"b\",{\"a\":\"val\"}]");
        Assert.NotNull(got);
        Assert.Equal("[\"b\",{\"a\":\"VAL\"}]", JsonSerializer.Serialize(got!.Data));
    }

    [Fact]
    public void Decodes_with_custom_reviver_options_object()
    {
        var decoder = new Decoder(new DecoderOptions
        {
            Reviver = (key, value) => key == "a" && value is string s ? s.ToUpper() : value,
        });
        Packet? got = null;
        decoder.On("decoded", args => got = (Packet)args[0]);
        decoder.Add("2[\"b\",{\"a\":\"val\"}]");
        Assert.Equal("[\"b\",{\"a\":\"VAL\"}]", JsonSerializer.Serialize(got!.Data));
    }

    [Fact]
    public void Throws_on_invalid_payloads()
    {
        void Invalid(string str) =>
            Assert.ThrowsAny<Exception>(() => new Decoder().Add(str));

        Invalid("442[\"some\",\"data\"");
        Invalid("0/admin,\"invalid\"");
        Invalid("0[]");
        Invalid("1/admin,{}");
        Invalid("2/admin,\"invalid");
        Invalid("2/admin,{}");
        Invalid("2[{\"toString\":\"foo\"}]");
        Invalid("2[true,\"foo\"]");
        Invalid("2[null,\"bar\"]");
        Invalid("2[\"connect\"]");
        Invalid("2[\"disconnect\",\"123\"]");

        void IllegalAttachments(string str) =>
            Assert.ThrowsAny<Exception>(() => new Decoder().Add(str));
        IllegalAttachments("5");
        IllegalAttachments("51");
        IllegalAttachments("5a-");
        IllegalAttachments("51.23-");

        Assert.ThrowsAny<Exception>(() => new Decoder().Add("999"));
        Assert.ThrowsAny<Exception>(() => new Decoder().Add(999!));
    }

    [Fact]
    public void Resumes_decoding_after_destroy()
    {
        var decoder = new Decoder();
        Packet? got = null;
        decoder.On("decoded", args => got = (Packet)args[0]);
        decoder.Add("51-[\"hello\"]");
        Assert.Null(got); // waiting for buffer
        decoder.Destroy();
        decoder.Add("2[\"hello\"]");
        Assert.NotNull(got);
        Assert.Equal("[\"hello\"]", JsonSerializer.Serialize(got!.Data));
    }

    [Fact]
    public void IsPacketValid_rules()
    {
        Assert.True(SocketIoParser.IsPacketValid(new Packet { Type = PacketType.Connect, Nsp = "/" }));
        Assert.False(SocketIoParser.IsPacketValid(new Packet { Type = PacketType.Connect, Nsp = "/admin", Data = "invalid" }));
        Assert.False(SocketIoParser.IsPacketValid(new Packet { Type = PacketType.Connect, Nsp = "/", Data = new object[] { } }));
        Assert.False(SocketIoParser.IsPacketValid(new Packet { Type = PacketType.Disconnect, Nsp = "/admin", Data = new Dictionary<string, object>() }));
        Assert.False(SocketIoParser.IsPacketValid(new Packet { Type = PacketType.Event, Nsp = "/admin", Data = "invalid" }));
        Assert.False(SocketIoParser.IsPacketValid(new Packet { Type = PacketType.Event, Nsp = "/admin", Data = new Dictionary<string, object>() }));
        Assert.False(SocketIoParser.IsPacketValid(new Packet { Type = PacketType.Event, Nsp = "/", Data = new Dictionary<string, object> { ["toString"] = "foo" } }));
        Assert.False(SocketIoParser.IsPacketValid(new Packet { Type = PacketType.Event, Nsp = "/", Data = new object[] { true, "foo" } }));
        Assert.False(SocketIoParser.IsPacketValid(new Packet { Type = PacketType.Event, Nsp = "/", Data = new object?[] { null, "bar" } }));
        Assert.False(SocketIoParser.IsPacketValid(new Packet { Type = PacketType.Event, Nsp = "/", Data = new object[] { "connect" } }));
        Assert.False(SocketIoParser.IsPacketValid(new Packet { Type = PacketType.Event, Nsp = "/", Data = new object[] { "disconnect", "123" } }));
    }
}
