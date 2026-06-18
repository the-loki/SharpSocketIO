using System.Text;
using SharpSocketIO.EngineIo.ParserV3;
using Xunit;

namespace SharpSocketIO.EngineIo.Tests;

// Port of test/parser.js (single mixed-payload test) + direct encode/decode parity cases.
public class ParserV3Tests
{
    private static byte[] Buf(params byte[] b) => b;

    [Fact]
    public void Properly_encodes_a_mixed_payload()
    {
        object? encoded = null;
        ParserV3Codec.EncodePayload(
            new[]
            {
                new V3Packet { Type = V3PacketType.Message, Data = "€€€€" },
                new V3Packet { Type = V3PacketType.Message, Data = Buf(1, 2, 3) },
            },
            supportsBinary: true,
            callback: e => encoded = e);

        Assert.NotNull(encoded);
        var bytes = (byte[])encoded!;

        V3Packet? first = null;
        ParserV3Codec.DecodePayload(bytes, (packet, index, total) =>
        {
            if (index == 0) first = packet;
            return true;
        });

        Assert.NotNull(first);
        Assert.Equal("€€€€", first!.Data);
    }

    [Fact]
    public void Encodes_and_decodes_a_string_packet()
    {
        string encoded = string.Empty;
        ParserV3Codec.EncodePacket(
            new V3Packet { Type = V3PacketType.Message, Data = "hello" },
            supportsBinary: false,
            callback: e => encoded = (string)e);

        Assert.Equal("4hello", encoded);

        V3Packet? decoded = null;
        ParserV3Codec.DecodePacket(encoded, false, p => decoded = p);
        Assert.NotNull(decoded);
        Assert.Equal(V3PacketType.Message, decoded!.Type);
        Assert.Equal("hello", decoded.Data);
    }

    [Fact]
    public void Encodes_and_decodes_a_binary_packet()
    {
        object encoded = new object();
        ParserV3Codec.EncodePacket(
            new V3Packet { Type = V3PacketType.Message, Data = Buf(1, 2, 3, 4) },
            supportsBinary: true,
            callback: e => encoded = e);

        Assert.IsType<byte[]>(encoded);
        V3Packet? decoded = null;
        ParserV3Codec.DecodePacket((byte[])encoded, true, p => decoded = p);
        Assert.Equal(Buf(1, 2, 3, 4), (byte[])decoded!.Data!);
    }

    [Fact]
    public void Encodes_and_decodes_a_string_payload()
    {
        object? encoded = null;
        ParserV3Codec.EncodePayload(
            new[]
            {
                new V3Packet { Type = V3PacketType.Message, Data = "a" },
                new V3Packet { Type = V3PacketType.Ping },
            },
            supportsBinary: false,
            callback: e => encoded = e);

        Assert.IsType<string>(encoded);
        var str = (string)encoded!;

        var received = new System.Collections.Generic.List<V3Packet>();
        ParserV3Codec.DecodePayload(str, (packet, index, total) =>
        {
            received.Add(packet);
            return true;
        });

        Assert.Equal(2, received.Count);
        Assert.Equal(V3PacketType.Message, received[0].Type);
        Assert.Equal("a", received[0].Data);
        Assert.Equal(V3PacketType.Ping, received[1].Type);
    }

    [Fact]
    public void Decodes_a_base64_packet()
    {
        // Encode binary as base64 then decode
        string encoded = string.Empty;
        ParserV3Codec.EncodePacket(
            new V3Packet { Type = V3PacketType.Message, Data = Buf(1, 2, 3) },
            supportsBinary: false,
            callback: e => encoded = (string)e);
        // base64-encoded form begins with 'b'
        Assert.StartsWith("b", encoded);

        // DecodeBase64Packet
        V3Packet? decoded = null;
        ParserV3Codec.DecodeBase64Packet(encoded, p => decoded = p);
        Assert.Equal(V3PacketType.Message, decoded!.Type);
        Assert.Equal(Buf(1, 2, 3), (byte[])decoded.Data!);
    }
}
