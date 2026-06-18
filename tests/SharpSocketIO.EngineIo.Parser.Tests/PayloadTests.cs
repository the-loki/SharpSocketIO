using SharpSocketIO.EngineIo.Parser.Commons;
using Xunit;

namespace SharpSocketIO.EngineIo.Parser.Tests;

public class PayloadTests
{
    private static readonly string Sep = ((char)30).ToString();

    [Fact]
    public void Encodes_and_decodes_all_packet_types()
    {
        var packets = new[]
        {
            new Packet(PacketType.Open),
            new Packet(PacketType.Close),
            new Packet(PacketType.Ping, new RawData("probe")),
            new Packet(PacketType.Pong, new RawData("probe")),
            new Packet(PacketType.Message, new RawData("test")),
        };
        var payload = EngineIoParser.EncodePayload(packets);
        Assert.Equal("0" + Sep + "1" + Sep + "2probe" + Sep + "3probe" + Sep + "4test", payload);
        Assert.Equal(packets, EngineIoParser.DecodePayload(payload));
    }

    [Fact]
    public void Encodes_string_plus_buffer_payload_as_base64()
    {
        var packets = new[]
        {
            new Packet(PacketType.Message, new RawData("test")),
            new Packet(PacketType.Message, new RawData(new byte[] { 1, 2, 3, 4 })),
        };
        var payload = EngineIoParser.EncodePayload(packets);
        Assert.Equal("4test" + Sep + "bAQIDBA==", payload);
        Assert.Equal(packets, EngineIoParser.DecodePayload(payload, BinaryType.NodeBuffer));
    }

    [Fact]
    public void Fails_to_decode_malformed_payload_brace()
    {
        var result = EngineIoParser.DecodePayload("{");
        Assert.Equal(new[] { ErrorPacket.Instance }, result);
    }

    [Fact]
    public void Fails_to_decode_malformed_payload_braces()
    {
        var result = EngineIoParser.DecodePayload("{}");
        Assert.Equal(new[] { ErrorPacket.Instance }, result);
    }

    [Fact]
    public void Fails_to_decode_malformed_payload_array_string()
    {
        var result = EngineIoParser.DecodePayload("[\"a123\", \"a456\"]");
        Assert.Equal(new[] { ErrorPacket.Instance }, result);
    }

    [Fact]
    public void Protocol_is_4()
    {
        Assert.Equal(4, EngineIoParser.Protocol);
    }
}
