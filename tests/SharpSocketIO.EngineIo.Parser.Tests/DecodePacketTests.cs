using SharpSocketIO.EngineIo.Parser.Commons;
using Xunit;

namespace SharpSocketIO.EngineIo.Parser.Tests;

public class DecodePacketTests
{
    [Fact]
    public void Decodes_a_string()
    {
        var decoded = DecodePacket.Decode(new RawData("4test"));
        Assert.Equal(new Packet(PacketType.Message, new RawData("test")), decoded);
    }

    [Fact]
    public void Fails_to_decode_empty()
    {
        Assert.Equal(ErrorPacket.Instance, DecodePacket.Decode(new RawData("")));
    }

    [Fact]
    public void Fails_to_decode_malformed()
    {
        Assert.Equal(ErrorPacket.Instance, DecodePacket.Decode(new RawData("a123")));
    }

    [Fact]
    public void Decodes_a_buffer_as_base64_nodebuffer()
    {
        var decoded = DecodePacket.Decode(new RawData("bAQIDBA=="), BinaryType.NodeBuffer);
        Assert.Equal(PacketType.Message, decoded.Type);
        Assert.Equal(RawDataKind.ByteArray, decoded.Data!.Value.Kind);
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, decoded.Data!.Value.AsByteArray());
    }

    [Fact]
    public void Decodes_a_buffer_as_base64_arraybuffer()
    {
        var decoded = DecodePacket.Decode(new RawData("bAQIDBA=="), BinaryType.ArrayBuffer);
        Assert.Equal(PacketType.Message, decoded.Type);
        Assert.Equal(RawDataKind.ArrayBuffer, decoded.Data!.Value.Kind);
        Assert.True(decoded.Data!.Value.AsArrayBuffer()!.Value
            .BytesEqual(new ArrayBuffer(new byte[] { 1, 2, 3, 4 })));
    }

    [Fact]
    public void Decodes_a_binary_byte_array_input_as_message()
    {
        var decoded = DecodePacket.Decode(new RawData(new byte[] { 1, 2, 3, 4 }), BinaryType.NodeBuffer);
        Assert.Equal(PacketType.Message, decoded.Type);
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, decoded.Data!.Value.AsByteArray());
    }

    [Fact]
    public void Decodes_packet_with_no_data()
    {
        var decoded = DecodePacket.Decode(new RawData("2"));
        Assert.Equal(new Packet(PacketType.Ping), decoded);
    }

    [Fact]
    public void Decodes_a_base64_zero_length_payload()
    {
        // b + empty base64 → empty byte[] under nodebuffer / empty ArrayBuffer under arraybuffer
        var decoded = DecodePacket.Decode(new RawData("b"), BinaryType.NodeBuffer);
        Assert.Equal(PacketType.Message, decoded.Type);
        byte[] bytes = decoded.Data!.Value.AsByteArray()!;
        Assert.Empty(bytes);
    }
}
