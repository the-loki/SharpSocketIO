using SharpSocketIO.EngineIo.Parser.Commons;
using Xunit;

namespace SharpSocketIO.EngineIo.Parser.Tests;

public class EncodePacketTests
{
    private static RawData Encode(Packet p, bool supportsBinary)
    {
        RawData result = default;
        EncodePacket.Encode(p, supportsBinary, r => result = r);
        return result;
    }

    [Fact]
    public void Encodes_a_string()
    {
        var packet = new Packet(PacketType.Message, new RawData("test"));
        var encoded = Encode(packet, true);
        Assert.Equal(RawDataKind.String, encoded.Kind);
        Assert.Equal("4test", encoded.AsString());
    }

    [Fact]
    public void Encodes_a_buffer_as_noop_when_supports_binary()
    {
        var packet = new Packet(PacketType.Message, new RawData(new byte[] { 1, 2, 3, 4 }));
        var encoded = Encode(packet, true);
        Assert.Equal(RawDataKind.ByteArray, encoded.Kind);
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, encoded.AsByteArray());
    }

    [Fact]
    public void Encodes_a_buffer_as_base64_when_no_binary_support()
    {
        var packet = new Packet(PacketType.Message, new RawData(new byte[] { 1, 2, 3, 4 }));
        var encoded = Encode(packet, false);
        Assert.Equal("bAQIDBA==", encoded.AsString());
    }

    [Fact]
    public void Encodes_an_arraybuffer_as_base64_when_no_binary_support()
    {
        var packet = new Packet(PacketType.Message, new RawData(new ArrayBuffer(new byte[] { 1, 2, 3, 4 })));
        var encoded = Encode(packet, false);
        Assert.Equal("bAQIDBA==", encoded.AsString());
    }

    [Fact]
    public void Encodes_a_typed_array_with_offset_and_length()
    {
        var buffer = new byte[] { 1, 2, 3, 4 };
        var view = new ArrayBuffer(buffer, 1, 2); // bytes [2,3]
        var packet = new Packet(PacketType.Message, new RawData(view));
        var encoded = Encode(packet, true);
        Assert.Equal(RawDataKind.ByteArray, encoded.Kind);
        Assert.Equal(new byte[] { 2, 3 }, encoded.AsByteArray());
    }

    [Fact]
    public void Encodes_a_packet_without_data()
    {
        var packet = new Packet(PacketType.Open);
        var encoded = Encode(packet, true);
        Assert.Equal("0", encoded.AsString());
    }

    [Fact]
    public void Encode_to_binary_yields_utf8_of_text_packet()
    {
        var packet = new Packet(PacketType.Message, new RawData("1€"));
        RawData result = default;
        EncodePacket.EncodeToBinary(packet, r => result = r);
        Assert.Equal(new byte[] { 52, 49, 226, 130, 172 }, result.AsByteArray());
    }

    [Fact]
    public void Encode_to_binary_yields_raw_bytes_for_binary_packet()
    {
        var packet = new Packet(PacketType.Message, new RawData(new byte[] { 1, 2, 3 }));
        RawData result = default;
        EncodePacket.EncodeToBinary(packet, r => result = r);
        Assert.Equal(new byte[] { 1, 2, 3 }, result.AsByteArray());
    }
}
