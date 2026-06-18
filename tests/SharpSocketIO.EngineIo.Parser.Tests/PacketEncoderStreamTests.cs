using SharpSocketIO.EngineIo.Parser.Commons;
using Xunit;

namespace SharpSocketIO.EngineIo.Parser.Tests;

public class PacketEncoderStreamTests
{
    [Fact]
    public void Encodes_a_plaintext_packet()
    {
        var stream = new PacketEncoderStream();
        stream.Write(new Packet(PacketType.Message, new RawData("1€")));
        Assert.Equal(new byte[] { 5 }, stream.ReadChunk());
        Assert.Equal(new byte[] { 52, 49, 226, 130, 172 }, stream.ReadChunk());
    }

    [Fact]
    public void Encodes_a_binary_packet_byte_array()
    {
        var stream = new PacketEncoderStream();
        var data = new byte[] { 1, 2, 3 };
        stream.Write(new Packet(PacketType.Message, new RawData(data)));
        Assert.Equal(new byte[] { 131 }, stream.ReadChunk()); // 0x03 | 0x80
        Assert.Equal(data, stream.ReadChunk());
    }

    [Fact]
    public void Encodes_a_binary_packet_arraybuffer()
    {
        var stream = new PacketEncoderStream();
        stream.Write(new Packet(PacketType.Message, new RawData(new ArrayBuffer(new byte[] { 1, 2, 3 }))));
        Assert.Equal(new byte[] { 131 }, stream.ReadChunk());
        Assert.Equal(new byte[] { 1, 2, 3 }, stream.ReadChunk());
    }

    [Fact]
    public void Encodes_a_uint16array_view_as_bytes()
    {
        var stream = new PacketEncoderStream();
        // JS Uint16Array.from([1, 2, 257]) → little-endian bytes 01 00 02 00 01 01
        var bytes = new byte[] { 1, 0, 2, 0, 1, 1 };
        stream.Write(new Packet(PacketType.Message, new RawData(bytes)));
        Assert.Equal(new byte[] { 134 }, stream.ReadChunk()); // 6 | 0x80
        Assert.Equal(bytes, stream.ReadChunk());
    }

    [Fact]
    public void Encodes_medium_packet_with_16bit_length()
    {
        var stream = new PacketEncoderStream();
        var data = new byte[12345];
        stream.Write(new Packet(PacketType.Message, new RawData(data)));
        Assert.Equal(new byte[] { 254, 48, 57 }, stream.ReadChunk()); // 126 | 0x80, big-endian 12345
        Assert.Equal(data, stream.ReadChunk());
    }

    [Fact]
    public void Encodes_big_packet_with_64bit_length()
    {
        var stream = new PacketEncoderStream();
        var data = new byte[123456789];
        stream.Write(new Packet(PacketType.Message, new RawData(data)));
        Assert.Equal(new byte[] { 255, 0, 0, 0, 0, 7, 91, 205, 21 }, stream.ReadChunk());
        Assert.Equal(data, stream.ReadChunk());
    }
}
