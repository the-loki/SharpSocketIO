using SharpSocketIO.EngineIo.Parser.Commons;
using Xunit;

namespace SharpSocketIO.EngineIo.Parser.Tests;

public class PacketDecoderStreamTests
{
    [Fact]
    public void Decodes_a_plaintext_packet()
    {
        var stream = new PacketDecoderStream(1_000_000, BinaryType.ArrayBuffer);
        stream.Write(new byte[] { 5 });
        stream.Write(new byte[] { 52, 49, 226, 130, 172 });
        Assert.True(stream.TryRead(out var packet));
        Assert.Equal(new Packet(PacketType.Message, new RawData("1€")), packet);
    }

    [Fact]
    public void Decodes_plaintext_byte_by_byte()
    {
        var stream = new PacketDecoderStream(1_000_000, BinaryType.ArrayBuffer);
        stream.Write(new byte[] { 5 });
        stream.Write(new byte[] { 52 });
        stream.Write(new byte[] { 49 });
        stream.Write(new byte[] { 226 });
        stream.Write(new byte[] { 130 });
        stream.Write(new byte[] { 172 });
        stream.Write(new byte[] { 1 });
        stream.Write(new byte[] { 50 }); // ping
        stream.Write(new byte[] { 1 });
        stream.Write(new byte[] { 51 }); // pong

        Assert.True(stream.TryRead(out var first));
        Assert.Equal(new Packet(PacketType.Message, new RawData("1€")), first);
        Assert.True(stream.TryRead(out var ping));
        Assert.Equal(new Packet(PacketType.Ping), ping);
        Assert.True(stream.TryRead(out var pong));
        Assert.Equal(new Packet(PacketType.Pong), pong);
    }

    [Fact]
    public void Decodes_plaintext_all_bytes_at_once()
    {
        var stream = new PacketDecoderStream(1_000_000, BinaryType.ArrayBuffer);
        stream.Write(new byte[] { 5, 52, 49, 226, 130, 172, 1, 50, 1, 51 });
        Assert.True(stream.TryRead(out var first));
        Assert.Equal(new Packet(PacketType.Message, new RawData("1€")), first);
        Assert.True(stream.TryRead(out var ping));
        Assert.Equal(new Packet(PacketType.Ping), ping);
        Assert.True(stream.TryRead(out var pong));
        Assert.Equal(new Packet(PacketType.Pong), pong);
    }

    [Fact]
    public void Decodes_a_binary_arraybuffer()
    {
        var stream = new PacketDecoderStream(1_000_000, BinaryType.ArrayBuffer);
        stream.Write(new byte[] { 131, 1, 2, 3 });
        Assert.True(stream.TryRead(out var packet));
        Assert.Equal(PacketType.Message, packet.Type);
        Assert.Equal(RawDataKind.ArrayBuffer, packet.Data!.Value.Kind);
        Assert.True(packet.Data!.Value.AsArrayBuffer()!.Value.BytesEqual(new ArrayBuffer(new byte[] { 1, 2, 3 })));
    }

    [Fact]
    public void Decodes_a_binary_nodebuffer()
    {
        var stream = new PacketDecoderStream(1_000_000, BinaryType.NodeBuffer);
        stream.Write(new byte[] { 131, 1, 2, 3 });
        Assert.True(stream.TryRead(out var packet));
        Assert.Equal(PacketType.Message, packet.Type);
        Assert.Equal(new byte[] { 1, 2, 3 }, packet.Data!.Value.AsByteArray());
    }

    [Fact]
    public void Decodes_a_binary_medium_packet()
    {
        var stream = new PacketDecoderStream(1_000_000, BinaryType.ArrayBuffer);
        var payload = new byte[12345];
        stream.Write(new byte[] { 254 });
        stream.Write(new byte[] { 48, 57 });
        stream.Write(payload);
        Assert.True(stream.TryRead(out var packet));
        Assert.Equal(PacketType.Message, packet.Type);
        Assert.True(packet.Data!.Value.AsArrayBuffer()!.Value.BytesEqual(new ArrayBuffer(payload)));
    }

    [Fact]
    public void Returns_error_when_payload_exceeds_max()
    {
        var stream = new PacketDecoderStream(10, BinaryType.ArrayBuffer);
        stream.Write(new byte[] { 11 });
        Assert.True(stream.TryRead(out var packet));
        Assert.Equal(ErrorPacket.Instance, packet);
    }

    [Fact]
    public void Returns_error_when_payload_length_is_zero()
    {
        var stream = new PacketDecoderStream(1_000_000, BinaryType.ArrayBuffer);
        stream.Write(new byte[] { 0 });
        Assert.True(stream.TryRead(out var packet));
        Assert.Equal(ErrorPacket.Instance, packet);
    }

    [Fact]
    public void Returns_error_when_length_exceeds_max_safe_integer()
    {
        var stream = new PacketDecoderStream(1_000_000, BinaryType.ArrayBuffer);
        stream.Write(new byte[] { 255, 1, 0, 0, 0, 0, 0, 0, 0, 0 });
        Assert.True(stream.TryRead(out var packet));
        Assert.Equal(ErrorPacket.Instance, packet);
    }
}
