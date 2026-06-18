using SharpSocketIO.EngineIo.Parser.Commons;
using Xunit;

namespace SharpSocketIO.EngineIo.Parser.Tests;

// Port of test/browser.ts — ArrayBuffer cases only. Blob has no .NET primitive
// (design spec §6.1); the four Blob-only upstream tests are intentionally omitted.
public class BrowserTests
{
    private static readonly string Sep = ((char)30).ToString();

    [Fact]
    public void Encodes_and_decodes_an_arraybuffer()
    {
        var data = TestUtil.CreateArrayBuffer(1, 2, 3, 4);
        var packet = new Packet(PacketType.Message, new RawData(data));
        RawData encoded = default;
        EncodePacket.Encode(packet, true, r => encoded = r);
        Assert.True(TestUtil.AreArraysEqual(data, encoded.AsArrayBuffer()!.Value));
        var decoded = DecodePacket.Decode(encoded, BinaryType.ArrayBuffer);
        Assert.Equal(PacketType.Message, decoded.Type);
        Assert.True(TestUtil.AreArraysEqual(data, decoded.Data!.Value.AsArrayBuffer()!.Value));
    }

    [Fact]
    public void Encodes_and_decodes_an_arraybuffer_as_base64()
    {
        var data = TestUtil.CreateArrayBuffer(1, 2, 3, 4);
        var packet = new Packet(PacketType.Message, new RawData(data));
        RawData encoded = default;
        EncodePacket.Encode(packet, false, r => encoded = r);
        Assert.Equal("bAQIDBA==", encoded.AsString());
        var decoded = DecodePacket.Decode(encoded, BinaryType.ArrayBuffer);
        Assert.Equal(PacketType.Message, decoded.Type);
        Assert.True(TestUtil.AreArraysEqual(data, decoded.Data!.Value.AsArrayBuffer()!.Value));
    }

    [Fact]
    public void Encodes_a_string_plus_arraybuffer_payload()
    {
        var packets = new[]
        {
            new Packet(PacketType.Message, new RawData("test")),
            new Packet(PacketType.Message, new RawData(TestUtil.CreateArrayBuffer(1, 2, 3, 4))),
        };
        var payload = EngineIoParser.EncodePayload(packets);
        Assert.Equal("4test" + Sep + "bAQIDBA==", payload);
        var decoded = EngineIoParser.DecodePayload(payload, BinaryType.ArrayBuffer);
        Assert.Equal(2, decoded.Count);
        Assert.Equal(packets[0], decoded[0]);
        Assert.Equal(PacketType.Message, decoded[1].Type);
        Assert.True(TestUtil.AreArraysEqual(TestUtil.CreateArrayBuffer(1, 2, 3, 4),
            decoded[1].Data!.Value.AsArrayBuffer()!.Value));
    }

    [Fact]
    public void Encodes_a_string_plus_zero_length_arraybuffer_payload()
    {
        var packets = new[]
        {
            new Packet(PacketType.Message, new RawData("test")),
            new Packet(PacketType.Message, new RawData(TestUtil.CreateArrayBuffer())),
        };
        var payload = EngineIoParser.EncodePayload(packets);
        Assert.Equal("4test" + Sep + "b", payload);
        Assert.Equal(packets, EngineIoParser.DecodePayload(payload, BinaryType.ArrayBuffer));
    }
}
