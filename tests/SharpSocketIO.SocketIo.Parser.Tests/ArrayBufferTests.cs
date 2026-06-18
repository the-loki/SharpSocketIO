using System.Collections.Generic;
using System.Text.Json;
using SharpSocketIO.SocketIo.Parser;
using SharpSocketIO.SocketIo.Parser.Commons;
using Xunit;

namespace SharpSocketIO.SocketIo.Parser.Tests;

// Port of test/arraybuffer.js. ArrayBuffer/Uint8Array map to byte[]. The
// "Object.create(null)" null-prototype object maps to Dictionary<string,object>.
public class ArrayBufferTests
{
    private static byte[] Ab(int n) => new byte[n];

    private static void TestBin(Packet obj)
    {
        var encoder = new Encoder();
        var encoded = encoder.Encode(obj);
        Packet? decoded = null;
        var decoder = new Decoder();
        decoder.On("decoded", args => decoded = (Packet)args[0]);
        foreach (var part in encoded) decoder.Add(part);
        Assert.NotNull(decoded);
        ParserTests.AssertEqual(obj, decoded!);
    }

    [Fact]
    public void Encodes_an_ArrayBuffer()
    {
        TestBin(new Packet { Type = PacketType.Event, Data = new object[] { "a", Ab(2) }, Id = 0, Nsp = "/" });
    }

    [Fact]
    public void Encodes_an_ArrayBuffer_in_a_dictionary()
    {
        TestBin(new Packet
        {
            Type = PacketType.Event,
            Data = new object[] { "a", new Dictionary<string, object> { ["array"] = Ab(2) } },
            Id = 0,
            Nsp = "/",
        });
    }

    [Fact]
    public void Encodes_a_TypedArray_as_bytes()
    {
        var arr = new byte[] { 0, 1, 2, 3, 4 };
        TestBin(new Packet { Type = PacketType.Event, Data = new object[] { "a", arr }, Id = 0, Nsp = "/" });
    }

    [Fact]
    public void Encodes_ArrayBuffers_deep_in_JSON()
    {
        TestBin(new Packet
        {
            Type = PacketType.Event,
            Data = new object[]
            {
                "a",
                new Dictionary<string, object>
                {
                    ["a"] = "hi",
                    ["b"] = new Dictionary<string, object> { ["why"] = Ab(3) },
                    ["c"] = new Dictionary<string, object> { ["a"] = "bye", ["b"] = new Dictionary<string, object> { ["a"] = Ab(6) } },
                },
            },
            Id = 999,
            Nsp = "/deep",
        });
    }

    [Fact]
    public void Encodes_deep_binary_with_null_values()
    {
        TestBin(new Packet
        {
            Type = PacketType.Event,
            Data = new object[]
            {
                "a",
                new Dictionary<string, object?>
                {
                    ["a"] = "b",
                    ["c"] = 4,
                    ["e"] = new Dictionary<string, object?> { ["g"] = null },
                    ["h"] = Ab(9),
                },
            },
            Nsp = "/",
            Id = 600,
        });
    }

    [Fact]
    public void Does_not_modify_the_input_packet()
    {
        var packet = new Packet
        {
            Type = PacketType.Event,
            Nsp = "/",
            Data = new object[] { "a", new byte[] { 1, 2, 3 }, new byte[] { 4, 5, 6 } },
        };
        var before = JsonSerializer.Serialize(packet.Data);
        var encoder = new Encoder();
        encoder.Encode(packet);
        var after = JsonSerializer.Serialize(packet.Data);
        Assert.Equal(before, after);
    }
}
