using System.Collections.Generic;
using System.Text;
using SharpSocketIO.SocketIo.Parser;
using SharpSocketIO.SocketIo.Parser.Commons;
using Xunit;

namespace SharpSocketIO.SocketIo.Parser.Tests;

// Port of test/buffer.js — Node Buffer maps to byte[] in .NET.
public class BufferTests
{
    private static byte[] Buf(string s) => Encoding.UTF8.GetBytes(s);

    internal static void TestBin(Packet obj)
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
    public void Encodes_a_Buffer()
    {
        TestBin(new Packet { Type = PacketType.Event, Data = new object[] { "a", Buf("abc") }, Id = 23, Nsp = "/cool" });
    }

    [Fact]
    public void Encodes_a_nested_Buffer()
    {
        TestBin(new Packet
        {
            Type = PacketType.Event,
            Data = new object[] { "a", new Dictionary<string, object> { ["b"] = new object[] { "c", Buf("abc") } } },
            Id = 23,
            Nsp = "/cool",
        });
    }

    [Fact]
    public void Encodes_a_binary_ack_with_Buffer()
    {
        TestBin(new Packet
        {
            Type = PacketType.Ack,
            Data = new object[] { "a", Buf("xxx"), new Dictionary<string, object>() },
            Id = 127,
            Nsp = "/back",
        });
    }

    [Fact]
    public void Throws_on_attachment_with_invalid_num_string()
    {
        var decoder = new Decoder();
        Assert.ThrowsAny<Exception>(() =>
        {
            decoder.Add("51-[\"hello\",{\"_placeholder\":true,\"num\":\"splice\"}]");
            decoder.Add(Buf("world"));
        });
    }

    [Fact]
    public void Throws_on_attachment_out_of_bound()
    {
        var decoder = new Decoder();
        Assert.ThrowsAny<Exception>(() =>
        {
            decoder.Add("51-[\"hello\",{\"_placeholder\":true,\"num\":1}]");
            decoder.Add(Buf("world"));
        });
    }

    [Fact]
    public void Throws_on_binary_without_header()
    {
        var decoder = new Decoder();
        Assert.ThrowsAny<Exception>(() => decoder.Add(Buf("world")));
    }

    [Fact]
    public void Throws_on_plaintext_when_reconstructing()
    {
        var decoder = new Decoder();
        Assert.ThrowsAny<Exception>(() =>
        {
            decoder.Add("51-[\"hello\",{\"_placeholder\":true,\"num\":0}]");
            decoder.Add("2[\"hello\"]");
        });
    }
}
