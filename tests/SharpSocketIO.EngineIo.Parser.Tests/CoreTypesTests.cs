using SharpSocketIO.EngineIo.Parser.Commons;
using Xunit;

namespace SharpSocketIO.EngineIo.Parser.Tests;

public class CoreTypesTests
{
    [Fact]
    public void Packet_equality_is_value_based()
    {
        var a = new Packet(PacketType.Message, new RawData("test"));
        var b = new Packet(PacketType.Message, new RawData("test"));
        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    [Fact]
    public void Packet_with_byte_array_data_is_equal_by_content()
    {
        var a = new Packet(PacketType.Message, new RawData(new byte[] { 1, 2, 3 }));
        var b = new Packet(PacketType.Message, new RawData(new byte[] { 1, 2, 3 }));
        Assert.Equal(a, b);
    }

    [Fact]
    public void PacketTypeMap_round_trips_wire_codes()
    {
        Assert.Equal("4", PacketTypeMap.Code(PacketType.Message));
        Assert.Equal(PacketType.Message, PacketTypeMap.FromCode("4"));
    }

    [Fact]
    public void PacketTypeMap_rejects_unknown_codes()
    {
        Assert.False(PacketTypeMap.IsKnownCode("8"));
        Assert.False(PacketTypeMap.IsKnownCode("{"));
    }
}
