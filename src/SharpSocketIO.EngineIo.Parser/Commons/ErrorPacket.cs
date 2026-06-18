namespace SharpSocketIO.EngineIo.Parser.Commons;

/// <summary>Port of ERROR_PACKET = { type: "error", data: "parser error" }.</summary>
public static class ErrorPacket
{
    public static readonly Packet Instance = new(PacketType.Error, new RawData("parser error"));
}
