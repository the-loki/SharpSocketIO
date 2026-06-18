namespace SharpSocketIO.EngineIo.Parser.Commons;

/// <summary>Port of Packet.options (compress, wsPreEncoded, wsPreEncodedFrame).</summary>
public sealed class PacketOptions
{
    public bool Compress { get; set; }
    public string? WsPreEncoded { get; set; }
    public byte[]? WsPreEncodedFrame { get; set; }
}
