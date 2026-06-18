namespace SharpSocketIO.SocketIo.Parser.Commons;

/// <summary>Port of interface Packet from lib/index.ts.</summary>
public sealed class Packet
{
    public PacketType Type { get; set; }
    public string Nsp { get; set; } = "/";
    public object? Data { get; set; }
    public int? Id { get; set; }
    public int? Attachments { get; set; }
}
