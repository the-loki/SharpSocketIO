namespace SharpSocketIO.SocketIo.Parser.Commons;

/// <summary>socket.io protocol packet type (protocol v5).</summary>
public enum PacketType
{
    Connect = 0,
    Disconnect = 1,
    Event = 2,
    Ack = 3,
    ConnectError = 4,
    BinaryEvent = 5,
    BinaryAck = 6,
}
