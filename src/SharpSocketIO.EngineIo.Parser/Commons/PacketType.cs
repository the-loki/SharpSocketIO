namespace SharpSocketIO.EngineIo.Parser.Commons;

/// <summary>Engine.io packet type. Numeric values are the on-wire codes (0–6).</summary>
public enum PacketType
{
    Open = 0,
    Close = 1,
    Ping = 2,
    Pong = 3,
    Message = 4,
    Upgrade = 5,
    Noop = 6,
    Error = 7,
}
