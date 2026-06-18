namespace SharpSocketIO.EngineIo.Commons;

/// <summary>Socket/transport ready-state (mirrors JS union of string literals).</summary>
public enum ReadyState
{
    Opening,
    Open,
    Closing,
    Closed,
}
