using System;

namespace SharpSocketIO.SocketIo.Parser.Commons;

/// <summary>Port of interface DecoderOptions.</summary>
public sealed class DecoderOptions
{
    public Func<string, object?, object?>? Reviver { get; set; }
    public int MaxAttachments { get; set; } = 10;
}
