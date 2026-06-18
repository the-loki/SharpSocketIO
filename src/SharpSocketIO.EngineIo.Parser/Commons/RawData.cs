namespace SharpSocketIO.EngineIo.Parser.Commons;

/// <summary>
/// Port of RawData = string | Buffer | ArrayBuffer | ArrayBufferView | Blob.
/// Discriminated wrapper so we can do the equivalent of
/// "data instanceof ArrayBuffer / ArrayBuffer.isView(data)" over a dynamic value.
/// Blob has no .NET primitive (see design spec §6); it is never produced here.
/// </summary>
public readonly struct RawData
{
    public RawData() { Value = null; Kind = RawDataKind.None; }

    public RawData(string value) { Value = value; Kind = RawDataKind.String; }
    public RawData(int value) { Value = value; Kind = RawDataKind.Int32; }
    public RawData(byte[] value) { Value = value; Kind = RawDataKind.ByteArray; }
    public RawData(ArrayBuffer value) { Value = value; Kind = RawDataKind.ArrayBuffer; }

    public object? Value { get; }
    public RawDataKind Kind { get; }

    public bool IsBinary => Kind == RawDataKind.ByteArray || Kind == RawDataKind.ArrayBuffer;

    public bool IsNone => Kind == RawDataKind.None;

    public string? AsString() => Value as string;
    public byte[]? AsByteArray() => Value as byte[];
    public ArrayBuffer? AsArrayBuffer() => Value as ArrayBuffer?;

    public static implicit operator RawData(string? s) => s is null ? default : new RawData(s);
    public static implicit operator RawData(byte[]? b) => b is null ? default : new RawData(b);
    public static implicit operator RawData(ArrayBuffer ab) => new RawData(ab);
    public static implicit operator RawData(int i) => new RawData(i);
}
