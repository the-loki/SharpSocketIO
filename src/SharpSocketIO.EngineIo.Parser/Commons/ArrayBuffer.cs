namespace SharpSocketIO.EngineIo.Parser.Commons;

/// <summary>
/// .NET analogue of a JS ArrayBuffer / typed-array-with-offset. Wraps a byte
/// buffer plus a byte offset and length, mirroring the cases the upstream
/// tests exercise (e.g. Int8Array(buffer, 1, 2)).
/// </summary>
public readonly struct ArrayBuffer
{
    public ArrayBuffer(byte[] buffer) : this(buffer, 0, buffer.Length) { }

    public ArrayBuffer(byte[] buffer, int byteOffset, int byteLength)
    {
        Buffer = buffer;
        ByteOffset = byteOffset;
        ByteLength = byteLength;
    }

    public byte[] Buffer { get; }
    public int ByteOffset { get; }
    public int ByteLength { get; }

    /// <summary>Returns a copy of the addressed byte range as a standalone array.</summary>
    public byte[] ToArray()
    {
        var copy = new byte[ByteLength];
        System.Array.Copy(Buffer, ByteOffset, copy, 0, ByteLength);
        return copy;
    }

    /// <summary>JS ArrayBuffer.prototype.slice(start, end) equivalent.</summary>
    public ArrayBuffer Slice(int start, int end)
    {
        int len = end - start;
        var copy = new byte[len];
        System.Array.Copy(Buffer, ByteOffset + start, copy, 0, len);
        return new ArrayBuffer(copy);
    }

    public bool BytesEqual(ArrayBuffer other)
    {
        if (ByteLength != other.ByteLength) return false;
        for (int i = 0; i < ByteLength; i++)
            if (Buffer[ByteOffset + i] != other.Buffer[other.ByteOffset + i]) return false;
        return true;
    }
}
