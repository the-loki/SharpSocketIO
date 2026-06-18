using SharpSocketIO.EngineIo.Parser.Commons;

namespace SharpSocketIO.EngineIo.Parser.Tests;

// Port of test/util.ts areArraysEqual + createArrayBuffer.
internal static class TestUtil
{
    public static bool AreArraysEqual(ArrayBuffer x, ArrayBuffer y) => x.BytesEqual(y);

    public static bool AreArraysEqual(byte[] x, byte[] y)
    {
        if (x.Length != y.Length) return false;
        for (int i = 0; i < x.Length; i++) if (x[i] != y[i]) return false;
        return true;
    }

    public static ArrayBuffer CreateArrayBuffer(params int[] bytes)
    {
        var b = new byte[bytes.Length];
        for (int i = 0; i < bytes.Length; i++) b[i] = (byte)bytes[i];
        return new ArrayBuffer(b);
    }
}
