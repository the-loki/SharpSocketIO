using System.Text;

namespace SharpSocketIO.EngineIo.Client;

/// <summary>Port of lib/util.ts (byteLength + installTimerFunctions stub).</summary>
public static class Util
{
    public static int ByteLength(string str) => Encoding.UTF8.GetByteCount(str);
}
