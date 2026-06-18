using System.Collections;
using System.Collections.Generic;
using SharpSocketIO.SocketIo.Parser.Commons;

namespace SharpSocketIO.SocketIo.Parser;

/// <summary>Port of lib/binary.ts (deconstructPacket / reconstructPacket).</summary>
public static class BinaryPacket
{
    /// <summary>Replaces every byte[] in packet.Data with a numbered placeholder; returns buffers.</summary>
    public static (Packet Packet, List<byte[]> Buffers) DeconstructPacket(Packet packet)
    {
        var buffers = new List<byte[]>();
        packet.Data = DeconstructValue(packet.Data, buffers);
        packet.Attachments = buffers.Count;
        return (packet, buffers);
    }

    private static object? DeconstructValue(object? data, List<byte[]> buffers)
    {
        if (data is null) return null;
        if (IsBinaryHelper.IsBinary(data))
        {
            var bytes = data is byte[] b ? b : ByteArrayFromSegment((ArraySegment<byte>)data);
            buffers.Add(bytes);
            return new Dictionary<string, object?>
            {
                ["_placeholder"] = true,
                ["num"] = buffers.Count - 1,
            };
        }
        if (data is IDictionary dict)
        {
            var result = new Dictionary<string, object?>();
            foreach (DictionaryEntry kv in dict)
                result[(string)kv.Key] = DeconstructValue(kv.Value, buffers);
            return result;
        }
        if (data is IList list)
        {
            var result = new List<object?>();
            foreach (var item in list) result.Add(DeconstructValue(item, buffers));
            return result;
        }
        return data;
    }

    /// <summary>Replaces placeholders in packet.Data with the supplied buffers.</summary>
    public static Packet ReconstructPacket(Packet packet, IReadOnlyList<byte[]> buffers)
    {
        packet.Data = ReconstructValue(packet.Data, buffers);
        packet.Attachments = null;
        return packet;
    }

    private static object? ReconstructValue(object? data, IReadOnlyList<byte[]> buffers)
    {
        if (data is null) return null;
        if (data is IDictionary dict)
        {
            if (dict.Contains("_placeholder") && dict["_placeholder"] is bool b && b)
            {
                // JS: typeof data.num === "number" && num >= 0 && num < buffers.length
                var raw = dict["num"];
                int num;
                switch (raw)
                {
                    case int i: num = i; break;
                    case long l: num = (int)l; break;
                    default:
                        throw new System.InvalidOperationException("illegal attachments");
                }
                if (num < 0 || num >= buffers.Count) throw new System.InvalidOperationException("illegal attachments");
                return buffers[num];
            }
            var result = new Dictionary<string, object?>();
            foreach (DictionaryEntry kv in dict)
                result[(string)kv.Key] = ReconstructValue(kv.Value, buffers);
            return result;
        }
        if (data is IList list)
        {
            var result = new List<object?>();
            foreach (var item in list) result.Add(ReconstructValue(item, buffers));
            return result;
        }
        return data;
    }

    private static byte[] ByteArrayFromSegment(ArraySegment<byte> seg)
    {
        var copy = new byte[seg.Count];
        System.Array.Copy(seg.Array!, seg.Offset, copy, 0, seg.Count);
        return copy;
    }
}
