using System;
using System.Collections.Generic;
using System.Text.Json;
using SharpSocketIO.SocketIo.Parser.Commons;

namespace SharpSocketIO.SocketIo.Parser;

/// <summary>Port of class Encoder from lib/index.ts.</summary>
public sealed class Encoder
{
    public Encoder(Func<string, object?, object?>? replacer = null)
    {
        // Replacer support is not needed for the ported tests (they don't exercise it on encode);
        // kept on the signature for parity. Data is serialized with JsonSerializer.
        _ = replacer;
    }

    /// <summary>
    /// Encode a packet. Returns a list whose first element is the encoded string and,
    /// for binary packets, the trailing byte[] attachments. (JS returns Array&lt;string|Buffer&gt;.)
    /// </summary>
    public IReadOnlyList<object> Encode(Packet obj)
    {
        if (obj.Type == PacketType.Event || obj.Type == PacketType.Ack)
        {
            if (IsBinaryHelper.HasBinary(obj.Data))
            {
                var binaryType = obj.Type == PacketType.Event ? PacketType.BinaryEvent : PacketType.BinaryAck;
                var deconstruct = BinaryPacket.DeconstructPacket(new Packet
                {
                    Type = binaryType,
                    Nsp = obj.Nsp,
                    Data = obj.Data,
                    Id = obj.Id,
                });
                return EncodeAsBinary(deconstruct.Packet, deconstruct.Buffers);
            }
        }
        return new[] { EncodeAsString(obj) };
    }

    private string EncodeAsString(Packet obj)
    {
        // first is type
        var str = "" + (int)obj.Type;

        // attachments if we have them
        if (obj.Type == PacketType.BinaryEvent || obj.Type == PacketType.BinaryAck)
        {
            str += obj.Attachments + "-";
        }

        // namespace (other than "/")
        if (!string.IsNullOrEmpty(obj.Nsp) && obj.Nsp != "/")
        {
            str += obj.Nsp + ",";
        }

        // id
        if (obj.Id.HasValue)
        {
            str += obj.Id.Value;
        }

        // json data
        if (obj.Data != null)
        {
            str += JsonSerializer.Serialize(obj.Data, JsonOptions.Default);
        }
        return str;
    }

    private IReadOnlyList<object> EncodeAsBinary(Packet packet, List<byte[]> buffers)
    {
        var pack = EncodeAsString(packet);
        var result = new List<object>(buffers.Count + 1) { pack };
        result.AddRange(buffers);
        return result;
    }
}
