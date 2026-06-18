using System;
using System.Collections;
using System.Collections.Generic;
using System.Text.Json;
using SharpSocketIO.ComponentEmitter;
using SharpSocketIO.SocketIo.Parser.Commons;

namespace SharpSocketIO.SocketIo.Parser;

/// <summary>Port of class Decoder from lib/index.ts.</summary>
public sealed class Decoder : Emitter<DecodedEvents>
{
    private BinaryReconstructor? _reconstructor;
    private readonly Func<string, object?, object?>? _reviver;
    private readonly int _maxAttachments;

    public Decoder() : this((DecoderOptions?)null) { }

    public Decoder(Func<string, object?, object?>? reviver)
    {
        _reviver = reviver;
        _maxAttachments = 10;
    }

    public Decoder(DecoderOptions? opts)
    {
        _reviver = opts?.Reviver;
        _maxAttachments = opts?.MaxAttachments ?? 10;
    }

    public void Add(object obj)
    {
        if (obj is string s)
        {
            if (_reconstructor != null)
                throw new Exception("got plaintext data when reconstructing a packet");
            var packet = DecodeString(s);
            bool isBinaryEvent = packet.Type == PacketType.BinaryEvent;
            if (isBinaryEvent || packet.Type == PacketType.BinaryAck)
            {
                packet.Type = isBinaryEvent ? PacketType.Event : PacketType.Ack;
                _reconstructor = new BinaryReconstructor(packet);
                if (packet.Attachments == 0)
                {
                    EmitReserved("decoded", packet);
                }
            }
            else
            {
                EmitReserved("decoded", packet);
            }
        }
        else if (IsBinaryHelper.IsBinary(obj))
        {
            if (_reconstructor == null)
                throw new Exception("got binary data when not reconstructing a packet");
            var bytes = obj is byte[] b ? b : ByteArrayFromSegment((ArraySegment<byte>)obj);
            var packet = _reconstructor.TakeBinaryData(bytes);
            if (packet != null)
            {
                _reconstructor = null;
                EmitReserved("decoded", packet);
            }
        }
        else
        {
            throw new Exception("Unknown type: " + obj);
        }
    }

    public void Destroy()
    {
        if (_reconstructor != null)
        {
            _reconstructor.FinishedReconstruction();
            _reconstructor = null;
        }
    }

    private Packet DecodeString(string str)
    {
        int i = 0;
        var p = new Packet { Type = (PacketType)(str[0] - '0') };

        if (!Enum.IsDefined(typeof(PacketType), p.Type))
            throw new Exception("unknown packet type " + (int)p.Type);

        if (p.Type == PacketType.BinaryEvent || p.Type == PacketType.BinaryAck)
        {
            int start = i + 1;
            while (++i < str.Length && str[i] != '-') { }
            var buf = str.Substring(start, i - start);
            if (!int.TryParse(buf, out int n) || str.Length <= i || str[i] != '-')
                throw new Exception("Illegal attachments");
            if (n < 0) throw new Exception("Illegal attachments");
            if (n > _maxAttachments) throw new Exception("too many attachments");
            p.Attachments = n;
        }

        if (i + 1 < str.Length && str[i + 1] == '/')
        {
            int start = i + 1;
            while (++i < str.Length && str[i] != ',') { }
            p.Nsp = str.Substring(start, i - start);
        }
        else
        {
            p.Nsp = "/";
        }

        if (i + 1 < str.Length)
        {
            char next = str[i + 1];
            if (char.IsDigit(next))
            {
                int start = i + 1;
                while (++i < str.Length && char.IsDigit(str[i])) { }
                i--; // back up so the JSON check below increments to the char after the id
                p.Id = int.Parse(str.Substring(start, i - start + 1));
            }
        }

        if (++i < str.Length)
        {
            var payloadStr = str.Substring(i);
            var payload = TryParse(payloadStr);
            if (payload != null && IsPayloadValid(p.Type, payload))
            {
                p.Data = payload;
            }
            else
            {
                throw new Exception("invalid payload");
            }
        }

        return p;
    }

    private object? TryParse(string str)
    {
        try
        {
            var element = JsonSerializer.Deserialize<JsonElement>(str, JsonOptions.Default);
            return ApplyReviver(ConvertElement(element));
        }
        catch (JsonException)
        {
            return null;
        }
    }

    // Convert JsonElement to plain CLR shapes so reviver & binary deconstruct see them.
    private static object? ConvertElement(JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Undefined:
            case JsonValueKind.Null:
                return null;
            case JsonValueKind.True:
                return true;
            case JsonValueKind.False:
                return false;
            case JsonValueKind.Number:
                if (element.TryGetInt64(out long l))
                    return l > int.MaxValue || l < int.MinValue ? (object)l : (object)(int)l;
                return element.GetDouble();
            case JsonValueKind.String:
                return element.GetString();
            case JsonValueKind.Array:
            {
                var list = new List<object?>();
                foreach (var item in element.EnumerateArray()) list.Add(ConvertElement(item));
                return list;
            }
            case JsonValueKind.Object:
            {
                var dict = new Dictionary<string, object?>();
                foreach (var prop in element.EnumerateObject()) dict[prop.Name] = ConvertElement(prop.Value);
                return dict;
            }
            default:
                return null;
        }
    }

    private object? ApplyReviver(object? value)
    {
        if (_reviver == null) return value;
        return ApplyReviverRecursive("", value);
    }

    private object? ApplyReviverRecursive(string key, object? value)
    {
        if (value is IDictionary dict)
        {
            var keys = new List<string>();
            foreach (DictionaryEntry kv in dict) keys.Add((string)kv.Key);
            foreach (var k in keys)
            {
                dict[k] = ApplyReviverRecursive(k, dict[k]);
            }
        }
        else if (value is IList list)
        {
            for (int idx = 0; idx < list.Count; idx++)
            {
                list[idx] = ApplyReviverRecursive("", list[idx]);
            }
        }
        return _reviver!(key, value);
    }

    private static bool IsPayloadValid(PacketType type, object? payload)
    {
        switch (type)
        {
            case PacketType.Connect:
                return IsPlainObject(payload);
            case PacketType.Disconnect:
                return payload == null;
            case PacketType.ConnectError:
                return payload is string || IsPlainObject(payload);
            case PacketType.Event:
            case PacketType.BinaryEvent:
                return payload is IList arr && arr.Count > 0 &&
                       (arr[0] is int || arr[0] is long ||
                        (arr[0] is string s0 && !ReservedEvents.Contains(s0)));
            case PacketType.Ack:
            case PacketType.BinaryAck:
                return payload is IList;
            default:
                return false;
        }
    }

    private static bool IsPlainObject(object? obj) => obj is IDictionary<string, object?>;

    private static byte[] ByteArrayFromSegment(ArraySegment<byte> seg)
    {
        var copy = new byte[seg.Count];
        Array.Copy(seg.Array!, seg.Offset, copy, 0, seg.Count);
        return copy;
    }

    private sealed class BinaryReconstructor
    {
        private readonly Packet _reconPack;
        private readonly List<byte[]> _buffers = new();

        public BinaryReconstructor(Packet packet) { _reconPack = packet; }

        public Packet? TakeBinaryData(byte[] binData)
        {
            _buffers.Add(binData);
            if (_buffers.Count == _reconPack.Attachments)
            {
                var packet = BinaryPacket.ReconstructPacket(_reconPack, _buffers);
                FinishedReconstruction();
                return packet;
            }
            return null;
        }

        public void FinishedReconstruction()
        {
            _buffers.Clear();
        }
    }
}
