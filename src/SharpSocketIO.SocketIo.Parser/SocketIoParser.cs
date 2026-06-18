using System;
using System.Collections;
using SharpSocketIO.SocketIo.Parser.Commons;

namespace SharpSocketIO.SocketIo.Parser;

public static class SocketIoParser
{
    public const int Protocol = 5;

    /// <summary>Port of isPacketValid: namespace + ack id + data validation.</summary>
    public static bool IsPacketValid(Packet packet) =>
        IsNamespaceValid(packet.Nsp) && IsAckIdValid(packet.Id) && IsDataValid(packet.Type, packet.Data);

    private static bool IsNamespaceValid(string? nsp) => nsp is string;

    private static bool IsAckIdValid(int? id) => !id.HasValue || IsInteger(id.Value);

    private static bool IsInteger(double v) => !double.IsInfinity(v) && Math.Floor(v) == v;

    private static bool IsDataValid(PacketType type, object? payload)
    {
        switch (type)
        {
            case PacketType.Connect:
                return payload == null || IsPlainObject(payload);
            case PacketType.Disconnect:
                return payload == null;
            case PacketType.Event:
                return payload is IList arr && arr.Count > 0 &&
                       (arr[0] is int || arr[0] is long ||
                        (arr[0] is string s0 && !ReservedEvents.Contains(s0)));
            case PacketType.Ack:
                return payload is IList;
            case PacketType.ConnectError:
                return payload is string || IsPlainObject(payload);
            default:
                return false;
        }
    }

    private static bool IsPlainObject(object? obj) => obj is IDictionary<string, object?>;
}
