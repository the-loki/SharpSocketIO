using System;

namespace SharpSocketIO.EngineIo.Parser.Commons;

/// <summary>Port of interface Packet from lib/commons.ts.</summary>
public sealed class Packet : IEquatable<Packet>
{
    public Packet(PacketType type) { Type = type; Data = null; }
    public Packet(PacketType type, RawData? data) { Type = type; Data = data; }
    public Packet(PacketType type, RawData? data, PacketOptions? options)
    { Type = type; Data = data; Options = options; }

    public PacketType Type { get; }
    public RawData? Data { get; }
    public PacketOptions? Options { get; }

    public bool Equals(Packet? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        if (Type != other.Type) return false;
        return DataEqual(Data, other.Data);
    }

    public override bool Equals(object? obj) => Equals(obj as Packet);

    public override int GetHashCode() => HashCode.Combine(Type, Data);

    public override string ToString()
        => Data is { Kind: not RawDataKind.None } d ? $"{Type}:{d.Value}" : Type.ToString();

    // Mirrors the structural equality the JS tests assert via expect(...).to.eql(packet):
    // two RawData are equal iff same Kind and same byte/string content.
    private static bool DataEqual(RawData? a, RawData? b)
    {
        if (a is null && b is null) return true;
        if (a is null || b is null) return false;
        if (a.Value.Kind != b.Value.Kind) return false;
        return a.Value.Kind switch
        {
            RawDataKind.None => true,
            RawDataKind.String => a.Value.AsString() == b.Value.AsString(),
            RawDataKind.Int32 => (int)a.Value.Value! == (int)b.Value.Value!,
            RawDataKind.ByteArray => BytesEqual(a.Value.AsByteArray()!, b.Value.AsByteArray()!),
            RawDataKind.ArrayBuffer => a.Value.AsArrayBuffer()!.Value.BytesEqual(b.Value.AsArrayBuffer()!.Value),
            _ => false,
        };
    }

    private static bool BytesEqual(byte[] x, byte[] y)
    {
        if (x.Length != y.Length) return false;
        for (int i = 0; i < x.Length; i++) if (x[i] != y[i]) return false;
        return true;
    }
}
