using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using SharpSocketIO.SocketIo.Adapter;
using SharpSocketIO.SocketIo.Parser;
using SharpSocketIO.SocketIo.Parser.Commons;
using SiPacketType = SharpSocketIO.SocketIo.Parser.Commons.PacketType;
using AdapterType = SharpSocketIO.SocketIo.Adapter.Adapter;

namespace SharpSocketIO.SocketIo;

/// <summary>
/// Port of lib/broadcast-operator.ts. Immutable builder: To/In/Except/Compress/Volatile/Local
/// return new instances; Emit encodes an EVENT packet and broadcasts via the Adapter.
/// </summary>
public sealed class BroadcastOperator
{
    private readonly AdapterType _adapter;
    private readonly HashSet<string> _rooms;
    private readonly HashSet<string> _except;
    private readonly BroadcastFlags _flags;
    private readonly Namespace _nsp;

    public BroadcastOperator(Namespace nsp, AdapterType adapter)
        : this(nsp, adapter, new HashSet<string>(), new HashSet<string>(), new BroadcastFlags()) { }

    private BroadcastOperator(Namespace nsp, AdapterType adapter, HashSet<string> rooms, HashSet<string> except, BroadcastFlags flags)
    {
        _nsp = nsp;
        _adapter = adapter;
        _rooms = rooms;
        _except = except;
        _flags = flags;
    }

    public BroadcastOperator To(string room) => To(new[] { room });
    public BroadcastOperator To(IEnumerable<string> rooms)
    {
        var next = new HashSet<string>(_rooms);
        foreach (var r in rooms) next.Add(r);
        return new BroadcastOperator(_nsp, _adapter, next, _except, CloneFlags());
    }
    public BroadcastOperator In(string room) => To(room);

    public BroadcastOperator Except(string room) => Except(new[] { room });
    public BroadcastOperator Except(IEnumerable<string> rooms)
    {
        var next = new HashSet<string>(_except);
        foreach (var r in rooms) next.Add(r);
        return new BroadcastOperator(_nsp, _adapter, _rooms, next, CloneFlags());
    }

    public BroadcastOperator Compress(bool compress)
    {
        var flags = CloneFlags(); flags.Compress = compress;
        return new BroadcastOperator(_nsp, _adapter, _rooms, _except, flags);
    }

    public BroadcastOperator Volatile()
    {
        var flags = CloneFlags(); flags.Volatile = true;
        return new BroadcastOperator(_nsp, _adapter, _rooms, _except, flags);
    }

    public BroadcastOperator Local()
    {
        var flags = CloneFlags(); flags.Local = true;
        return new BroadcastOperator(_nsp, _adapter, _rooms, _except, flags);
    }

    public Task EmitAsync(string eventName, params object[] args)
    {
        var packet = new Packet
        {
            Type = SiPacketType.Event,
            Nsp = _nsp.Name,
            Data = new[] { eventName }.Concat(args).ToArray(),
        };
        var encoder = _nsp.Server.Encoder;
        string encoded = string.Empty;
        foreach (var part in encoder.Encode(packet))
        {
            if (part is string s) { encoded = s; break; }
        }
        return _adapter.BroadcastAsync(encoded, new BroadcastOptions
        {
            Rooms = _rooms,
            Except = _except.Count == 0 ? null : _except,
            Flags = _flags,
        });
    }

    private BroadcastFlags CloneFlags() => new()
    {
        Volatile = _flags.Volatile,
        Compress = _flags.Compress,
        Local = _flags.Local,
        Broadcast = _flags.Broadcast,
        Binary = _flags.Binary,
    };
}
