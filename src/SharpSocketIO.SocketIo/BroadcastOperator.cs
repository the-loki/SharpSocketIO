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
        var encodedParts = encoder.Encode(packet);
        // For in-process broadcast: deliver all encoded parts (header + binary attachments)
        // to each matching socket via the namespace.
        var opts = new BroadcastOptions
        {
            Rooms = _rooms,
            Except = _except.Count == 0 ? null : _except,
            Flags = _flags,
        };
        // Find matching sockets and send all parts (preserves binary attachments)
        foreach (var sid in MatchSocketsForBroadcast(opts))
        {
            _nsp.SendPartsToSocket(sid, encodedParts);
        }
        return Task.CompletedTask;
    }

    private IEnumerable<string> MatchSocketsForBroadcast(BroadcastOptions opts)
    {
        var rooms = opts.Rooms;
        var except = opts.Except ?? new HashSet<string>();
        var matched = new HashSet<string>();
        if (rooms == null || rooms.Count == 0)
        {
            foreach (var sid in _adapter.Sids.Keys) matched.Add(sid);
        }
        else
        {
            foreach (var r in rooms)
                if (_adapter.Rooms.TryGetValue(r, out var set))
                    foreach (var sid in set) matched.Add(sid);
        }
        // Expand except rooms to their socket members
        foreach (var exRoom in except)
            if (_adapter.Rooms.TryGetValue(exRoom, out var exSet))
                foreach (var sid in exSet) matched.Remove(sid);
        return matched;
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
