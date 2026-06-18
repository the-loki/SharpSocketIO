using System.Collections.Generic;
using System.Threading.Tasks;
using SharpSocketIO.ComponentEmitter;

namespace SharpSocketIO.SocketIo.Adapter;

/// <summary>
/// Port of socket.io-adapter in-memory Adapter. Maintains room→socketIds and
/// socketId→rooms maps; emits create-room/join-room/leave-room/delete-room.
/// Broadcast is decoupled from Namespace via IAdapterNamespace.Send.
/// </summary>
public class Adapter : Emitter<UnitEvents>
{
    public Dictionary<string, HashSet<string>> Rooms { get; } = new();
    public Dictionary<string, HashSet<string>> Sids { get; } = new();
    protected readonly IAdapterNamespace Nsp;

    public Adapter(IAdapterNamespace nsp) { Nsp = nsp; }
    public Adapter(object nsp) { Nsp = nsp as IAdapterNamespace ?? new ThrowingNamespace(); }

    public virtual void Init() { }
    public virtual void Close() { }
    public virtual int ServerCount() => 1;

    public void AddAll(string id, ISet<string> rooms)
    {
        if (!Sids.ContainsKey(id)) Sids[id] = new HashSet<string>();
        foreach (var room in rooms)
        {
            Sids[id].Add(room);
            if (!Rooms.ContainsKey(room))
            {
                Rooms[room] = new HashSet<string>();
                Emit("create-room", room);
            }
            if (!Rooms[room].Contains(id))
            {
                Rooms[room].Add(id);
                Emit("join-room", room, id);
            }
        }
    }

    public void Add(string id, string room) => AddAll(id, new HashSet<string> { room });

    public void Del(string id, string room)
    {
        if (Sids.ContainsKey(id)) Sids[id].Remove(room);
        DelInternal(room, id);
    }

    private void DelInternal(string room, string id)
    {
        if (!Rooms.TryGetValue(room, out var set)) return;
        if (set.Remove(id))
        {
            Emit("leave-room", room, id);
            if (set.Count == 0)
            {
                Rooms.Remove(room);
                Emit("delete-room", room);
            }
        }
    }

    public void DelAll(string id)
    {
        if (!Sids.TryGetValue(id, out var rooms)) return;
        foreach (var room in new List<string>(rooms)) DelInternal(room, id);
        Sids.Remove(id);
    }

    public bool HasRoom(string room) => Rooms.ContainsKey(room);
    public bool HasSocket(string id) => Sids.ContainsKey(id);

    public Task<IReadOnlyCollection<string>> SocketsAsync(ISet<string> rooms)
    {
        var result = new HashSet<string>();
        foreach (var room in rooms)
        {
            if (Rooms.TryGetValue(room, out var set))
                foreach (var sid in set) result.Add(sid);
        }
        return Task.FromResult<IReadOnlyCollection<string>>(result);
    }

    public Task<IReadOnlyCollection<string>> RoomsAsync(string socketId)
    {
        var result = new HashSet<string>();
        if (Sids.TryGetValue(socketId, out var rooms))
            foreach (var r in rooms) result.Add(r);
        return Task.FromResult<IReadOnlyCollection<string>>(result);
    }

    public virtual Task BroadcastAsync(string packet, BroadcastOptions opts)
    {
        var except = opts.Except ?? new HashSet<string>();
        foreach (var sid in MatchSockets(opts.Rooms, except))
        {
            Nsp.Send(sid, packet);
        }
        return Task.CompletedTask;
    }

    private IEnumerable<string> MatchSockets(ISet<string> rooms, ISet<string> except)
    {
        var matched = new HashSet<string>();
        if (rooms == null || rooms.Count == 0)
        {
            foreach (var sid in Sids.Keys) matched.Add(sid);
        }
        else
        {
            foreach (var room in rooms)
                if (Rooms.TryGetValue(room, out var set))
                    foreach (var sid in set) matched.Add(sid);
        }
        // Expand except rooms: remove every socket that is a member of an except room.
        var exceptSocketIds = new HashSet<string>();
        foreach (var exRoom in except)
        {
            if (Rooms.TryGetValue(exRoom, out var exSet))
                foreach (var sid in exSet) exceptSocketIds.Add(sid);
        }
        foreach (var sid in exceptSocketIds) matched.Remove(sid);
        return matched;
    }

    private sealed class ThrowingNamespace : IAdapterNamespace
    {
        public void Send(string socketId, string packet) =>
            throw new System.NotImplementedException("Namespace does not implement IAdapterNamespace");
    }
}
