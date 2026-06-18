using System.Collections.Generic;

namespace SharpSocketIO.SocketIo.Adapter;

/// <summary>A persisted session for connection-state recovery.</summary>
public sealed class Session
{
    public Session(string sid, string pid, IReadOnlyList<string> rooms, object? data, IReadOnlyList<object[]> missedPackets)
    {
        Sid = sid; Pid = pid; Rooms = rooms; Data = data; MissedPackets = missedPackets;
    }

    public string Sid { get; set; }
    public string Pid { get; }
    public IReadOnlyList<string> Rooms { get; }
    public object? Data { get; }
    public IReadOnlyList<object[]> MissedPackets { get; }

    /// <summary>Returns a copy with a new Sid (mirrors the JS `with`-style restore).</summary>
    public Session WithSid(string newSid) => new(newSid, Pid, Rooms, Data, MissedPackets);
}

/// <summary>
/// Port of SessionAwareAdapter. Tracks private sessions (pid) for connection-state
/// recovery. Minimal for cycle 4: in-memory persist/restore stubs that later cycles
/// (or cluster adapters) can back with real storage.
/// </summary>
public class SessionAwareAdapter : Adapter
{
    private readonly Dictionary<string, Session> _sessions = new();

    public SessionAwareAdapter(IAdapterNamespace nsp) : base(nsp) { }

    public void PersistSession(Session session) => _sessions[session.Pid] = session;

    public Session? RestoreSession(string pid, string sid) =>
        _sessions.TryGetValue(pid, out var s) ? s.WithSid(sid) : null;
}
