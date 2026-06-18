using System.Collections.Generic;

namespace SharpSocketIO.SocketIo.Adapter;

/// <summary>
/// A persisted session for connection-state recovery.
/// </summary>
public sealed record Session(
    string Sid,
    string Pid,
    IReadOnlyList<string> Rooms,
    object? Data,
    IReadOnlyList<object[]> MissedPackets);

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
        _sessions.TryGetValue(pid, out var s) ? s with { Sid = sid } : null;
}
