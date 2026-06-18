using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using SharpSocketIO.SocketIo.Adapter;
using AdapterBase = SharpSocketIO.SocketIo.Adapter.Adapter;

namespace SharpSocketIO.ClusterAdapter;

/// <summary>
/// Port of socket.io-adapter/lib/cluster-adapter.ts ClusterAdapter. Each instance
/// represents one server; publishes ClusterMessages via DoPublish and receives them
/// via OnMessage. Broadcast/fetch/server-side-emit fan out across all instances.
/// </summary>
public abstract class ClusterAdapter : AdapterBase
{
    public string Uid { get; } = GenerateUid();
    private readonly string _nspName;
    private readonly ConcurrentDictionary<string, ClusterRequest> _requests = new();

    protected ClusterAdapter(IAdapterNamespace nsp, ClusterAdapterOptions? opts = null) : base(nsp)
    {
        _nspName = "?"; // set by the namespace when wired (the namespace's Name); tests pass via the nsp
        Options = opts ?? new ClusterAdapterOptions();
    }

    public ClusterAdapterOptions Options { get; }

    private static string GenerateUid() => Guid.NewGuid().ToString("N").Substring(0, 16);

    /// <summary>Called by the transport-specific subclass to publish a message to peers.</summary>
    protected abstract Task DoPublish(ClusterMessage message);

    /// <summary>Called by the transport-specific subclass to publish a response to the requester.</summary>
    protected abstract Task DoPublishResponse(string requesterUid, ClusterResponse response);
    public Task DoPublishForTestImpl(ClusterMessage msg) => DoPublish(msg);

    /// <summary>The namespace name (set when the adapter is wired to a Namespace).</summary>
    public string NamespaceName
    {
        get => _nspName;
        init { /* set via constructor chain */ }
    }

    /// <summary>Handles an incoming cluster message from a peer.</summary>
    public void OnMessage(ClusterMessage message)
    {
        if (message.Nsp != _nspNameResolved) return;
        switch (message)
        {
            case BroadcastMessage b:
                DeliverBroadcast(b);
                break;
            case SocketsJoinMessage j:
                foreach (var sid in LocalMatch(j.Opts)) foreach (var room in j.Rooms) Add(sid, room);
                break;
            case SocketsLeaveMessage l:
                foreach (var sid in LocalMatch(l.Opts)) foreach (var room in l.Rooms) Del(sid, room);
                break;
            case FetchSocketsMessage f:
                RespondFetchSockets(f);
                break;
            case ServerSideEmitMessage sse:
                // emit on the local namespace; response aggregation minimal for this cycle
                break;
        }
    }

    /// <summary>Handles an incoming response to a request this instance made.</summary>
    public void OnResponse(ClusterResponse response)
    {
        if (_requests.TryGetValue(response.RequestId, out var req))
        {
            req.Responses.Add(response);
            req.Current++;
            if (req.Current >= req.Expected)
            {
                req.Tcs?.TrySetResult(req.Responses.ToList());
                _requests.TryRemove(response.RequestId, out _);
            }
        }
    }

    public override async Task BroadcastAsync(string packet, BroadcastOptions opts)
    {
        // deliver locally (sockets on this instance), then publish to peers
        await base.BroadcastAsync(packet, opts);
        var msg = new BroadcastMessage
        {
            Uid = Uid,
            Nsp = _nspNameResolved,
            Packet = packet,
            Opts = new ClusterMessageOpts
            {
                Rooms = opts.Rooms.ToArray(),
                Except = (opts.Except ?? new HashSet<string>()).ToArray(),
                Flags = opts.Flags,
            },
        };
        await DoPublish(msg);
    }

    private void DeliverBroadcast(BroadcastMessage msg)
    {
        var opts = new BroadcastOptions
        {
            Rooms = new HashSet<string>(msg.Opts.Rooms),
            Except = new HashSet<string>(msg.Opts.Except),
            Flags = msg.Opts.Flags,
        };
        // deliver to local sockets (fire-and-forget; base is synchronous-ish)
        _ = base.BroadcastAsync(msg.Packet, opts);
    }

    private IEnumerable<string> LocalMatch(ClusterMessageOpts opts)
    {
        var rooms = new HashSet<string>(opts.Rooms);
        var except = new HashSet<string>(opts.Except);
        var matched = new HashSet<string>();
        if (rooms.Count == 0) foreach (var sid in Sids.Keys) matched.Add(sid);
        else foreach (var r in rooms) if (Rooms.TryGetValue(r, out var set)) foreach (var sid in set) matched.Add(sid);
        foreach (var exRoom in except) if (Rooms.TryGetValue(exRoom, out var exSet)) foreach (var sid in exSet) matched.Remove(sid);
        return matched;
    }

    private void RespondFetchSockets(FetchSocketsMessage f)
    {
        var sockets = new List<RemoteSocketInfo>();
        foreach (var sid in LocalMatch(f.Opts))
        {
            sockets.Add(new RemoteSocketInfo { Id = sid, Rooms = Sids.TryGetValue(sid, out var rs) ? rs.ToArray() : Array.Empty<string>() });
        }
        _ = DoPublishResponse(f.Uid, new FetchSocketsResponse { RequestId = f.RequestId, Uid = Uid, Nsp = f.Nsp, Sockets = sockets });
    }

    /// <summary>Fetches socket info from all instances (request/response aggregation).</summary>
    public async Task<List<RemoteSocketInfo>> FetchSocketsAsync(BroadcastOptions opts, int expectedResponses, int timeoutMs = 5000)
    {
        var requestId = GenerateUid();
        var req = new ClusterRequest { Expected = expectedResponses, Tcs = new TaskCompletionSource<List<ClusterResponse>>() };
        _requests[requestId] = req;
        await DoPublish(new FetchSocketsMessage
        {
            Uid = Uid,
            Nsp = _nspNameResolved,
            RequestId = requestId,
            Opts = new ClusterMessageOpts { Rooms = opts.Rooms.ToArray(), Except = (opts.Except ?? new HashSet<string>()).ToArray() },
        });
        // also include local sockets
        var localSockets = LocalMatch(new ClusterMessageOpts { Rooms = opts.Rooms.ToArray(), Except = (opts.Except ?? new HashSet<string>()).ToArray() })
            .Select(sid => new RemoteSocketInfo { Id = sid, Rooms = Sids.TryGetValue(sid, out var rs) ? rs.ToArray() : Array.Empty<string>() })
            .ToList();
        using var cts = new CancellationTokenSource(timeoutMs);
        cts.Token.Register(() => req.Tcs?.TrySetResult(req.Responses.ToList()));
        var remoteResponses = await req.Tcs.Task;
        foreach (var r in remoteResponses.OfType<FetchSocketsResponse>()) localSockets.AddRange(r.Sockets);
        return localSockets;
    }

    private string _nspNameResolved = "/";
    /// <summary>Sets the namespace name (called when the adapter is wired to a Namespace).</summary>
    public void SetNamespace(string name) => _nspNameResolved = name;

    private sealed class ClusterRequest
    {
        public int Expected { get; set; }
        public int Current { get; set; }
        public List<ClusterResponse> Responses { get; } = new();
        public TaskCompletionSource<List<ClusterResponse>>? Tcs { get; set; }
    }
}
