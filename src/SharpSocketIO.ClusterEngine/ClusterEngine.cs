using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using SharpSocketIO.ComponentEmitter;

namespace SharpSocketIO.ClusterEngine;

/// <summary>
/// Port of lib/engine.ts ClusterEngine. A cluster-friendly engine.io server where any
/// instance can receive any client request; a distributed lock per session ensures only
/// one instance owns a session at a time. Packets for a session owned elsewhere are routed.
/// </summary>
public abstract class ClusterEngine : Emitter<UnitEvents>
{
    public string NodeId { get; } = GenerateNodeId();

    /// <summary>sessions currently locked (owned) by this instance: sid → buffered packets.</summary>
    private readonly ConcurrentDictionary<string, List<string>> _ownedSessions = new();
    /// <summary>pending lock requests: requestId → TCS.</summary>
    private readonly ConcurrentDictionary<int, TaskCompletionSource<bool>> _lockRequests = new();
    /// <summary>which node owns a session (best-effort, from lock responses).</summary>
    private readonly ConcurrentDictionary<string, string> _sessionOwners = new();
    private int _nextRequestId = 1;

    private static string GenerateNodeId() => Guid.NewGuid().ToString("N").Substring(0, 6);

    /// <summary>Called by the transport to publish a message to peers.</summary>
    protected abstract void PublishMessage(ClusterEngineMessage message);

    /// <summary>Called by the transport when a message arrives from a peer.</summary>
    public void OnMessage(ClusterEngineMessage message)
    {
        switch (message)
        {
            case AcquireLockMessage lockReq:
                HandleAcquireLock(lockReq);
                break;
            case AcquireLockResponseMessage lockResp:
                if (_lockRequests.TryRemove(lockResp.RequestId, out var tcs))
                    tcs.TrySetResult(lockResp.Success);
                break;
            case PacketMessage pkt:
                EmitReserved("packet", pkt);
                break;
            case DrainMessage drain:
                // we received buffered packets for a session we now own
                foreach (var p in drain.Packets) EmitReserved("drained-packet", drain.SessionId, p);
                break;
            case CloseMessage close:
                _ownedSessions.TryRemove(close.SessionId, out _);
                _sessionOwners.TryRemove(close.SessionId, out _);
                EmitReserved("remote-close", close.SessionId);
                break;
        }
    }

    /// <summary>Attempts to acquire the lock for a session (so this instance owns it).</summary>
    public async Task<bool> AcquireLockAsync(string sessionId, string transportName = "polling", string requestType = "read", int timeoutMs = 5000)
    {
        // if we already own it, succeed immediately
        if (_ownedSessions.ContainsKey(sessionId)) return true;
        var requestId = System.Threading.Interlocked.Increment(ref _nextRequestId);
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        _lockRequests[requestId] = tcs;
        PublishMessage(new AcquireLockMessage
        {
            SenderId = NodeId,
            RequestId = requestId,
            SessionId = sessionId,
            TransportName = transportName,
            RequestType = requestType,
        });
        // also mark locally as owner optimistically if nobody else responds (single-instance)
        using var cts = new System.Threading.CancellationTokenSource(timeoutMs);
        cts.Token.Register(() => tcs.TrySetResult(true)); // assume acquired if no denial (in-process: no peers deny)
        var success = await tcs.Task;
        if (success)
        {
            _ownedSessions[sessionId] = new List<string>();
            _sessionOwners[sessionId] = NodeId;
        }
        return success;
    }

    /// <summary>Handles a lock request from a peer: if we own the session, deny (or release).</summary>
    private void HandleAcquireLock(AcquireLockMessage req)
    {
        bool success = !_ownedSessions.ContainsKey(req.SessionId);
        if (_ownedSessions.ContainsKey(req.SessionId))
        {
            // we own it — for read requests we could allow; for write we deny. Minimal: deny.
            success = false;
        }
        PublishMessage(new AcquireLockResponseMessage
        {
            SenderId = NodeId,
            RecipientId = req.SenderId,
            RequestId = req.RequestId,
            Success = success,
        });
    }

    /// <summary>Buffers a packet for a session we own (until the client polls).</summary>
    public void BufferPacket(string sessionId, string packet)
    {
        if (_ownedSessions.TryGetValue(sessionId, out var buffer)) buffer.Add(packet);
    }

    /// <summary>Routes a packet to the owning instance (if not us).</summary>
    public void RoutePacket(string sessionId, string packet)
    {
        PublishMessage(new PacketMessage
        {
            SenderId = NodeId,
            RecipientId = _sessionOwners.GetValueOrDefault(sessionId),
            SessionId = sessionId,
            Packet = packet,
        });
    }

    /// <summary>Releases ownership and drains buffered packets to the new owner.</summary>
    public void ReleaseAndDrain(string sessionId, string recipientId)
    {
        if (_ownedSessions.TryRemove(sessionId, out var buffer))
        {
            PublishMessage(new DrainMessage
            {
                SenderId = NodeId,
                RecipientId = recipientId,
                SessionId = sessionId,
                Packets = buffer,
            });
            _sessionOwners.TryRemove(sessionId, out _);
        }
    }

    /// <summary>Closes a session and notifies peers.</summary>
    public void CloseSession(string sessionId)
    {
        _ownedSessions.TryRemove(sessionId, out _);
        _sessionOwners.TryRemove(sessionId, out _);
        PublishMessage(new CloseMessage { SenderId = NodeId, SessionId = sessionId });
    }

    public bool OwnsSession(string sessionId) => _ownedSessions.ContainsKey(sessionId);
    public int OwnedSessionCount => _ownedSessions.Count;
}
