using System.Collections.Generic;
using System.Threading.Tasks;
using SharpSocketIO.ComponentEmitter;
using SharpSocketIO.SocketIo.Parser;
using SharpSocketIO.SocketIo.Parser.Commons;
using SiPacketType = SharpSocketIO.SocketIo.Parser.Commons.PacketType;

namespace SharpSocketIO.SocketIoClient;

/// <summary>
/// Port of lib/socket.ts — a per-namespace socket.io client Socket. Handles connect/
/// disconnect, on/emit, ack round-trips, and reserved events (connect/connect_error/disconnect).
/// </summary>
public sealed class SocketIoClientSocket : Emitter<UnitEvents>
{
    public string Nsp { get; }
    public Manager Manager { get; }
    public string? Id { get; private set; }
    public bool Connected { get; private set; }
    public bool Active => Connected;

    private readonly Dictionary<int, System.Action<object[]>> _acks = new();
    private int _ackId;
    private bool _volatile;
    private HashSet<string> _rooms = new();

    public SocketIoClientSocket(Manager manager, string nsp)
    {
        Manager = manager;
        Nsp = nsp;
    }

    /// <summary>Connects (or reconnects) this namespace socket.</summary>
    public async Task ConnectAsync()
    {
        // send a CONNECT packet
        Manager.SendPacket(new Packet { Type = SiPacketType.Connect, Nsp = Nsp });
        await Task.CompletedTask;
    }

    /// <summary>Disconnects this namespace socket.</summary>
    public SocketIoClientSocket Disconnect()
    {
        Manager.SendPacket(new Packet { Type = SiPacketType.Disconnect, Nsp = Nsp });
        if (Connected) OnDestroy("client namespace disconnect");
        return this;
    }

    /// <summary>Registers an event handler.</summary>
    public new SocketIoClientSocket On(string eventName, System.Action<object[]> fn)
    {
        base.On(eventName, fn);
        return this;
    }

    /// <summary>Emits an event to the server.</summary>
    public new void Emit(string eventName, params object[] args)
    {
        if (_volatile && !Connected) { _volatile = false; return; }
        var packet = new Packet
        {
            Type = SiPacketType.Event,
            Nsp = Nsp,
            Data = BuildData(eventName, args),
        };
        Manager.SendPacket(packet);
    }

    /// <summary>Emits an event with an ack callback (invoked when the server replies).</summary>
    public void EmitWithAck(string eventName, System.Action<object[]> ack, params object[] args)
    {
        int id = _ackId++;
        _acks[id] = ack;
        var packet = new Packet
        {
            Type = SiPacketType.Event,
            Nsp = Nsp,
            Data = BuildData(eventName, args),
            Id = id,
        };
        Manager.SendPacket(packet);
    }

    /// <summary>Marks the next emit as volatile (dropped if not connected).</summary>
    public SocketIoClientSocket Volatile() { _volatile = true; return this; }

    private static object BuildData(string eventName, object[] args)
    {
        var list = new List<object> { eventName };
        list.AddRange(args);
        return list;
    }

    /// <summary>Called by the Manager when a socket.io packet arrives for this namespace.</summary>
    public void OnPacket(Packet packet)
    {
        switch (packet.Type)
        {
            case SiPacketType.Connect:
                Connected = true;
                Id = ExtractId(packet.Data);
                EmitReserved("connect");
                break;
            case SiPacketType.ConnectError:
                EmitReserved("connect_error", packet.Data ?? new object[0]);
                break;
            case SiPacketType.Event:
            case SiPacketType.BinaryEvent:
                HandleEvent(packet);
                break;
            case SiPacketType.Ack:
            case SiPacketType.BinaryAck:
                HandleAck(packet);
                break;
            case SiPacketType.Disconnect:
                OnDestroy("io server disconnect");
                break;
        }
    }

    private void HandleEvent(Packet packet)
    {
        var data = packet.Data as System.Collections.IList;
        if (data == null || data.Count == 0) return;
        var eventName = data[0]?.ToString();
        var args = new List<object>();
        for (int i = 1; i < data.Count; i++)
        {
            if (packet.Id.HasValue && i == data.Count - 1 && data[i] is System.Collections.IList)
            {
                // last arg might be an ack placeholder — JS passes a function; we omit
            }
            args.Add(data[i]!);
        }
        base.Emit(eventName!, args.ToArray());
    }

    private void HandleAck(Packet packet)
    {
        int ackId = packet.Id ?? -1;
        if (_acks.TryGetValue(ackId, out var ack))
        {
            var data = (packet.Data as System.Collections.IList);
            var args = new List<object>();
            if (data != null) foreach (var d in data!) args.Add(d!);
            ack(args.ToArray());
            _acks.Remove(ackId);
        }
    }

    private static string? ExtractId(object? data)
    {
        // server CONNECT ack may carry { sid } — extract if present
        if (data is string s)
        {
            try
            {
                using var doc = System.Text.Json.JsonDocument.Parse(s);
                if (doc.RootElement.TryGetProperty("sid", out var sid)) return sid.GetString();
            }
            catch { }
        }
        return null;
    }

    private void OnDestroy(string reason)
    {
        Connected = false;
        EmitReserved("disconnect", reason);
    }
}
