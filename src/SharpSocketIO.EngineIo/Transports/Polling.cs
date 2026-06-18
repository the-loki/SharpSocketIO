using System;
using System.Collections.Generic;
using System.Text;
using SharpSocketIO.EngineIo.Commons;
using SharpSocketIO.EngineIo.Parser.Commons;
using EioParser = SharpSocketIO.EngineIo.Parser;
using EioPacketType = SharpSocketIO.EngineIo.Parser.Commons.PacketType;

namespace SharpSocketIO.EngineIo.Transports;

/// <summary>
/// Port of lib/transports/polling.ts (logic subset for 3A). Real HTTP wiring
/// (compression, pause/resume, write-through-on-GET) is 3B. Here: ingest POST
/// payloads → emit packets; buffer outbound packets and flush as a \x1e-joined
/// string (mirrors JS encodePayload's supportsBinary=false path).
/// </summary>
public class Polling : Transport
{
    private const char Sep = (char)30;
    private readonly List<Packet> _outbound = new();

    public Polling(IEngineRequest req) : base(req) { }

    public override string Name => "polling";

    public void Enqueue(Packet packet) => _outbound.Add(packet);

    public override void Send(IReadOnlyList<Packet> packets)
    {
        foreach (var p in packets) _outbound.Add(p);
    }

    /// <summary>Flushes queued packets as a \x1e-joined payload string (force base64 for binary).</summary>
    public string FlushToString()
    {
        if (_outbound.Count == 0) return string.Empty;
        var encoded = new string[_outbound.Count];
        for (int i = 0; i < _outbound.Count; i++)
        {
            string captured = string.Empty;
            EioParser.EncodePacket.Encode(_outbound[i], false, r => captured = r.AsString()!);
            encoded[i] = captured;
        }
        _outbound.Clear();
        return string.Join(Sep.ToString(), encoded);
    }

    /// <summary>Ingests a POSTed payload body, emitting one packet event per decoded packet.</summary>
    public void OnDataRequest(IEngineRequest req)
    {
        var body = req.Body;
        if (body == null || body.Length == 0) return;
        var payload = Encoding.UTF8.GetString(body);
        var parts = payload.Split(Sep);
        foreach (var part in parts)
        {
            var packet = EioParser.DecodePacket.Decode(new RawData(part));
            OnPacket(packet);
            if (packet.Type == EioPacketType.Error) break;
        }
    }

    public override void DoClose(Action? callback = null) => callback?.Invoke();
}
