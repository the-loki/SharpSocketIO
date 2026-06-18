using SharpSocketIO.ComponentEmitter;

namespace SharpSocketIO.SocketIo.Parser.Commons;

/// <summary>
/// Marker event map for the Decoder's emitted events. Currently: 'decoded' (Packet).
/// Dispatch is string-keyed (see Emitter&lt;TEvents&gt;).
/// </summary>
public sealed class DecodedEvents : EmitterEvents { }
