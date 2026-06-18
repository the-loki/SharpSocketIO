# Design Addendum: engine.io Sub-cycle 3C — WebSocket transport + upgrade

**Date:** 2026-06-18
**Scope:** Sub-cycle 3C of the engine.io port. Adds the WebSocket transport
(`System.Net.WebSockets`) and the polling→websocket upgrade handshake, ported
from `lib/transports/websocket.ts` + the upgrade logic in `lib/server.ts` +
`lib/socket.ts`.

---

## 1. Goal & success criteria (3C gate)

1. `Transports/WebSocketTransport` — wraps an accepted `System.Net.WebSockets.WebSocket`,
   uses the 3A `SharpSocketIO.EngineIo.Parser` binary packet encode/decode for each frame.
2. `Server.HandleWebSocketUpgradeAsync(HttpContext)` — accepts the WS upgrade for a
   request carrying an existing sid, builds a `WebSocketTransport`, and either (a)
   upgrades an existing polling socket (probe ping/pong + `upgrade` packet) or
   (b) handshakes a fresh ws-only session.
3. Polling→websocket upgrade handshake works end-to-end against a `ClientWebSocket`
   driver: client opens polling (gets sid + `upgrades:["websocket"]`), opens WS with
   sid, sends `2probe` (ping/probe), server replies `3probe` (pong/probe), client
   sends `5upgrade`, server swaps transport, emits `upgrade`.
4. ws-only handshake works (transports=`["websocket"]`): direct WS open → `0{...}` open packet.
5. Ported server.js ws subset tests pass via `ClientWebSocket` driver.
6. `dotnet test` green on net8/9/10.

---

## 2. WebSocket mapping (System.Net.WebSockets)

| JS (`ws` lib) | .NET |
|---|---|
| `ws.on("message", (data, isBinary))` | `await ws.ReceiveAsync(buffer, ct)` loop; `EndOfMessage` frames a full message; `WebSocketMessageType.Text/Binary` ↔ isBinary |
| `ws.send(data)` | `await ws.SendAsync(data, messageType, endOfMessage: true, ct)` |
| `ws.close()` | `await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, ..., ct)` |
| `ws.on("close")` | `ReceiveAsync` returns `Result.CloseStatus != null` |
| `req.websocket` (server-accepted socket) | `await context.WebSockets.AcceptWebSocketAsync()` |

**Framing:** engine.io over WS uses one packet per WS message — the v4 parser
encodes each packet to a string (text) or bytes (binary). Each `Send` writes one
WS message. Binary packets use `WebSocketMessageType.Binary`; text uses `Text`.

---

## 3. Upgrade handshake (port of socket._maybeUpgrade + server.handleUpgrade)

Server-side flow when a WS request arrives with an existing sid:
1. Look up the existing `Socket` (must be polling, open, not already upgrading).
2. Accept the WS → build `WebSocketTransport`, mark `socket.upgrading=true`, set
   `upgradeTimeout` (default 10s) timer.
3. Listen on the new transport for packets:
   - `ping` with data `"probe"` → reply `pong`/`"probe"` on the WS transport.
   - `upgrade` packet → cleanup, discard the polling transport, swap to WS,
     emit `upgrade`, flush buffered packets.
4. Force a polling cycle (send `noop` on the polling transport) to speed up the probe.
5. On timeout / error / wrong packet → cleanup, close the WS transport.

ws-only (no sid on the WS request): treat as a fresh handshake — accept the WS,
build the WebSocketTransport, create the Socket, send the open packet over WS.

---

## 4. File structure additions

```
src/SharpSocketIO.EngineIo/
  Transports/
    WebSocketTransport.cs        // Transport : wraps System.Net.WebSockets.WebSocket
  Http/
    ServerExtensions.cs          // extend: detect WS upgrade, route to HandleWebSocketUpgradeAsync
  Socket.cs                      // extend: MaybeUpgrade(transport) logic
  Server.cs                      // extend: HandleWebSocketUpgradeAsync + ws-only handshake
tests/SharpSocketIO.EngineIo.Tests/
  Commons/
    WsDriver.cs                  // ClientWebSocket driver: open polling, upgrade, send/recv
  WebSocketTests.cs              // ← ported server.js ws subset
```

---

## 5. Behavior to preserve verbatim

- WS messages are one packet each; text packets as `TextMessage`, binary as `BinaryMessage`.
- Upgrade advertised in handshake `upgrades: ["websocket"]` only when both polling
  and websocket transports are enabled and `allowUpgrades` is true.
- ws-only (`transports: ["websocket"]`): no `upgrades` field items, open over WS directly.
- Probe sequence: client→server `ping`/`probe`, server→client `pong`/`probe`,
  client→server `upgrade`. Exact packet types.
- After upgrade, polling transport is discarded; further packets flow over WS.

---

## 6. Adaptations / deviations

1. **No `ws` lib internals** (`_sender.sendFrame`, `wsPreEncodedFrame`). The pre-encoded-frame
   optimization is `ws`-specific; in .NET we always encode via the parser. Omit `PacketOptions.WsPreEncodedFrame`.
2. **`perMessageDeflate` deferred.** Kestrel's WebSocket compression (`WebSocketsOptions.UseCompression`)
   is a separate flag; we leave it disabled in 3C for simplicity (tests don't require it).
3. **Receive loop on a background Task.** Each WS transport spawns a receive loop that
   feeds `OnData`/`OnPacket`; cancellation on close.

---

## 7. Test scope (ported server.js ws subset)

- ws-only open: `transports:["websocket"]` → connect via WS, receive open packet, transport name = websocket, upgrades empty.
- upgrades advertised in default handshake (already covered in 3B body assertion; reaffirm over WS).
- full upgrade flow: open polling → upgrade to WS via probe sequence → send/receive a message over WS.
- server-initiated close over WS.
- ws packet round-trip (string + binary).

---

## Next step

writing-plans → 3C implementation plan, then TDD.
