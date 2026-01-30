# Prompts Used in Chat-Flow Project

## Development Prompts

### JavaScript/Frontend
1. **Path Parameter Analysis**: "what happens when htven the getRoomId from path returns the last segment after splitting the path by &quot;/"
2. **URL Structure Testing**: "http://localhost:3000/chat/sdsfh/ssssssss"
3. **404 Debugging**: "but the browser shows 404, why?"
4. **Routing Strategy**: "which approach is better? query params or path?"
5. **Route Confirmation**: "I'll stick with chat/id"
6. **Route Restriction**: "no, I dont want nested routes to chat/34"

### Documentation
7. **Prompt Tracking**: "add the list of prompts used in docs/PROMPTS.md. Keep track of all the promps"

## Technical Decisions Made

- **URL Structure**: Chose path parameters (`/chat/roomId`) over query parameters for cleaner URLs
- **Route Pattern**: Decided on exact match `/chat/:roomId` to prevent nested routes
- **WebSocket Integration**: Room ID extracted from URL path for WebSocket connection

## Code Snippets Generated

### Server Route Configuration
```javascript
app.get('/chat/:roomId', (req, res) => {
    res.sendFile(path.join(__dirname, 'html/index.html'));
});
```

### Alternative Query Parameter Approach (Not Used)
```javascript
function getRoomIdFromPath() {
    return new URLSearchParams(window.location.search).get('room');
}
```

---
*Last updated: [Current Date]*
*Total prompts tracked: 7*

## Discussion Notes (added 2026-01-30)

Summary of the WebSocket / client-pool discussion and actionable guidance taken during development:

- Where to defer Close
    - Do NOT defer `conn.Close()` inside a constructor (e.g. `NewWsClient`). Defer in the owner scope (typically `main`) or expose a `Close()` method on `WsClient` and call `defer client.Close()` after creation or `defer pool.CloseAll()` after creating the pool.

- wsClient pool design (size 20 / simulations)
    - Pool holds `*WsClient` objects in a buffered channel. Create N clients up-front if you want N concurrent users.
    - Example: `pool, _ := NewWsClientPool(20, env.Port); defer pool.CloseAll()`

- readCh / writeCh purpose
    - `readCh`: receives incoming messages from a single reader goroutine that does `conn.ReadJSON(&msg)` and pushes into `readCh`.
    - `writeCh`: optional; used by a single writer goroutine to serialize writes (drains `writeCh` and calls `conn.WriteJSON`). Alternative: provide a `Write()` helper that uses a mutex to serialize `WriteJSON` calls.

- Start/Stop IO and lifecycle
    - Start a single persistent reader goroutine when a `WsClient` is created (in `NewWsClient`). It should `close(readCh)` when the connection dies.
    - Do not start/stop the reader repeatedly on each `Pool.Get()`; that caused multiple concurrent readers and corruption.
    - If you use a writer goroutine, create `writeCh` in `NewWsClient` and close it to stop the writer on shutdown.

- Reset and room IDs
    - Do NOT attempt to change the WS path (roomId) on an existing connection. To join a different room you must reconnect (close + dial with a new URL) or implement a server-side JOIN control message.
    - `Reset()` may safely recreate outbound queues (e.g. `writeCh`) but must not recreate `readCh` while the reader goroutine is still running.

- Concurrency rules and crash root cause
    - Gorilla WebSocket is NOT safe for concurrent reads. Concurrent `ReadJSON` calls corrupt internal buffers, producing weird frame errors (RSV/bad opcode) and panics (slice bounds). The project saw this because the pool started `go c.Read()` on every checkout.
    - Fix: start exactly one reader per connection (call `go c.Read()` in `NewWsClient`) and remove `go c.Read()` from `Pool.Get()`.
    - Serialize writes either with a per-client `sync.Mutex` around `WriteJSON` or with a single per-connection writer goroutine.

- Handling Ctrl+C and noisy logs
    - When shutting down, readers can see errors like "use of closed network connection". Treat these as expected and do not log them as errors. Check `errors.Is(err, net.ErrClosed)` or use `websocket.IsCloseError`.
    - Graceful close option: send a close control frame (`WriteMessage(websocket.CloseMessage, ...)`) before `Close()`.

- RSV / bad opcode errors troubleshooting
    - These indicate non-WebSocket frames or corrupted frames: common causes are wrong endpoint (HTTP instead of WS), proxy/upgrade misconfiguration, TLS/scheme mismatch, or concurrent read/write corruption. Verify correct URL, test with a single client (browser/wscat), and ensure the upgrade/ proxy supports WebSocket.

- Empty messages observed in logs
    - `generate.NewMessage()` intentionally sets `Message == ""` for JOIN/LEAVE types. The server echoes the struct; therefore logs show empty `Message` fields for join/leave events. Options: change generator to populate a join/leave message, or log message type in addition to the body.

- Simulation guidance (100 users x 20 messages)
    - If you want 100 fully concurrent simulated users, allocate 100 connections (pool size 100) — then each goroutine can Get() a client and send 20 messages without contention.
    - If you must share a smaller pool across many logical users, use exclusive checkout for the session or implement a multiplexing/dispatcher pattern: one reader + one writer per physical connection, envelope messages with a session id, and route them to per-session channels.

- Testing recommendations
    - Run the race detector: `go run -race ./client` or `go test -race ./...` to surface concurrency issues.
    - Reproduce with a single client (browser/wscat) to ensure server behavior is correct before scaling.

If you want, I can apply the small patches discussed: move reader startup into `NewWsClient`, remove reader startup from `Pool.Get`, and add/keep `Write()` with a mutex.

## Reading & Resources (backpressure, dispatcher/multiplexing)

High-signal resources and reading to understand backpressure and dispatcher/multiplexing patterns:

- Reactive Manifesto — foundations of responsive systems and backpressure concepts
    - https://www.reactivemanifesto.org/

- Reactive Streams (spec) — contract for asynchronous stream processing with backpressure
    - https://www.reactive-streams.org/

- Designing Data-Intensive Applications (Martin Kleppmann) — chapters on streaming, flow-control and fault-tolerance
    - https://dataintensive.net/

- Go blog — Pipelines (channel-based stages and backpressure)
    - https://blog.golang.org/pipelines

- Go blog — Context package (cancellation/timeouts for IO and graceful shutdown)
    - https://blog.golang.org/context

- Gorilla WebSocket README — concurrency caveats and best practices (one reader, serialized writes)
    - https://github.com/gorilla/websocket

- RFC6455 — WebSocket protocol (control frames, close semantics)
    - https://tools.ietf.org/html/rfc6455

- Akka Streams docs — concrete backpressure implementation and patterns (JVM-focused but instructive)
    - https://doc.akka.io/docs/akka/current/stream/index.html

Practical patterns and quick recipes:

- Use bounded channels and blocking sends for natural backpressure; choose a sender policy (block, error, drop-oldest).
- Prefer a single reader goroutine per connection and either a mutex-protected Write() or a single writer goroutine draining a `writeCh`.
- For multiplexing: use an Envelope{SessionID, Payload} and run a per-connection dispatcher that demuxes incoming Envelopes to per-session channels and serializes outgoing Envelopes.
- Instrument queue lengths and dropped messages; run with `-race` during development.

If you want, I can add a small example implementation (Go) for either: (A) writer-goroutine per connection, or (B) a dispatcher/multiplexer with Envelope and RegisterSession APIs.