# ChatFlow v2 — Design Decisions

---

### 1. RabbitMQ Topic Exchange for Cross-Server Fan-Out

Each server publishes messages to a topic exchange with routing key `room.<id>`. Each server has one auto-delete queue per room. This gives every server a copy of every message regardless of which server the sender is connected to. Auto-delete queues prevent stale accumulation across restarts. A `ServerID` field in every message lets each server skip its own published messages to prevent double-delivery.

---

### 2. Two Separate AMQP Connections

Initially one connection was used for both publishers and consumers. Under load, 30 publish workers flooded the connection with outbound frames, starving the consumer's `Accept()` acknowledgments — causing AMQP credit exhaustion and permanently stalling all queues.

**Fix**: split into `conn` (publishers) and `consumerConn` (consumers), each monitored independently via `NotifyStatusChange`.

---

### 3. Async `Accept()` for AMQP Credit Flow

AMQP 1.0 uses credit-based flow control. Calling `Accept()` synchronously inside the receive loop blocked the loop on each acknowledgment round-trip. With 20 consumers and `InitialCredits=1000`, this stacked up 20,000 Unacked messages, freezing all queues permanently.

**Fix**: `Accept()` is called in a separate goroutine per delivery so the receive loop immediately calls `Receive()` again, keeping credits flowing continuously.

```go
go func() {
    if err := d.Accept(ctx); err != nil { ... }
}()
```

---

### 4. Consumer Goroutines Pre-Started at Init

Consumer goroutines were initially started lazily when the first client joined a room. Messages arriving before the first client were left permanently Unacked — the consumer existed but had no drain loop.

**Fix**: all N consumer goroutines start in `NewRabbitMQ` at server startup, unconditionally calling `Accept()` on every delivery regardless of whether a handler is registered.

---

### 5. Blocking `Publish()` with Back-Pressure

The original `Publish()` silently dropped messages when `publishChan` was full.

**Fix**: `Publish()` blocks on the channel, propagating back-pressure from the AMQP publish path all the way back to the WebSocket read loop → TCP → client send rate. `publishChanSize=4000` with 20 drain workers keeps queue depth bounded without bottlenecking WebSocket throughput.

```
WebSocket read loop → broadcastAndPublish → Publish(ctx) blocks on full publishChan
                                                    ↓
                                         TCP back-pressure to client
                                                    ↓
                                         Client slows send rate
```

---

### 6. 20 Publish Workers (Tuned Empirically)

| Workers | Outcome |
|---------|---------|
| 512 | Excessive memory pressure, goroutine overhead |
| 200 | Queue oscillated 0–8,000+; back-pressure too loose |
| 30 | ~260–1,100 peak queue; stable but slightly over target |
| 20 | ~20 peak queue; optimal — saturates broker without flooding |
| 80 | Queue spiked to ~10,000, violating the <1,000 target |

**Chosen**: Workers=20

---

### 7. 15s WebSocket Write Deadline

Without a write deadline, a slow or dead client's `Send` channel fills up, the broadcast goroutine starts dropping messages for that client, and the connection goroutines leak with no way to detect the dead connection.

**Fix**: `SetWriteDeadline` before every `WriteJSON`, `conn.Close()` on error.

---

### 8. Force-Close WebSockets on Shutdown

`http.Server.Shutdown()` drains active HTTP requests but skips hijacked WebSocket connections. Read goroutines block indefinitely on `ReadJSON`, hanging the process.

**Fix**: before calling `Shutdown()`, explicitly call `conn.Close()` on every WebSocket connection to unblock all read loops.

---

### 9. In-Order User Removal via Broadcast Channel

Calling `RemoveUserFromRoom` directly closed the client's `Send` channel, racing with the broadcast goroutine mid-fan-out — risking missed final messages or a panic on closed channel write.

**Fix**: LEAVE enqueues a `roomEvent{removeUserId}` into the room's `Broadcast` channel. The broadcast goroutine processes it only after all preceding messages are delivered, guaranteeing no channel-close race.

---

### 10. Circuit Breaker for RabbitMQ Failures

A circuit breaker using the `gobreaker` library handles transient broker disconnections. The initial custom three-state implementation (`Closed → Buffering → Open`) was replaced with `gobreaker` to get well-tested state transitions, configurable failure thresholds, and automatic half-open probing without hand-rolling the state machine. When the circuit opens, publish calls fail fast rather than blocking WebSocket read loops indefinitely.

---

### Key Architectural Constraint Discovered

Fan-out to large rooms (50 users) means every published message is routed to N queues (one per server per room). Adding servers increases total AMQP publish work linearly — the broker saturates before the chat servers do. Single server is optimal for this workload; horizontal scaling degraded throughput at every pool size tested.

| Servers | Throughput | vs 1 server |
|---------|------------|-------------|
| 1 | 1,583 msg/s | — |
| 2 | 1,529 msg/s | -3.4% |
| 4 | 933 msg/s | -41.0% |
