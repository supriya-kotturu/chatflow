# ChatFlow v2 — Assignment 2 Report

## Overview

Assignment 2 extends the single-server WebSocket chat server from Assignment 1 into a horizontally scalable, multi-server architecture. The core addition is a RabbitMQ message broker that enables cross-server fan-out: a message sent by a client connected to Server A is received by all clients in the same room regardless of which server they are connected to.

---

## How the Design Changed from Assignment 1

### Assignment 1: Single Server, Direct Broadcast

In Assignment 1, the server handled all chat traffic on a single EC2 instance. When a client sent a message, the server handler immediately fanned it out to all other clients in the same room by iterating the room's `Users` map and writing to each client's `Send` channel.

```bash
Client → Server → Room.Broadcast channel → broadcast goroutine → all Clients in room
```

This worked because all clients were connected to the same process and shared the same in-memory `Rooms` map. There was no coordination needed between servers.

**Limitations:**

- Single point of failure — if the server goes down, all connections are lost
- Vertical scaling only — the single instance was a CPU bottleneck at high concurrency
- No message delivery across server boundaries

### Assignment 2: Multi-Server with RabbitMQ Fan-Out

Assignment 2 introduces a RabbitMQ topic exchange as the coordination layer between servers. Each server publishes every received message to the exchange with a routing key of `room.<id>`. Each server has one auto-delete queue per room, bound to that routing key, and consumes from those queues. This gives every server a copy of every message sent to any room, regardless of which server the sender is connected to.

```
Client → Server N
           ├── local fan-out → all Clients connected to Server N in that room
           └── Publish → RabbitMQ (chat.exchange, room.<id>)
                              ↓ (routed to all server queues bound to room.<id>)
                         Server 1..N (excluding sender via self-filter)
                              └── consume → Room.Broadcast → all local Clients
```

**Key invariant**: The publishing server skips its own consumed messages using a `ServerID` field embedded in every `QueueMessage`. This prevents double-delivery to local clients.

---

## Architecture Decisions

### 1. Topic Exchange with Per-Server Auto-Delete Queues

A topic exchange was chosen over direct or fanout exchanges because it allows per-room routing using binding keys (`room.1`, `room.2`, etc.). Each server declares `N` queues named `room.<id>.<serverID>` — one per room — and binds them to the exchange. Auto-delete queues are used so that queues disappear when the server disconnects, avoiding stale queue accumulation across restarts.

### 2. Two AMQP Connections (Publishers and Consumers Separated)

A single AMQP connection was initially used for both publishers and consumers. Under load, the 30 publish workers flooded the connection with outbound messages, starving the consumer's `Accept()` acknowledgments. This caused AMQP credit exhaustion: after `InitialCredits` messages were delivered but not Acked, the broker stopped sending new messages, leaving them permanently Unacked.

**Fix**: Two separate AMQP connections — one for the 30 publish workers (`conn`) and one for all consumer goroutines (`consumerConn`). Both connections are monitored via `NotifyStatusChange` so the circuit breaker responds to failure of either.

### 3. Async Consumer Accept (Credit Flow)

AMQP 1.0 uses a credit-based flow control mechanism. When a consumer receives a message, it must call `Accept()` to return credit to the broker. If `Accept()` is called synchronously inside the receive loop, it blocks the loop while the acknowledgment round-trips to the broker. At `InitialCredits=1000` and 20 consumers per server (80 across 4 servers), this caused 80,000 messages to pile up Unacked, permanently stalling all queues.

**Fix**: `Accept()` is called in a separate goroutine for each delivery. The receive loop immediately calls `Receive()` again, keeping credits flowing continuously.

```go
go func() {
    if err := d.Accept(ctx); err != nil { ... }
}()
```

### 4. Consumer Goroutines Pre-Started at Init

Initially, consumer goroutines were started lazily when a room was first accessed (`Consume()` call from `AddNewRoom`). This meant that if messages arrived before the first client joined a room, those messages were left Unacked indefinitely — the AMQP consumer existed but had no receive loop draining it.

**Fix**: All `N` consumer goroutines are started in `NewRabbitMQ` at server startup, before any client connects. Each goroutine always calls `Accept()` regardless of whether a handler is registered, so no messages are ever left Unacked. The handler is registered separately via `Consume()` and the goroutine routes to it from that point forward.

### 5. Blocking Publish with Back-Pressure

The original `Publish()` implementation dropped messages when the `publishChan` was full. This silently lost messages under load. Instead, `Publish()` blocks on the channel until space is available, propagating back-pressure all the way back to the WebSocket read loop:

```
WebSocket read loop → broadcastAndPublish → Publish(ctx) blocks on full publishChan
                                                    ↓
                                         TCP back-pressure to client
                                                    ↓
                                         Client slows send rate
```

`publishChanSize=4000` keeps queue depth bounded. With 30 workers draining it, the peak observed queue depth was ~280 messages during the 502k test runs.

### 6. 30 Publish Workers

Each publish worker owns its own dedicated AMQP publisher (sender link) to avoid serialization. The worker count was tuned empirically:

- **512 workers**: excessive goroutine overhead, high memory pressure
- **200 workers**: RabbitMQ queue depth oscillated between 0 and 8,000+; back-pressure too loose
- **30 workers**: queue depth bounded to ~260–1,100 peak; back-pressure tight enough to prevent buffer exhaustion, loose enough to not bottleneck WebSocket throughput

### 7. Write Deadline on WebSocket Sends

The server sets a 15-second write deadline before every `WriteJSON` call and closes the connection on error. Without this, a slow or disconnected client's `Send` channel fills up, the broadcast goroutine starts dropping messages for that client, and the connection goroutines leak indefinitely with no way to detect the dead connection.

### 8. Force-Close WebSockets on Shutdown

`http.Server.Shutdown()` drains active HTTP requests but skips hijacked connections (WebSockets). The read goroutines in `ChatRoomHandler` block indefinitely on `ReadJSON`, so they never exit and the process hangs. On shutdown, the server explicitly iterates all rooms and calls `conn.Close()` on every WebSocket, unblocking all `ReadJSON` calls before handing off to `Shutdown`.

### 9. In-Order User Removal via Broadcast Channel

In Assignment 1, `RemoveUserFromRoom` closed the client's `Send` channel immediately when a LEAVE message arrived. This caused a teardown race: the broadcast goroutine could be mid-fan-out when the channel was closed, causing missed final messages or a panic.

**Fix**: LEAVE no longer closes the channel directly. Instead, a `roomEvent{removeUserId: userId}` is enqueued into the room's `Broadcast` channel. The broadcast goroutine processes this event after all preceding messages have been fanned out, guaranteeing that the client's Send channel is only closed after all in-flight messages are delivered.

### 10. Circuit Breaker for RabbitMQ Disconnections

A three-state circuit breaker (`CircuitClosed` → `CircuitBuffering` → `CircuitOpen`) handles transient RabbitMQ connection failures. When reconnecting, messages are buffered in a small `tempBuffer` channel. If the buffer fills before reconnection, the circuit opens and messages are dropped rather than blocking the WebSocket read loops. On reconnection, buffered messages are flushed before normal publishing resumes.

---

## Threading Model Changes

### Assignment 1

Per connection: **2 goroutines** (reader + writer), communicating via a buffered `Send` channel.

```
ChatRoomHandler (reader goroutine)
    └── handleClientWrites (writer goroutine)
```

Shared state protected by `Server.Mu` (rooms map) and `Room.Mu` (users map).

### Assignment 2

Per connection: same **2 goroutines** (reader + writer). Added globally per server:

- **1 broadcast goroutine per room** (unchanged from v1 structurally, but now processes both fan-out and removal events)
- **30 publish worker goroutines** (new) — drain `publishChan`, each owns one AMQP publisher
- **N consumer goroutines** (new, one per room) — receive from RabbitMQ, call async Accept
- **1 state watcher goroutine** (new) — monitors AMQP connection state, drives circuit breaker
- **1 monitor goroutine** — logs throughput and RabbitMQ drop stats every interval

For 1,000 connections across 20 rooms (50 users/room):
- Assignment 1: `2 × 1000 + 20 = 2,020` goroutines
- Assignment 2: `2 × 1000 + 20 + 30 + 20 + 1 + 1 = 2,072` goroutines (overhead is minimal)

---

## Infrastructure Changes

| Aspect | Assignment 1 | Assignment 2 |
|--------|-------------|-------------|
| Server instances | 1 × t3.micro | 1–4 × t3.micro |
| Instance size | t3.micro (1 GB RAM) | t3.micro (1 GB RAM) |
| Load balancer | None | AWS ALB (port 80 → port 3000) |
| Message broker | None | RabbitMQ 4.0 on t3.medium |
| Client → server | Direct connection | ALB with session stickiness |
| Per-client send buffer | 2,048 | 60,000–120,000 |
| File descriptor limit | 1,024 (default) | 65,535 (systemd `LimitNOFILE`) |

The per-client buffer increase (2,048 → 60,000) was necessary because with RabbitMQ fan-out, each message now generates N-1 consumed copies delivered to local clients. With 50 users per room, each TEXT message generates 49 inbound deliveries from RabbitMQ, all competing to write to each client's `Send` channel simultaneously. The larger buffer absorbs this burst rather than dropping messages.

---

## Performance Results

Full results are in [RESULTS.md](RESULTS.md).

### Assignment 1 — Single Server Baseline

| Config | Total Messages | Throughput | Mean Latency | Failures |
|--------|---------------|------------|-------------|---------|
| 500K | 502,000 | 58,944 msg/s | 1,166ms | 0 |
| 1M | 1,002,000 | 45,617 msg/s | 2,508ms | 3,583 (0.36%) |

Assignment 1 ran client and server in the same region (local → EC2 over internet). Throughput was high (~45–59k msg/s) because the single server's broadcast was all in-memory with no external coordination overhead.

### Assignment 2 — Multi-Server with RabbitMQ

Test config: 1,000 connections, 50 users/room, 500 messages/user, 20 rooms = **502,000 messages**.

| Servers | Runtime | Throughput | Mean Latency | Failures |
|---------|---------|------------|-------------|---------|
| 1 | 600.5s | 836 msg/s | 148s | 0 |
| 2 | 243s | 2,067 msg/s | 115s | 0 |
| 4 | 323.8s | 1,550 msg/s | 0 | 0 |

**Why Assignment 2 throughput is lower than Assignment 1**: The latency unit is seconds (not milliseconds) because every message now passes through RabbitMQ — adding serialization, AMQP publish, queue routing, and consumption on top of the direct in-memory fan-out. The client test also measures end-to-end latency including the receive echo, not just the server-side processing time.

### Scaling Analysis

**1 → 2 servers (2.5× improvement)**: Halving per-server connection count cuts CPU contention on the `publishChan` and the broadcast goroutine. Each server processes 500 connections instead of 1,000, reducing queuing delay significantly.

**2 → 4 servers (25% regression)**: Throughput drops because each message must now fan out to 4 queues instead of 2. With the same 30 publish workers, the per-message RabbitMQ overhead increases linearly with server count. The bottleneck shifts from CPU contention (per-server) to RabbitMQ fan-out overhead (shared). With 4 servers, a single message published to `chat.exchange` is routed to 4 queues and consumed by 4 servers, quadrupling cross-server traffic compared to 1 server.

---

## Key Bugs Fixed During Development

| # | Bug | Fix |
|---|-----|-----|
| 1 | AMQP publish timeout 5s too short under load | Increased to 30s |
| 2 | InitialCredits=200, queues stalled at 200 Unacked | Increased to 1,000 |
| 3 | Single AMQP connection: publishers starved consumer Acks | Split into two connections |
| 4 | consumerConn not monitored — failures silently ignored | Added `NotifyStatusChange` on both connections |
| 5 | stateChanged channel capacity 1 — dropped state events under load | Increased to 8 |
| 6 | No WebSocket write deadline — dead clients leaked goroutines | Added 15s deadline + `conn.Close()` on error |
| 7 | `httpServer.Shutdown` skipped hijacked WebSocket connections | Force-close all WebSocket conns before shutdown |
| 8 | Consumer goroutines started lazily — early messages left permanently Unacked | Pre-start all consumer goroutines in `NewRabbitMQ` |
| 9 | 512 publish workers: excessive memory pressure | Tuned to 30 workers |
| 10 | `Publish()` dropped messages silently when channel full | Changed to blocking with ctx — propagates back-pressure |
| 11 | Synchronous `Accept()` inside receive loop — exhausted AMQP credits | Moved `Accept()` into a separate goroutine per delivery |
