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

A single AMQP connection was initially used for both publishers and consumers. Under load, the publish workers flooded the connection with outbound messages, starving the consumer's `Accept()` acknowledgments. This caused AMQP credit exhaustion: after `InitialCredits` messages were delivered but not Acked, the broker stopped sending new messages, leaving them permanently Unacked.

**Fix**: Two separate AMQP connections — one for the publish workers (`conn`) and one for all consumer goroutines (`consumerConn`). Both connections are monitored via `NotifyStatusChange` so the circuit breaker responds to failure of either.

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

`publishChanSize=500` keeps back-pressure tight: the channel fills quickly under burst, causing `Publish()` to block and propagate flow control to the WebSocket read loops before RabbitMQ queue depth has a chance to spike.

### 6. 20 Publish Workers

Each publish worker owns its own dedicated AMQP publisher (sender link) to avoid serialization. The worker count was tuned empirically:

- **512 workers**: excessive goroutine overhead, high memory pressure
- **200 workers**: RabbitMQ queue depth oscillated between 0 and 8,000+; back-pressure too loose
- **30 workers**: queue depth bounded to ~260–1,100 peak; back-pressure tight enough but queue briefly exceeded the 1,000 target
- **20 workers**: queue depth ~20 peak; optimal — saturates broker without flooding; all tuning targets met

### 7. Write Deadline on WebSocket Sends

The server sets a 15-second write deadline before every `WriteJSON` call and closes the connection on error. Without this, a slow or disconnected client's `Send` channel fills up, the broadcast goroutine starts dropping messages for that client, and the connection goroutines leak indefinitely with no way to detect the dead connection.

### 8. Force-Close WebSockets on Shutdown

`http.Server.Shutdown()` drains active HTTP requests but skips hijacked connections (WebSockets). The read goroutines in `ChatRoomHandler` block indefinitely on `ReadJSON`, so they never exit and the process hangs. On shutdown, the server explicitly iterates all rooms and calls `conn.Close()` on every WebSocket, unblocking all `ReadJSON` calls before handing off to `Shutdown`.

### 9. In-Order User Removal via Broadcast Channel

In Assignment 1, `RemoveUserFromRoom` closed the client's `Send` channel immediately when a LEAVE message arrived. This caused a teardown race: the broadcast goroutine could be mid-fan-out when the channel was closed, causing missed final messages or a panic.

**Fix**: LEAVE no longer closes the channel directly. Instead, a `roomEvent{removeUserId: userId}` is enqueued into the room's `Broadcast` channel. The broadcast goroutine processes this event after all preceding messages have been fanned out, guaranteeing that the client's Send channel is only closed after all in-flight messages are delivered.

### 10. Circuit Breaker for RabbitMQ Disconnections

A circuit breaker using the `gobreaker` library handles transient RabbitMQ connection failures. The custom three-state implementation (`CircuitClosed` → `CircuitBuffering` → `CircuitOpen`) was replaced with `gobreaker` to get well-tested state transitions, configurable thresholds, and automatic half-open probing out of the box. When the circuit opens, publish calls fail fast rather than blocking the WebSocket read loops indefinitely.

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
- **20 publish worker goroutines** (new) — drain `publishChan`, each owns one AMQP publisher
- **N consumer goroutines** (new, one per room) — receive from RabbitMQ, call async Accept
- **1 state watcher goroutine** (new) — monitors AMQP connection state, drives circuit breaker
- **1 monitor goroutine** — logs throughput and RabbitMQ drop stats every interval

For 1,000 connections across 20 rooms (50 users/room):
- Assignment 1: `2 × 1000 + 20 = 2,020` goroutines
- Assignment 2: `2 × 1000 + 20 + 20 + 20 + 1 + 1 = 2,062` goroutines (overhead is minimal)

---

## Infrastructure Changes

| Aspect | Assignment 1 | Assignment 2 |
|--------|-------------|-------------|
| Server instances | 1 × t3.micro | 1–4 × t3.small |
| Instance size | t3.micro (1 GB RAM) | t3.small (2 GB RAM) |
| Load balancer | None | AWS ALB (port 80 → port 3000) |
| Message broker | None | RabbitMQ 4.0 on t3.micro |
| Client → server | Direct connection | ALB with session stickiness |
| Per-client send buffer | 2,048 | 120,000 |
| File descriptor limit | 1,024 (default) | 65,535 (systemd `LimitNOFILE`) |

The per-client buffer increase (2,048 → 120,000) was necessary because with RabbitMQ fan-out, each message now generates N-1 consumed copies delivered to local clients. With 50 users per room, each TEXT message generates 49 inbound deliveries from RabbitMQ, all competing to write to each client's `Send` channel simultaneously. The larger buffer absorbs this burst rather than dropping messages.

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

**2 → 4 servers (25% regression)**: Throughput drops because each message must now fan out to 4 queues instead of 2. The per-message RabbitMQ overhead increases linearly with server count. The bottleneck shifts from CPU contention (per-server) to RabbitMQ fan-out overhead (shared). With 4 servers, a single message published to `chat.exchange` is routed to 4 queues and consumed by 4 servers, quadrupling cross-server traffic compared to 1 server.

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
| 9 | 512 publish workers: excessive memory pressure | Tuned down: 512 → 200 → 30 → 20 workers |
| 10 | `Publish()` dropped messages silently when channel full | Changed to blocking with ctx — propagates back-pressure |
| 11 | Synchronous `Accept()` inside receive loop — exhausted AMQP credits | Moved `Accept()` into a separate goroutine per delivery |

---

## Part 4 — System Tuning

### Tuning Targets

- Queue depth < 1,000 messages
- Consumer lag < 100ms
- 0 failed messages
- 0 failed connections

All targets were met across all valid runs (22 total). Full run-by-run log in [TUNING.md](TUNING.md).

---

### Worker Sweep (`PUBLISH_WORKERS`, Pool=1000, single t3.small)

| Workers | Throughput | Peak Queue | Meets Target |
|---------|------------|------------|--------------|
| 15 | 1,982 msg/s | ~20 | ✓ |
| 20 | 2,091 msg/s | ~20 | ✓ ← optimal |
| 40 | 2,079 msg/s | ~35 | ✓ |
| 80 | 2,037 msg/s | ~10,000 | ✗ |

**Finding**: 20 workers is the sweet spot. Fewer workers under-saturate AMQP publishers; more workers flood RabbitMQ with concurrent publishes, spiking queue depth and risking credit exhaustion deadlock.

---

### Client Pool Sweep (`PoolSize`, Workers=20, single t3.small)

The `PoolSize` semaphore limits how many WebSocket connections are open simultaneously. Smaller values stagger the connection ramp-up, reducing burst pressure on both the server and RabbitMQ.

| PoolSize | Throughput | Mean Latency | Peak Queue |
|----------|------------|--------------|------------|
| 16 | 4,216 msg/s | 917ms | ~30 |
| 32 | 3,881 msg/s | 2,076ms | ~25 |
| 64 | 3,512 msg/s | 4,372ms | ~45 |
| 128 | 3,278 msg/s | 9,543ms | ~35 |
| 256 | 2,888 msg/s | 20,771ms | ~35 |
| 512 | 2,421 msg/s | 47,776ms | ~22 |
| 1,000 | 2,091 msg/s | 114,804ms | ~20 |

**Finding**: Smaller pool = dramatically better throughput and latency. Pool=16 gives 4,216 msg/s (2× better than Pool=1000) because serializing connection ramp-up prevents simultaneous burst that would overwhelm the server's `publishChan` and RabbitMQ simultaneously. Pool=512+ has the flattest queue graph (all connections open at once → steady-state publish rate), but at significant throughput and latency cost.

**Chosen config**: Pool=64 — best balance of throughput (3,512 msg/s), manageable latency (4.4s mean), and stable queue graph.

---

### 2-Server Horizontal Scaling (ALB, Workers=20/server)

| PoolSize | Servers | Instance | Throughput | Mean Latency | Peak Queue |
|----------|---------|----------|------------|--------------|------------|
| 1,000 | 1 | t3.small | 2,091 msg/s | 114,804ms | ~20 |
| 1,000 | 2 | t3.small | 2,054 msg/s | 114,410ms | ~300 |
| 256 | 1 | t3.small | 2,888 msg/s | 20,771ms | ~35 |
| 256 | 2 | t3.micro | 2,904 msg/s | 21,335ms | ~100 |
| 64 | 1 | t3.small | 3,512 msg/s | 4,372ms | ~45 |
| 64 | 2 | t3.micro | 3,708 msg/s | 4,182ms | ~130 |

**Finding**: Horizontal scaling provides negligible benefit in this architecture. With RabbitMQ fan-out, adding a second server doubles the total publisher count (20 → 40 AMQP senders), doubling queue pressure on the broker. This offsets any per-server CPU savings. The best 2-server result (Pool=64, +5.6% over single server) was limited by the client being the bottleneck — the pool size constrained connection ramp-up so both servers weren't fully loaded.

**Root cause**: In a fan-out architecture where every message is broadcast to all room members, the broker is a shared bottleneck. Doubling servers doubles publish throughput to the broker, but the broker must still route and deliver each message to all consumers — so per-message broker work scales linearly with server count.

**Additional test**: Pool=1000 on 2×t3.micro crashed both instances (OOM — 500 concurrent WebSocket connections per instance exceeds 1 GB RAM). Upgrading to t3.small confirmed the architectural constraint: 2 t3.small servers at Pool=1000 matched single-server throughput exactly (-1.8%), with queue spiking to ~300 (15× the single-server ~20).

---

### Fixed-Config 1/2/4 Server Scaling (Pool=1000, Workers=5/server)

To isolate the effect of server count under identical per-server configuration, three runs were executed with the same settings across 1, 2, and 4 servers. `Workers=5/server` was chosen to keep total publishers proportional (5/10/20) and within the safe queue zone across all configurations.

| Servers | Total publishers | Runtime | Throughput | vs 1 server | Mean latency | Peak queue |
|---------|-----------------|---------|------------|-------------|--------------|------------|
| 1 | 5 | 317.2s | 1,582.7 msg/s | — | 156,188ms | ~10 |
| 2 | 10 | 328.3s | 1,529.0 msg/s | -3.4% | 152,741ms | ~80–100 |
| 4 | 20 | 538.1s | 933.0 msg/s | -41.0% | 254,301ms | ~130 |

**Finding**: Throughput degrades monotonically as server count increases. With 4 servers, throughput is 41% lower than 1 server and latency is 63% higher. The queue depth increase from ~10 (1 server) to ~130 (4 servers) confirms that RabbitMQ fan-out overhead — not per-server CPU — is the binding constraint.

**Mechanism**: With N servers, each published message must be routed to N queues (one per server per room) and consumed by N consumers simultaneously. Doubling server count doubles the AMQP publish fan-out work per message. With 20 workers × 20 rooms × 4 servers, the broker handles 4× the routing load of a single server — RabbitMQ saturates before the chat servers do.

**Implication**: Fan-out architectures with large room sizes do not benefit from horizontal scaling at the broker layer. Scaling chat servers adds publishers faster than it adds consumers, worsening the sender-side bottleneck on every additional server.

---

### Final Tuned Configuration

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| `PUBLISH_WORKERS` | 20 | Optimal from worker sweep — saturates broker without flooding |
| `publishChanSize` | 500 | Small buffer ensures back-pressure reaches WebSocket read loops quickly |
| `PoolSize` (client) | 64 | Best throughput/latency tradeoff; serializes connection ramp-up |
| `InitialCredits` | 1,000 | Per-consumer AMQP credit — must exceed burst delivery rate |
| `bufferSize` (send channel) | 120,000 | Absorbs fan-out burst (50 users/room × 500 msgs × safety margin) |
| Chat server | t3.small (1 instance) | Single server optimal; 2nd server adds queue pressure with minimal gain |
| RabbitMQ | t3.micro | Sufficient for 20 workers, 20 consumers, <1,000 queue depth |

**Result**: 502,000 messages, 0 failures, 3,512 msg/s, mean latency 4.4s, peak queue ~45.
