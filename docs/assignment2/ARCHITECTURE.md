# ChatFlow v2 — Architecture

## System Architecture

```
                        ┌─────────────────────────────────────────────────┐
                        │                 AWS us-west-2                   │
                        │                                                 │
  ┌──────────┐          │  ┌──────────────────────────────────────────┐   │
  │  Client  │──port 80─►  │  Application Load Balancer (ALB)         │   │
  │  (local) │          │  │  Sticky sessions (LB cookie, 1 day)      │   │
  └──────────┘          │  └────────┬──────────┬──────────┬───────────┘   │
                        │           │          │          │               │
                        │        :3000      :3000      :3000              │
                        │    ┌───▼────┐ ┌───▼────┐ ┌───▼────┐           │
                        │    │Server 1│ │Server 2│ │Server N│           │
                        │    │t3.small│ │t3.small│ │t3.small│           │
                        │    └───┬────┘ └───┬────┘ └───┬────┘           │
                        │        │          │          │                 │
                        │        └──────────┼──────────┘                │
                        │                  │ :5672 (AMQP 1.0)           │
                        │           ┌──────▼──────┐                     │
                        │           │  RabbitMQ   │                     │
                        │           │  t3.micro   │                     │
                        │           │  (topic     │                     │
                        │           │  exchange)  │                     │
                        │           └─────────────┘                     │
                        └─────────────────────────────────────────────────┘
```

Each chat server maintains:
- WebSocket connections to all clients assigned to it by the ALB
- One AMQP publisher connection (20 publish workers)
- One AMQP consumer connection (N consumers, one per room)

---

## Message Flow Sequence

### Happy Path

```
Client A          Server 1          RabbitMQ           Server 2          Client B
(room 5)                          chat.exchange        (room 5)          (room 5)
   │                  │                  │                  │                │
   │── TEXT msg ──►   │                  │                  │                │
   │                  │── local fan-out ─────────────────────────────────►   │
   │                  │  (to all Server 1 clients in room 5)                 │
   │                  │                  │                  │                │
   │                  │── Publish ──────►│                  │                │
   │                  │  routing key:    │                  │                │
   │                  │  "room.5"        │                  │                │
   │                  │                  │── deliver ──────►│                │
   │                  │                  │  (queue:         │                │
   │                  │                  │  room.5.srv2)    │                │
   │                  │                  │                  │── fan-out ─►   │
   │                  │                  │                  │  (all Server 2 │
   │◄── echo ─────────│                  │                  │  clients       │
   │  (local path)    │                  │◄── Accept() ─────│  in room 5)    │
   │                  │                  │  (async goroutine)│               │
```

**Self-filter**: Server 1 also consumes from its own queue (`room.5.srv1`). Each `QueueMessage` carries the publishing server's `ServerID`. On consume, the server checks `queueMsg.ServerID == s.ID` and skips the message if it matches — preventing double-delivery to local clients.

---

### Error / Failure Paths

#### 1. Client Disconnects Unexpectedly (Read Error)

```
Client A          Server 1                              Client B
(room 5)          (room 5)                              (room 5)
   │                  │                                     │
   │  [TCP drop]      │                                     │
   │                  │  ReadJSON returns error             │
   │                  │  ├── synthetic LEAVE msg created    │
   │                  │  ├── broadcastAndPublish(LEAVE) ──►  │  (others notified)
   │                  │  └── scheduleRemoveUser(A)          │
   │                  │      │                              │
   │                  │      └── enqueue removeUserId       │
   │                  │          into room.Broadcast        │
   │                  │          (processed after LEAVE     │
   │                  │           echo is fanned out)       │
   │                  │  close(A.Send)                      │
   │                  │  delete Users[A]                    │
```

#### 2. Slow / Dead Client — Write Deadline Exceeded

```
Client A (slow)   Server 1
   │                  │
   │  [TCP stall]     │
   │                  │  handleClientWrites:
   │                  │  SetWriteDeadline(now + 15s)
   │                  │  WriteJSON → timeout error
   │                  │  conn.Close()  ──────────────────► unblocks ReadJSON
   │                  │                                     in ChatRoomHandler
   │                  │  ReadJSON returns error
   │                  │  └── synthetic LEAVE + scheduleRemoveUser
```

#### 3. Full Client Send Channel — Message Dropped

```
Server 1 broadcast goroutine
   │
   ├── fan-out loop over room.Users:
   │     ├── User B:  u.Send <- resp  ✓ (delivered)
   │     ├── User C:  u.Send <- resp  ✓ (delivered)
   │     └── User A:  u.Send full (writer goroutine behind)
   │                  select { default: DroppedSend.Add(1) }
   │                  message DROPPED for A — broadcast goroutine not blocked
```

#### 4. Full Broadcast Channel — Message Dropped

```
WebSocket read goroutine (Client A)
   │
   │  broadcastAndPublish():
   │  select {
   │  case room.Broadcast <- event:   ✓ enqueued
   │  default:                        DroppedBroadcast.Add(1)
   │  }                               message DROPPED — read loop not blocked
   │
   │  (RabbitMQ publish still proceeds regardless)
```

#### 5. RabbitMQ Publish Failure — Circuit Breaker

```
publishWorker goroutine
   │
   │  cb.Execute(publisher.Publish)
   │     │
   │     ├── success: message delivered to chat.exchange  ✓
   │     │
   │     ├── failure (≥5 consecutive):
   │     │   circuit → OPEN
   │     │   publish calls fail fast (no AMQP attempt)
   │     │   message → tempBuffer (small secondary buffer)
   │     │   if tempBuffer full: DroppedMessages.Add(1)
   │     │
   │     └── after 30s timeout: circuit → HALF-OPEN
   │         single probe attempt
   │         success → CLOSED; drain tempBuffer → publishChan
   │         failure → OPEN again
   │
   │  Publish() blocks caller (WebSocket read loop)
   │  if publishChan full → TCP back-pressure to client
```

#### 6. LEAVE Race — In-Order Removal via Broadcast Channel

```
room.Broadcast (FIFO):
  [M998 event] ──► [M999 event] ──► [LEAVE echo] ──► [removeUser A]
       ↓                ↓                 ↓                  ↓
   fan-out A,B      fan-out A,B       fan-out A,B        close(A.Send)
   (A still in      (A still in       (A still in        delete Users[A]
    Users map)       Users map)        Users map)

Without serialization (naive close):
  [M998] ──► [M999] ... close(A.Send) ← race here → panic or missed messages
```

---

## Queue Topology

```
                      chat.exchange (topic)
                      ┌─────────────────────────────────────────────────┐
                      │                                                 │
     Binding Keys:  room.1  room.2  room.3  ...  room.N                │
                      │                                                 │
                      └──────────────────┬──────────────────────────────┘
                                         │ routes to all matching queues
              ┌──────────────────────────┼──────────────────────────────┐
              │                          │                              │
      Server 1 queues            Server 2 queues              Server N queues
      ┌────────────────┐         ┌────────────────┐         ┌────────────────┐
      │ room.1.srv1    │         │ room.1.srv2     │         │ room.1.srvN    │
      │ room.2.srv1    │         │ room.2.srv2     │         │ room.2.srvN    │
      │ room.3.srv1    │         │ room.3.srv2     │         │ room.3.srvN    │
      │   ...          │         │   ...           │         │   ...          │
      │ room.N.srv1    │         │ room.N.srv2     │         │ room.N.srvN    │
      └────────────────┘         └────────────────┘         └────────────────┘
      (auto-delete)              (auto-delete)              (auto-delete)
```

**Queue naming**: `room.<roomId>.<serverID>` — unique per server per room.

**Auto-delete**: Queues are declared with `IsAutoDelete: true`. When the server disconnects (and its AMQP consumer connection closes), all its queues are deleted automatically. This prevents stale queue accumulation across restarts.

**Fan-out math**: A single message published to `chat.exchange` with routing key `room.5` is delivered to `N` queues (one per server). With 4 servers and 20 rooms, there are 80 queues total.

---

## Consumer Threading Model

```
NewRabbitMQ() (called at server startup)
│
├── consumerConn (AMQP connection — consumers only)
│
├── for each room 1..N:
│   └── go consumerLoop(consumer, roomId)          ← N goroutines, run for server lifetime
│       │
│       │  loop:
│       ├── delivery, err = consumer.Receive(ctx)  ← blocks until message arrives
│       │
│       ├── handlersMu.RLock()
│       │   h = handlers[roomId]                   ← handler registered later by Consume()
│       │   handlersMu.RUnlock()
│       │
│       ├── measure consumer lag                   ← always runs (even if h == nil)
│       │   parse Timestamp from raw JSON
│       │   totalLagMs.Add(time.Since(t).Ms())
│       │   lagSampleCount.Add(1)
│       │
│       ├── if h != nil: h(data)                  ← calls consumeMessages() in server.go
│       │       └── unmarshal QueueMessage
│       │           if ServerID == myID: skip      ← self-filter
│       │           else: room.Broadcast <- event  ← enqueue for local fan-out
│       │
│       └── go d.Accept(ctx)                       ← async: return credit to broker immediately
│                                                     (does NOT block the receive loop)
│
└── broadcast goroutine (one per room, in server.go)
    │
    loop:
    ├── event = <-room.Broadcast
    │   ├── if event.resp != nil:
    │   │   └── fan out to all room.Users[*].Send channels
    │   └── if event.removeUserId != "":
    │       └── close Send channel + delete from Users map (in-order removal)
    └── <-room.Ctx.Done():
        ├── user.Conn.Close()   ← unblocks ReadJSON in handler goroutines
        └── close(user.Send)   ← signals writer goroutine to exit
        return
```

**Why async Accept**: AMQP 1.0 uses credit-based flow control. The broker delivers at most `InitialCredits` (1,000) messages before waiting for acknowledgments. If `Accept()` is called synchronously inside the receive loop, it blocks the loop while the ack round-trips to the broker. With 20 consumer goroutines × 1,000 credits = 20,000 messages delivered but not yet acked — the broker stops sending. By calling `Accept()` in a separate goroutine, the receive loop immediately calls `Receive()` again, keeping credits flowing continuously.

**Handler registration**: Consumer goroutines are started before any clients connect. When `AddNewRoom(roomId)` is first called (on the first client JOIN), `Consume()` registers the handler. Messages that arrive before registration are still accepted (preventing stale Unacked) but not forwarded.

---

## Load Balancing Configuration

```
                ┌────────────────────────────────────────────┐
                │          AWS Application Load Balancer      │
                │                                            │
                │  Listener: port 80 (HTTP)                  │
                │  Target group: port 3000 (HTTP)            │
                │  Health check: GET /health → 200 OK        │
                │  Stickiness: enabled (LB cookie, 1 day)    │
                └──────────────────┬─────────────────────────┘
                                   │
              ┌────────────────────┼────────────────────┐
              │                    │                    │
        ┌─────▼─────┐        ┌─────▼─────┐       ┌─────▼─────┐
        │ Server 1  │        │ Server 2  │       │ Server N  │
        │ :3000     │        │ :3000     │       │ :3000     │
        └───────────┘        └───────────┘       └───────────┘
```

**Sticky sessions (required for WebSockets)**: WebSocket connections are long-lived. Without stickiness, subsequent HTTP requests from the same client (e.g., reconnects) could be routed to a different server, losing the connection state. The ALB `AWSALB` cookie pins each client to one backend for 1 day.

**Health check path**: `GET /health` returns `200 OK` immediately. The ALB marks a target unhealthy after 2 consecutive failures and stops routing new connections to it. Existing sticky sessions may still route to the unhealthy target until the cookie expires or the client reconnects.

**Port mapping**: ALB listens on port 80 (standard HTTP, no root privileges required on the client side). Chat servers listen on port 3000 (ec2-user cannot bind ports < 1024 without elevated privileges).

---

## Failure Handling Strategies

### 1. RabbitMQ Connection Failures — Circuit Breaker (`gobreaker`)

The `Rabbit` struct uses the `gobreaker` library to wrap AMQP publish calls. `gobreaker` provides a standard three-state circuit breaker (Closed → Open → Half-Open) with configurable failure thresholds and automatic half-open probing:

```
                    ┌─────────────┐
                    │   CLOSED    │  publish calls pass through normally
                    │             │
                    └──────┬──────┘
                           │ consecutive failures exceed threshold
                           ▼
                    ┌─────────────┐
                    │    OPEN     │  publish calls fail fast (no AMQP attempt)
                    │             │
                    └──────┬──────┘
                           │ timeout expires → probe attempt
                           ▼
                    ┌─────────────┐
                    │  HALF-OPEN  │  single probe; success → Closed, fail → Open
                    └─────────────┘
```

Both AMQP connections (`conn` and `consumerConn`) are monitored via `NotifyStatusChange`. When the circuit opens, publish calls return immediately rather than blocking the WebSocket read loops.

### 2. Slow / Dead WebSocket Clients — Write Deadline

```go
client.Conn.SetWriteDeadline(time.Now().Add(15 * time.Second))
err := client.Conn.WriteJSON(resp)
if err != nil {
    client.Conn.Close()   // unblocks ReadJSON in the reader goroutine
}
```

If a client's TCP connection is slow or dead, writes stall. Without a deadline, the writer goroutine blocks indefinitely, holding the connection's goroutines and Send channel slot. The 15-second deadline bounds the maximum time a dead client can occupy resources. On deadline expiry, `WriteJSON` returns an error, the connection is closed, and both the reader and writer goroutines exit cleanly.

### 3. Full Client Send Channels — Drop with Counter

Each client has a buffered `Send` channel (capacity 120,000). If the broadcast goroutine tries to send to a client whose `Send` channel is full (the writer goroutine is behind), the message is dropped rather than blocking the broadcast goroutine (which serves all users in the room):

```go
select {
case u.Send <- event.resp:
default:
    r.DroppedSend.Add(1)
    log.Printf("Dropping message for user [%s]...", u.UserID)
}
```

The drop count is exposed via `GET /metrics`. The large buffer size (120k vs 2,048 in Assignment 1) was chosen specifically to absorb the burst of N-1 cross-server fan-out deliveries per message.

### 4. Simplified JOIN/LEAVE — One Per Connection

Each WebSocket connection performs exactly one JOIN on open and one LEAVE on close. There is no reconnection tracking, session state, or re-join logic. When a client disconnects, its user is removed from the room and all resources are released. If the client reconnects, it opens a new WebSocket connection and sends a fresh JOIN.

This was a deliberate simplification: session persistence was evaluated and removed because it added complexity (tracking user sessions across reconnects, deduplicating JOINs) without measurable benefit for the load test workload. The ALB's sticky session cookie ensures reconnects land on the same server, so state loss on reconnect is not a concern in the test setup.

### 5. In-Order LEAVE Processing — Broadcast Queue Serialization

A LEAVE message triggers user removal. Removing the user immediately (closing `Send`) would race with in-flight broadcast messages queued before the LEAVE arrived, causing those messages to be dropped.

**Fix**: The LEAVE handler enqueues a `roomEvent{removeUserId: userId}` into `room.Broadcast` instead of closing the channel directly. The broadcast goroutine processes this event only after all preceding broadcast events have been fanned out, guaranteeing the user receives all messages sent before their LEAVE.

### 6. Server Shutdown — Force-Close WebSockets

`http.Server.Shutdown()` gracefully drains active HTTP requests but skips hijacked connections (WebSockets). Reader goroutines blocked on `ReadJSON` never unblock, so the process hangs indefinitely.

**Fix**: Before calling `Shutdown`, the server iterates all rooms and explicitly calls `conn.Close()` on every WebSocket:

```go
for _, room := range s.Rooms {
    for _, user := range room.Users {
        user.Conn.Close()   // unblocks ReadJSON in all handler goroutines
    }
}
httpServer.Shutdown(shutdownCtx)
```

### 7. Full publishChan — Back-Pressure to WebSocket Read Loop

If the 20 publish workers cannot drain `publishChan` fast enough, `Publish()` blocks the calling WebSocket read goroutine. This propagates back-pressure through the TCP stack to the client, slowing its send rate naturally. The alternative (dropping messages when the channel is full) was rejected because it silently loses cross-server deliveries.

The `publishChanSize=500` cap keeps back-pressure tight: the channel fills quickly under burst load, causing `Publish()` to block and propagate flow control to the WebSocket read loops before RabbitMQ queue depth has a chance to spike.
