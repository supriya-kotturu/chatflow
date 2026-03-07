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
                        │    │t3.micro│ │t3.micro│ │t3.micro│           │
                        │    └───┬────┘ └───┬────┘ └───┬────┘           │
                        │        │          │          │                 │
                        │        └──────────┼──────────┘                │
                        │                  │ :5672 (AMQP 1.0)           │
                        │           ┌──────▼──────┐                     │
                        │           │  RabbitMQ   │                     │
                        │           │  t3.medium  │                     │
                        │           │  (topic     │                     │
                        │           │  exchange)  │                     │
                        │           └─────────────┘                     │
                        └─────────────────────────────────────────────────┘
```

Each chat server maintains:
- WebSocket connections to all clients assigned to it by the ALB
- One AMQP publisher connection (30 publish workers)
- One AMQP consumer connection (N consumers, one per room)

---

## Message Flow Sequence

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
│       ├── lookup handlers[roomId]                ← handler registered later by Consume()
│       │   if handler != nil: handler(data)       ← calls consumeMessages() in server.go
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
    └── <-room.Ctx.Done(): close all Send channels + return
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

### 1. RabbitMQ Connection Failures — Circuit Breaker

The `Rabbit` struct implements a three-state circuit breaker driven by AMQP connection state events:

```
                    ┌─────────────┐
          conn ok   │             │  conn ok +
          ─────────►│   CLOSED    │◄─ flush buffer
                    │  (publish   │
                    │  directly)  │
                    └──────┬──────┘
                           │ StateReconnecting
                           ▼
                    ┌─────────────┐
                    │  BUFFERING  │  publish to tempBuffer (cap 2048)
                    │             │─────────────────────────────────►  StateClosed
                    └──────┬──────┘
                           │ buffer full
                           │ or StateClosed/Closing
                           ▼
                    ┌─────────────┐
                    │    OPEN     │  drop messages, log error
                    └─────────────┘
```

Both AMQP connections (`conn` and `consumerConn`) are monitored. Failure of either drives the circuit to `Buffering` or `Open`. On reconnection (`StateOpen`), buffered messages from `tempBuffer` are flushed before normal publishing resumes.

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

Each client has a buffered `Send` channel (capacity 60,000–120,000). If the broadcast goroutine tries to send to a client whose `Send` channel is full (the writer goroutine is behind), the message is dropped rather than blocking the broadcast goroutine (which serves all users in the room):

```go
select {
case u.Send <- event.resp:
default:
    r.DroppedSend.Add(1)
    log.Printf("Dropping message for user [%s]...", u.UserID)
}
```

The drop count is exposed via `GET /metrics`. The large buffer size (60k–120k vs 2,048 in Assignment 1) was chosen specifically to absorb the burst of N-1 cross-server fan-out deliveries per message.

### 4. In-Order LEAVE Processing — Broadcast Queue Serialization

A LEAVE message triggers user removal. Removing the user immediately (closing `Send`) would race with in-flight broadcast messages queued before the LEAVE arrived, causing those messages to be dropped.

**Fix**: The LEAVE handler enqueues a `roomEvent{removeUserId: userId}` into `room.Broadcast` instead of closing the channel directly. The broadcast goroutine processes this event only after all preceding broadcast events have been fanned out, guaranteeing the user receives all messages sent before their LEAVE.

### 5. Server Shutdown — Force-Close WebSockets

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

### 6. Full publishChan — Back-Pressure to WebSocket Read Loop

If the 30 publish workers cannot drain `publishChan` fast enough, `Publish()` blocks the calling WebSocket read goroutine. This propagates back-pressure through the TCP stack to the client, slowing its send rate naturally. The alternative (dropping messages when the channel is full) was rejected because it silently loses cross-server deliveries.

The `publishChanSize=4,000` cap keeps the back-pressure tight enough to prevent unbounded memory growth while loose enough to absorb short bursts.
