# ChatFlow Threading Model

## Server Goroutines

Each WebSocket connection spawns **2 goroutines** on the server — one for reading and one for writing. They communicate through a buffered `Send` channel per client.

```
HTTP Request (per user per room)
│
├── ChatRoomHandler goroutine (reader)
│   │   reads JSON from WebSocket in a loop
│   │   routes by message type: JOIN → TEXT → LEAVE
│   │   pushes *Response into client.Send channel
│   │   on LEAVE: calls RemoveUserFromRoom() → closes Send channel
│   │   waits for writer to finish (<-writeDone)
│   │
│   └── handleClientWrites goroutine (writer)
│       drains client.Send channel
│       writes JSON responses back to WebSocket
│       exits when Send is closed or room context is cancelled
│
│  Shared state:
│  ├── Server.Rooms map[string]*Room  (protected by Server.Mu RWMutex)
│  └── Room.Users   map[string]*Client (protected by Room.Mu RWMutex)
│  └── Stats.SuccessfulRequests / FailedRequests (atomic counters)
```

For **N users × M rooms**, the server runs **2×N×M goroutines** plus the main HTTP listener.

## Client Goroutines (Pipeline)

The pipeline client uses **3 stages** connected by channels, with a connection pool semaphore limiting concurrency.

```
main goroutine
│
├── go GenerateMessages()                          ← 1 coordinator goroutine
│   ├── go GenerateConnElements(user-1)            ← N user goroutines
│   ├── go GenerateConnElements(user-2)               each creates M connections
│   ├── ...                                            (blocks on pool semaphore)
│   └── go GenerateConnElements(user-N)
│       │
│       │  Pool.Sem ← chan struct{} (bounded to PoolSize)
│       │  Pool.GetOrCreateNewWsClient():
│       │    acquire semaphore → dial WebSocket → start reader goroutine
│       │
│       └──► roomChan (buffered channel of *ConnElement)
│
├── go CollectMetrics(ctx)                         ← 1 goroutine: drains metricChan → CSV
│
├── WriteMessages(ctx)                             ← runs on main goroutine
│   │  for room := range roomChan:
│   │
│   └── go func(room)                             ← N×M writer goroutines
│       ├── go reader()                            ← N×M reader goroutines
│       │   reads from WsClient.Send channel
│       │   computes latency per message
│       │   sends *RoomStats to statsChan
│       │   sends *Metric to metricChan
│       │
│       ├── writer: sends JOIN + TEXT×K + LEAVE
│       └── <-done → Pool.Remove() → release semaphore
│
├── Wg.Wait()
├── CloseChannels()                                ← closes metricChan + statsChan
├── GetPerformanceMetricsSummary()
└── GetOverAllStats()                              ← drains statsChan, prints summary
```

For **N users × M rooms** with **K messages each**, the pipeline spawns:
- **N** generator goroutines (Stage 1)
- **N×M** writer + **N×M** reader goroutines (Stage 2)
- **N×M** WebSocket reader goroutines (inside WsClient)
- **1** metric collector goroutine (Stage 3)
- **Total: ~3×N×M + N + 2 goroutines**

## Client Goroutines (Fan-Out)

The fan-out client is simpler — each user goroutine owns its full lifecycle sequentially across rooms.

```
main goroutine
│
├── go func(user-1)                                ← N user goroutines
│   ├── for each room:
│   │   ├── Pool.GetOrCreateNewWsClient()          ← acquire semaphore + dial
│   │   ├── go reader()                            ← reads WsClient.Send, counts msgs
│   │   ├── Write JOIN
│   │   ├── go writer()                            ← sends K TEXT messages
│   │   ├── <-doneCh
│   │   ├── Write LEAVE
│   │   ├── select { <-msgDone | <-timeout }
│   │   ├── Pool.Remove()                          ← close + release semaphore
│   │   └── <-readerDone
│   └── wg.Done()
│
├── go func(user-2)
├── ...
└── wg.Wait()
```

## Synchronization Primitives

| Primitive | Location | Purpose |
|-----------|----------|---------|
| `Pool.Sem` (buffered chan) | Client | Limits concurrent WebSocket connections to PoolSize |
| `Pool.Mu` (RWMutex) | Client | Protects the connection map (`Conns`) |
| `Server.Mu` (RWMutex) | Server | Protects the rooms map (`Rooms`) |
| `Room.Mu` (RWMutex) | Server | Protects the users map within a room |
| `WsClient.WriteMu` (Mutex) | Client | Serializes WebSocket writes (gorilla requires it) |
| `WsClient.closeOnce` (sync.Once) | Client | Prevents double-close panic on Send channel |
| `atomic.Int64` | Both | Lock-free counters for messages, connections, stats |
| `roomChan` (buffered chan) | Client | Decouples Stage 1 (generate) from Stage 2 (write/read) |
| `metricChan` (buffered chan) | Client | Decouples Stage 2 from Stage 3 (CSV collection) |
| `statsChan` (buffered chan) | Client | Carries per-room stats to final aggregation |
