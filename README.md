# ChatFlow

A real-time chat application built with Go and WebSockets. Includes a load-testing client that measures latency, throughput, and message distribution across concurrent users and rooms.

## Architecture

```
client/          Load-testing client (connects N users to M rooms)
  ws/            WebSocket client, connection pool, stats collection
server/          WebSocket server
  internal/      Room management, handlers, upgrader
  html/          Static frontend (HTML/CSS/JS)
pkg/
  models/        Shared types (Message, Response, Metric, RoomStats)
  generate/      Test data factories (users, rooms, messages)
  env/           .env loader
  utils/         CSV writer
results/         Output metrics CSV and Jupyter notebook for analysis
```

Each user gets one WebSocket connection per room. The server echoes messages back with a server timestamp, and the client measures round-trip latency.

## Load Test Patterns

The client supports two concurrency patterns:

### Fan-Out (`RunFanOutLoadTest`)

Each user goroutine owns its full lifecycle — connections, writes, reads, and cleanup. Simple, no shared state between users.

```
main
 ├── goroutine(user-1)
 │    ├── connect(room-1) → JOIN → write 1000 msgs → LEAVE → wait → close
 │    ├── connect(room-2) → JOIN → write 1000 msgs → LEAVE → wait → close
 │    └── ...
 ├── goroutine(user-2)
 │    ├── connect(room-1) → ...
 │    └── ...
 └── wg.Wait()
```

### Pipeline (`RunPipelineLoadTest`)

Three decoupled stages connected by channels. Separates connection setup from message I/O, and enables metric collection.

```
Stage 1: Generate               Stage 2: Write + Read          Stage 3: Collect
┌─────────────────┐             ┌─────────────────────┐        ┌──────────────┐
│ goroutine/user  │             │ goroutine/room      │        │              │
│                 │  roomChan   │  ┌── writer ──────┐ │        │              │
│ create conns +  ├────────────►│  │ send msgs      │ │        │              │
│ pre-gen msgs    │             │  └────────────────┘ │ metric │  write to    │
│                 │             │  ┌── reader ──────┐ ├───────►│  CSV file    │
│                 │             │  │ collect latency │ │  Chan  │              │
└─────────────────┘             │  │ compute stats  │ │        │              │
                                │  └────────────────┘ │        └──────────────┘
                                └─────────────────────┘
                                         │
                                    statsChan
                                         │
                                         ▼
                                ┌─────────────────────┐
                                │  GetOverAllStats()   │
                                │  mean/median/p95/p99 │
                                │  per-room breakdown  │
                                └─────────────────────┘
```

## Prerequisites

- Go 1.23+
- Python 3 + matplotlib (optional, for charts)

## Setup

```bash
git clone <repo-url>
cd chat-flow
```

Create a `.env` file in the project root:

```
NAME="DEVELOPMENT"
PORT=3000
```

## Running

### Build

```bash
make build
```

### Start the server

```bash
make run-server
```

The server listens on the port from `.env` (default 3000). Open `http://localhost:3000` for the web UI.

### Run the load-testing client

In a separate terminal:

```bash
make run-client
```

Default config (in `client/main.go`):

| Parameter      | Value | Description                          |
|----------------|-------|--------------------------------------|
| PoolSize       | 320   | Max concurrent WebSocket connections |
| UserCount      | 32    | Number of simulated users            |
| MessageCount   | 1000  | Messages per user per room           |
| RoomCount      | 10    | Rooms each user joins                |
| MessageBuffer  | 1250  | Channel buffer size                  |
| CollectMetrics | true  | Write per-message CSV metrics        |

The client prints per-room and aggregate stats on completion:

```
Mean Latency across all rooms: 609ms
Median Latency across all rooms: 614ms
95th Percentile Latency across all rooms: 870ms
...
Room 5 | users: 32 | throughput: 4821.3 msg/s | mean latency: 602ms | median latency: 611ms
```

### Analyze results

Metrics are written to `results/metrics.csv`. Open the Jupyter notebook:

```bash
cd results
jupyter notebook metrics.ipynb
```

## Makefile targets

| Target         | Description                     |
|----------------|----------------------------------|
| `make build`   | Build server and client binaries |
| `make run-server` | Build and start the server    |
| `make run-client` | Build and run the load client |
| `make test`    | Run all tests                    |
| `make fmt`     | Format all Go files              |
| `make vet`     | Vet all Go files                 |
| `make clean`   | Remove built binaries            |

## Project structure

```
.
├── Makefile
├── .env
├── go.mod
├── client/
│   ├── main.go              # Entry point (RunFanOutLoadTest / RunPipelineLoadTest)
│   └── ws/
│       ├── client.go         # Load test orchestration and stats
│       ├── wsClient.go       # Single WebSocket connection wrapper
│       └── pool.go           # Connection pool with semaphore
├── server/
│   ├── main.go              # Entry point
│   ├── html/                # Static frontend
│   └── internal/server/
│       ├── server.go         # Room/client management
│       ├── chatRoomHandler.go # WebSocket lifecycle handler
│       ├── upgrader.go       # WebSocket upgrader config
│       ├── healthHandler.go  # Health check endpoint
│       ├── homeHandler.go    # Landing page
│       └── chatRoomPageHandler.go
├── pkg/
│   ├── models/
│   │   ├── message.go        # Message type + validation
│   │   ├── response.go       # Server response wrapper
│   │   ├── metric.go         # CSV metric record
│   │   └── roomStats.go      # Per-room latency/throughput stats
│   ├── generate/
│   │   └── generate.go       # Test data factories
│   ├── env/
│   │   └── environment.go    # .env loader
│   └── utils/
│       └── csvWriter.go      # Thread-safe CSV writer
└── results/
    ├── metrics.csv           # Raw per-message metrics
    └── metrics.ipynb         # Analysis notebook
```
