# ChatFlow

A real-time chat application built with Go and WebSockets. Includes load-testing clients that measure latency, throughput, and message distribution across concurrent users and rooms.

## Architecture

```
client/          Load-testing client library (connects N users to M rooms)
  ws/            WebSocket client, connection pool, stats collection
client-part1/    Fan-out load test entry point
client-part2/    Pipeline load test entry point (500K → 2.5M messages)
server/          WebSocket server
  internal/      Room management, handlers, upgrader
  html/          Static frontend (HTML/CSS/JS)
pkg/
  models/        Shared types (Message, Response, Metric, RoomStats)
  generate/      Test data factories (users, rooms, messages)
  env/           .env loader
  utils/         CSV writer
results/         Output metrics CSV per config and Jupyter notebook for analysis
```

Each user gets one WebSocket connection per room. The server echoes messages back with a server timestamp, and the client measures round-trip latency.

## Threading Model

See [THREADING.md](THREADING.md) for the full goroutine model, diagrams, and synchronization primitives for both server and client.

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
SERVER_HOST=localhost
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

### Run the fan-out client (Part 1)

```bash
make run-client1
```

Default config (in `client-part1/main.go`):

| Parameter      | Value | Description                          |
|----------------|-------|--------------------------------------|
| PoolSize       | 320   | Max concurrent WebSocket connections |
| UserCount      | 32    | Number of simulated users            |
| MessageCount   | 1000  | Messages per user per room           |
| RoomCount      | 10    | Rooms each user joins                |
| MessageBuffer  | 1250  | Channel buffer size                  |

### Run the pipeline client (Part 2)

```bash
make run-client2
```

Runs all 5 load test configurations sequentially (defined in `client-part2/main.go`):

| Config | Pool | Users | Messages | Rooms | Total Messages |
|--------|------|-------|----------|-------|----------------|
| 500K   | 1000 | 50    | 500      | 20    | 502,000        |
| 1M     | 1000 | 100   | 1000     | 10    | 1,002,000      |
| 1.5M   | 1000 | 100   | 750      | 20    | 1,504,000      |
| 2M     | 2000 | 100   | 1000     | 20    | 2,004,000      |
| 2.5M   | 2500 | 125   | 1000     | 20    | 2,505,000      |

The client prints per-room and aggregate stats on completion:

```
Mean Latency across all rooms: 609ms
Median Latency across all rooms: 614ms
95th Percentile Latency across all rooms: 870ms
...
Room 5 | users: 32 | throughput: 4821.3 msg/s | mean latency: 602ms | median latency: 611ms
```

### Analyze results

Metrics are written to `results/<config>/metrics.csv`. Open the Jupyter notebook:

```bash
cd results
jupyter notebook metrics.ipynb
```

## Deployment

See [DEPLOYMENT.md](DEPLOYMENT.md) for instructions to build and deploy the server on AWS EC2.

## Load Test Results

See [RESULTS.md](RESULTS.md) for detailed load test results, insights, and recommendations.

## Makefile targets

| Target             | Description                          |
|--------------------|--------------------------------------|
| `make build`       | Build server and both client binaries |
| `make build-server`| Build the server binary              |
| `make build-client1`| Build the fan-out client binary     |
| `make build-client2`| Build the pipeline client binary    |
| `make run-server`  | Build and start the server           |
| `make run-client1` | Build and run the fan-out client     |
| `make run-client2` | Build and run the pipeline client    |
| `make test`        | Run all tests                        |
| `make fmt`         | Format all Go files                  |
| `make vet`         | Vet all Go files                     |
| `make clean`       | Remove built binaries                |

## Project structure

```
.
├── Makefile
├── .env
├── go.mod
├── DEPLOYMENT.md          # EC2 deployment guide
├── RESULTS.md             # Load test results and insights
├── THREADING.md           # Goroutine model and sync primitives
├── client/
│   ├── loadtest.go         # RunFanOutLoadTest / RunPipelineLoadTest
│   └── ws/
│       ├── client.go       # Load test orchestration and stats
│       ├── wsClient.go     # Single WebSocket connection wrapper
│       └── pool.go         # Connection pool with semaphore
├── client-part1/
│   └── main.go             # Fan-out load test entry point
├── client-part2/
│   └── main.go             # Pipeline load test entry point (5 configs)
├── server/
│   ├── main.go             # Entry point
│   ├── html/               # Static frontend
│   └── internal/server/
│       ├── server.go        # Room/client management
│       ├── chatRoomHandler.go # WebSocket lifecycle handler
│       ├── upgrader.go      # WebSocket upgrader config
│       ├── healthHandler.go # Health check endpoint
│       ├── homeHandler.go   # Landing page
│       └── chatRoomPageHandler.go
├── pkg/
│   ├── models/
│   │   ├── message.go       # Message type + validation
│   │   ├── response.go      # Server response wrapper
│   │   ├── metric.go        # CSV metric record
│   │   └── roomStats.go     # Per-room latency/throughput stats
│   ├── generate/
│   │   └── generate.go      # Test data factories
│   ├── env/
│   │   └── environment.go   # .env loader
│   └── utils/
│       └── csvWriter.go     # Thread-safe CSV writer
└── results/
    ├── 500K/                # 500K config metrics
    ├── 1M/                  # 1M config metrics
    ├── 1_5M/                # 1.5M config metrics
    ├── 2M/                  # 2M config metrics
    ├── 2_5M/                # 2.5M config metrics
    └── metrics.ipynb        # Analysis notebook (throughput + latency graphs)
```
