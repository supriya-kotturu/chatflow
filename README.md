# ChatFlow

Real-time WebSocket chat server with RabbitMQ fan-out for cross-server message delivery. Built for CS6650 Assignment 2.

## How it works

Clients connect via WebSocket. Each message is fanned out locally to all room members on the same server, and published to a RabbitMQ topic exchange so other servers in the cluster deliver it to their local clients.

```
Client → Server N
           ├── local fan-out → all clients on Server N in that room
           └── Publish → RabbitMQ (chat.exchange, routing key: room.<id>)
                              ↓ routed to all server queues bound to room.<id>
                         Server 1..N (self-filtered by ServerID)
                              └── consume → broadcast → local clients
```

See [docs/assignment2/ARCHITECTURE.md](docs/assignment2/ARCHITECTURE.md) for the full design.

## Prerequisites

- Go 1.23+
- RabbitMQ 4.0+ (AMQP 1.0)

## Setup

```bash
git clone <repo-url>
cd chat-flow
```

Create `.env` in the project root:

```
NAME=DEVELOPMENT
PORT=3000
SERVER_HOST=localhost
RABBIT_HOST=localhost
RABBIT_USER=guest
RABBIT_PASSWORD=guest
PUBLISH_WORKERS=20
ROOM_COUNT=20
```

## Running

```bash
# Build server
make build-server-v2

# Start server
make run-server-v2

# Run load test (502K messages, 50 users/room, 500 msgs/user, 20 rooms)
make run-client2
```

Output: per-room latency/throughput stats + aggregate percentiles printed on completion. Metrics written to `results-a2/metrics/<config>.csv`.

## Key Makefile targets

| Target | Description |
|--------|-------------|
| `make build-server-v2` | Build the v2 server binary (with RabbitMQ) |
| `make run-server-v2` | Build and start the v2 server |
| `make run-client2` | Run the pipeline load test |
| `make fmt` | Format all Go files |
| `make vet` | Vet all Go files |

## Docs

| File | Contents |
|------|----------|
| [docs/assignment2/ARCHITECTURE.md](docs/assignment2/ARCHITECTURE.md) | System design, message flow, failure handling |
| [docs/assignment2/REPORT.md](docs/assignment2/REPORT.md) | Full assignment report with tuning results |
| [docs/assignment2/REPORT_SUMMARY.md](docs/assignment2/REPORT_SUMMARY.md) | Summary tables per tuning dimension |
| [docs/assignment2/DESIGN_DECISIONS.md](docs/assignment2/DESIGN_DECISIONS.md) | Key design decisions and tradeoffs |
| [docs/assignment2/TUNING.md](docs/assignment2/TUNING.md) | Run-by-run tuning log (Runs 1–25) |
| [docs/assignment2/DEPLOYMENT.md](docs/assignment2/DEPLOYMENT.md) | AWS EC2 deployment guide |
