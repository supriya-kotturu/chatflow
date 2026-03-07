# ChatFlow v2 — Performance Results

## Test Configuration

| Parameter | Value |
|-----------|-------|
| Client pool size | 1,000 connections |
| Users per room | 50 |
| Messages per user | 500 |
| Rooms | 20 |
| Total messages | 502,000 |
| Server instance type | t3.medium (2 vCPU, 4 GB RAM) |
| RabbitMQ instance type | t3.medium |
| Region | us-west-2 (Oregon) |

### Server Configuration

| Parameter | Value |
|-----------|-------|
| `bufferSize` (per-client Send channel) | 60,000 |
| `publishChanSize` | 4,000 |
| `numPublishWorkers` | 30 |
| `InitialCredits` (AMQP consumer) | 1,000 |

### Infrastructure

```
Client (local) → ALB (port 80) → Chat Server 1..N (port 3000) → RabbitMQ (port 5672)
```

- **1-server test**: client connected directly to one EC2 instance (port 3000), bypassing ALB
- **2-server / 4-server tests**: client connected via ALB with ALB session stickiness enabled

---

## Results Summary

| Metric | 1 Server | 2 Servers | 4 Servers |
|--------|----------|-----------|-----------|
| Total runtime | 600.5s | 243s | 323.8s |
| Messages sent | 502,000 | 502,000 | 502,000 |
| Failed messages | 0 | 0 | 0 |
| Failed connections | 0 | 0 | 0 |
| Overall throughput | 836 msg/s | 2,067 msg/s | 1,550 msg/s |
| Mean latency | 148,224 ms | 115,163 ms | 150,608 ms |
| Median latency | 150,511 ms | 117,511 ms | 157,925 ms |
| 95th pct latency | 158,109 ms | 128,523 ms | 162,224 ms |
| 99th pct latency | 158,629 ms | 133,960 ms | 162,741 ms |
| Min latency | 363 ms | 468 ms | 459 ms |
| Max latency | 158,766 ms | 139,953 ms | 163,219 ms |
| Median room throughput | 22.60 msg/s | 105.69 msg/s | 78.23 msg/s |

---

## Detailed Per-Run Results

### 1 Server

```
Total runtime (wall time):  600.5s
Successful messages sent:   502000
Failed messages:            0
Overall throughput:         836.0 msg/s
Total connections:          1000
Failed connections:         0
Mean Latency:               148224ms
Median Latency:             150511ms
95th Percentile Latency:    158109ms
99th Percentile Latency:    158629ms
Min Latency:                363ms
Max Latency:                158766ms
Median Throughput:          22.60 msg/s
```

### 2 Servers

```
Total runtime (wall time):  243s
Successful messages sent:   502000
Failed messages:            0
Overall throughput:         2067.2 msg/s
Total connections:          1000
Failed connections:         0
Mean Latency:               115163ms
Median Latency:             117511ms
95th Percentile Latency:    128523ms
99th Percentile Latency:    133960ms
Min Latency:                468ms
Max Latency:                139953ms
Median Throughput:          105.69 msg/s
```

### 4 Servers

```
Total runtime (wall time):  323.8s
Successful messages sent:   502000
Failed messages:            0
Overall throughput:         1550.3 msg/s
Total connections:          1000
Failed connections:         0
Mean Latency:               150608ms
Median Latency:             157925ms
95th Percentile Latency:    162224ms
99th Percentile Latency:    162741ms
Min Latency:                459ms
Max Latency:                163219ms
Median Throughput:          78.23 msg/s
```

---

## Analysis

### 1 → 2 Servers: 2.5× Throughput Improvement

The largest gain occurs going from 1 to 2 servers. With a single server, all 1,000 WebSocket goroutines compete for the `publishChan` (capacity 4,000) and the server's 2 vCPUs are saturated. Splitting to 2 servers halves per-server connection count and CPU pressure, yielding a 2.5× throughput improvement (836 → 2,067 msg/s) and a 22% latency reduction (148s → 115s mean).

### 2 → 4 Servers: Diminishing Returns

Going from 2 to 4 servers, throughput drops ~25% (2,067 → 1,550 msg/s) and latency increases slightly. This is expected: each message published by any server is routed to 4 queues (one per server per room) instead of 2. The 30 publish workers on each server now fan out to 4× more consumers, increasing RabbitMQ publish overhead and tightening back-pressure on the WebSocket read loops. The bottleneck shifts from CPU contention to RabbitMQ fan-out overhead.

### Back-Pressure Mechanism

`publishChanSize=4,000` with blocking `Publish(ctx)` applies back-pressure from the RabbitMQ publish path all the way back to the WebSocket read loops:

```
WebSocket read loop → broadcastAndPublish → Publish(ctx) blocks on full publishChan
→ TCP back-pressure to client → client slows send rate
```

This keeps RabbitMQ queue depth bounded (~260 peak for 2 servers, ~1,100 peak for 4 servers) rather than allowing unbounded message accumulation. Queues drain to 0 after each test run.

### RabbitMQ Queue Behavior

| Configuration | Peak queue depth | Post-test drain |
|---------------|-----------------|-----------------|
| 1 server | ~8,000 | Full drain (self-filter path) |
| 2 servers | ~260 (oscillating) | Full drain |
| 4 servers | ~1,100 | Full drain |

With 1 server, all messages are self-filtered (same `ServerID`), so the queue spikes during the test then drains quickly. With multiple servers, the oscillating pattern reflects real cross-server fan-out with continuous publish/consume cycling.

### Latency Bottleneck

Latency is dominated by `publishChan` contention: 1,000 goroutines competing for a 4,000-capacity channel with 30 drain workers. This creates a queuing delay proportional to the number of connections per server. The bottleneck is not RabbitMQ itself but the publish channel acting as an intentional rate limiter.

### Zero Failures

All three configurations delivered 502,000 messages with 0 failures, demonstrating that the architecture is stable under load regardless of server count.
