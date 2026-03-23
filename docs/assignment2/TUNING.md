# Part 4 — System Tuning Log

Target metrics:
- Queue depth < 1,000 messages
- Consumer lag < 100ms
- No message loss (0 failed messages)
- Failed connections: 0

Config that does NOT change across runs:
- `UserCount=50`, `MessageCount=500`, `RoomCount=20`, `PoolSize=1000`
- `MessageBuffer=60000`, `tempBufferSize=2048`
- `InitialCredits=1000` (per consumer)
- 1 server (direct EC2, no ALB) unless noted
- Runs 1–5: Chat server instance not noted; RabbitMQ t3.micro → t3.small (Run 4)
- Runs 6+: Chat server upgraded to **t3.small**; RabbitMQ on **t3.micro**

---

## Run 1 — Baseline (pre-tuning)

| Parameter | Value |
|-----------|-------|
| `PUBLISH_WORKERS` | 30 |
| `publishChanSize` | 4000 |

| Metric | Value |
|--------|-------|
| Runtime | 205.8s |
| Successful messages | 475,866 |
| Failed messages | 0 |
| Failed connections | 325 |
| Throughput | 2,312 msg/s |
| Mean latency | 47,734ms |
| Median latency | 15,478ms |
| 95th pct latency | 142,424ms |
| 99th pct latency | 150,687ms |
| Peak queue depth | ~75,000 (far above target) |

Notes: Queue depth spiked to 75K, server stalled during run. Circuit breaker opened, buffer filled, messages dropped. Server became unresponsive and required reboot.

---

## Run 2 — Reduce workers + channel size

| Parameter | Value |
|-----------|-------|
| `PUBLISH_WORKERS` | 10 |
| `publishChanSize` | 500 |

| Metric | Value |
|--------|-------|
| Peak queue depth | ~2,700 (better, still above target) |

Notes: Significant improvement over Run 1 but still above 1,000 target. Server stalled again mid-run.

---

## Run 3 — Reduced workers + smaller channel

| Parameter | Value |
|-----------|-------|
| `PUBLISH_WORKERS` | 10 |
| `publishChanSize` | 500 |

| Metric | Value |
|--------|-------|
| Runtime | 724.6s |
| Successful messages | 463,427 |
| Failed messages | 0 |
| Failed connections | 386 |
| Throughput | 639.5 msg/s |
| Mean latency | 101,314ms |
| Median latency | 13,824ms |
| 95th pct latency | 511,045ms |
| 99th pct latency | 554,539ms |

Notes: RabbitMQ was t3.micro — TCP write timeouts to broker caused circuit breaker to trip. fd limit was fine (65535). Root cause: RabbitMQ instance too small for burst load.

---

## Run 4 — Upgraded RabbitMQ to t3.small

| Parameter | Value |
|-----------|-------|
| `PUBLISH_WORKERS` | 10 |
| `publishChanSize` | 500 |
| RabbitMQ instance | t3.small (upgraded from t3.micro) |

| Metric | Value |
|--------|-------|
| Runtime | 245.4s |
| Successful messages | 502,000 |
| Failed messages | 0 |
| Failed connections | 0 |
| Throughput | 2,045 msg/s |
| Mean latency | 115,918ms |
| Median latency | 120,169ms |
| 95th pct latency | 125,469ms |
| 99th pct latency | 128,831ms |
| Peak queue depth | ~13 (well under 1,000 target) |

Notes: All targets met. 502K/502K messages delivered, 0 failures, queue depth never exceeded ~13. Upgrading RabbitMQ from t3.micro to t3.small resolved the TCP write timeout issue. This is the tuned baseline config.

---

## Run 5 — Increase workers for flatter queue graph

| Parameter | Value |
|-----------|-------|
| `PUBLISH_WORKERS` | 20 |
| `publishChanSize` | 500 |
| RabbitMQ instance | t3.small |

| Metric | Value |
|--------|-------|
| Runtime | 241.6s |
| Successful messages | 502,000 |
| Failed messages | 0 |
| Failed connections | 0 |
| Throughput | 2,077 msg/s |
| Mean latency | 116,142ms |
| Median latency | 118,162ms |
| 95th pct latency | 129,499ms |
| 99th pct latency | 133,478ms |
| Peak queue depth | ~22 (well under 1,000 target) |

Notes: Marginally better throughput than Run 4 (2,077 vs 2,045 msg/s). Queue depth slightly higher (~22 vs ~13) but still well under target. Latency very consistent across all rooms (110–122s range). Sawtooth pattern persists — inherent to bursty WebSocket traffic. **This is the final tuned config.**

---

## Run 6 — EC2 Direct | Pool=1000 | Workers=5 | 500K msgs | Sync Accept | InitialCredits=50

| Parameter | Value |
|-----------|-------|
| `PUBLISH_WORKERS` | 5 |
| `publishChanSize` | 500 |
| `InitialCredits` | 50 |
| Accept mode | Sync |
| `CONSUMER_RATE` | not set |
| Chat server | t3.small |
| RabbitMQ instance | t3.micro |

| Metric | Value |
|--------|-------|
| Runtime | 313.9s |
| Successful messages | 502,000 |
| Failed messages | 0 |
| Failed connections | 0 |
| Throughput | 1,599 msg/s |
| Mean latency | 151,874ms |
| Median latency | 156,351ms |
| 95th pct latency | 159,206ms |
| 99th pct latency | 159,585ms |
| Median room throughput | 82.68 msg/s |

Notes: Sync Accept with 5 publish workers throttled throughput to 1,599 msg/s. Latency very consistent across all rooms (122–158s range) — sign of steady draining. Queue depth graph TBD.

---

## Run 7 — EC2 Direct | Pool=1000 | Workers=20 | 500K msgs | Sync Accept | InitialCredits=1000 | ConsumerRate=80

| Parameter | Value |
|-----------|-------|
| `PUBLISH_WORKERS` | 20 |
| `publishChanSize` | 500 |
| `InitialCredits` | 1,000 |
| Accept mode | Sync |
| `CONSUMER_RATE` | 80 |
| Chat server | t3.small |
| RabbitMQ instance | t3.micro |

| Metric | Value |
|--------|-------|
| Runtime | 304.4s |
| Successful messages | 502,000 |
| Failed messages | 0 |
| Failed connections | 0 |
| Throughput | 1,649 msg/s |
| Mean latency | 73,518ms |
| Median latency | 74,227ms |
| 95th pct latency | 77,841ms |
| 99th pct latency | 79,690ms |
| Peak queue depth | ~27 |
| Median room throughput | 103.81 msg/s |

Notes: Sync Accept fixed the AMQP credit exhaustion deadlock (async Accept + rate limiter caused Receive() to block permanently after InitialCredits messages). Queue stays near 0 (sawtooth ~27 max) because ConsumerRate=80/room ≈ actual publish rate of ~82/room. Throughput lower than Run 5 (1,649 vs 2,077 msg/s) due to sync Accept overhead, but **mean latency improved significantly: 73s vs 116s**. Acks confirmed working (Unacked=15 in-flight, Ready=0 at end).

---

## Run 8 — EC2 Direct | Pool=1000 | Workers=20 | 500K msgs | Async Accept | ConsumerRate=0

| Parameter | Value |
|-----------|-------|
| `PUBLISH_WORKERS` | 20 |
| `publishChanSize` | 500 |
| `InitialCredits` | 1,000 |
| Accept mode | Async |
| `CONSUMER_RATE` | 0 (unlimited) |
| Chat server | t3.small |
| RabbitMQ instance | t3.micro |

| Metric | Value |
|--------|-------|
| Runtime | 240.0s |
| Successful messages | 502,000 |
| Failed messages | 0 |
| Failed connections | 0 |
| Throughput | 2,091 msg/s |
| Mean latency | 114,804ms |
| Median latency | 117,544ms |
| 95th pct latency | 128,975ms |
| 99th pct latency | 133,589ms |
| Peak queue depth | ~20 |
| Median room throughput | 107.67 msg/s |

Notes: Best throughput result. Async Accept + unlimited consumer rate. Queue sawtooth pattern, max ~20, drains to 0 after test. 0 failures, 0 reconnections. **This is the final tuned config.**

---

## Run 9 — EC2 Direct | Pool=1000 | Workers=40 | 500K msgs | Async Accept | ConsumerRate=0

| Parameter | Value |
|-----------|-------|
| `PUBLISH_WORKERS` | 40 |
| `publishChanSize` | 500 |
| `InitialCredits` | 1,000 |
| Accept mode | Async |
| `CONSUMER_RATE` | 0 (unlimited) |
| Chat server | t3.small |
| RabbitMQ instance | t3.micro |

| Metric | Value |
|--------|-------|
| Runtime | 241.5s |
| Successful messages | 502,000 |
| Failed messages | 0 |
| Failed connections | 0 |
| Throughput | 2,079 msg/s |
| Mean latency | 115,779ms |
| Median latency | 118,203ms |
| 95th pct latency | 129,411ms |
| 99th pct latency | 133,395ms |
| Peak queue depth | ~35–40 |
| Median room throughput | 106.96 msg/s |

Notes: Doubling workers from 20→40 yields no throughput gain (2,079 vs 2,091 msg/s) and slightly higher queue depth (~35 vs ~20). Worker count is not the bottleneck. **Run 8 (Workers=20) remains the optimal config.**

---

## Run 10 — EC2 Direct | Pool=1000 | Workers=80 | 500K msgs | Async Accept | ConsumerRate=0

| Parameter | Value |
|-----------|-------|
| `PUBLISH_WORKERS` | 80 |
| `publishChanSize` | 500 |
| `InitialCredits` | 1,000 |
| Accept mode | Async |
| `CONSUMER_RATE` | 0 (unlimited) |
| Chat server | t3.small |
| RabbitMQ instance | t3.micro |

| Metric | Value |
|--------|-------|
| Runtime | 246.4s |
| Successful messages | 502,000 |
| Failed messages | 0 |
| Failed connections | 0 |
| Throughput | 2,037 msg/s |
| Mean latency | 117,041ms |
| Median latency | 118,937ms |
| 95th pct latency | 130,433ms |
| 99th pct latency | 134,653ms |
| Peak queue depth | ~10,000 (exceeds 1,000 target) |
| Median room throughput | 105.23 msg/s |

Notes: 80 workers overwhelm RabbitMQ — queue spiked to ~10K, violating the <1,000 target. Throughput also dropped (2,037 vs 2,091 msg/s). A second run triggered the async Accept credit exhaustion deadlock: Ready=0, Unacked=6,088 — publish burst outpaced Accept goroutine throughput, exhausting AMQP credits and freezing consumers. More workers = more concurrent AMQP publishes = burst pressure on broker. **Workers=20 (Run 8) is the optimal config.**

---

## Run 11 — EC2 Direct | Pool=1000 | Workers=15 | 500K msgs | Async Accept | ConsumerRate=0

| Parameter | Value |
|-----------|-------|
| `PUBLISH_WORKERS` | 15 |
| `publishChanSize` | 500 |
| `InitialCredits` | 1,000 |
| Accept mode | Async |
| Chat server | t3.small |
| RabbitMQ instance | t3.micro |

| Metric | Value |
|--------|-------|
| Runtime | 253.3s |
| Successful messages | 502,000 |
| Failed messages | 0 |
| Failed connections | 0 |
| Throughput | 1,982 msg/s |
| Mean latency | 114,636ms |
| Median latency | 117,763ms |
| 95th pct latency | 129,039ms |
| 99th pct latency | 134,534ms |
| Peak queue depth | ~20 |
| Median room throughput | 107.59 msg/s |

Notes: Fewer workers throttle publish throughput — 1,982 msg/s vs 2,091 with 20 workers, with no improvement in queue depth. 20 workers is the minimum needed to saturate the broker.

---

## Worker Sweep Summary (Runs 8–11)

| Workers | Throughput | Peak Queue | Meets Target |
| ------- | ---------- | ---------- | ------------ |
| 15 | 1,982 msg/s | ~20 | ✓ |
| 20 | 2,091 msg/s | ~20 | ✓ ← optimal |
| 40 | 2,079 msg/s | ~35–40 | ✓ |
| 80 | 2,037 msg/s | ~10,000 | ✗ |

---

## Run 12 — EC2 Direct | Pool=64 | Workers=20 | 500K msgs | Async Accept

| Parameter | Value |
|-----------|-------|
| `PoolSize` | 64 |
| `PUBLISH_WORKERS` | 20 |
| `publishChanSize` | 500 |
| `InitialCredits` | 1,000 |
| Accept mode | Async |
| Chat server | t3.small |
| RabbitMQ instance | t3.micro |

| Metric | Value |
| ------ | ----- |
| Runtime | 142.9s |
| Successful messages | 502,000 |
| Failed messages | 0 |
| Failed connections | 0 |
| Throughput | 3,512 msg/s |
| Mean latency | 4,372ms |
| Median latency | 4,130ms |
| 95th pct latency | 7,533ms |
| 99th pct latency | 9,132ms |
| Peak queue depth | ~45 |
| Median room throughput | 1,556.69 msg/s |

Notes: Dramatic improvement over Pool=1000. Throttling to 64 concurrent connections staggers send load — server and broker never get overwhelmed simultaneously. Throughput +68%, mean latency collapsed from 115s → 4.4s, runtime cut by 40%. Queue stays well under target despite lower PoolSize.

---

## Summary — Tuned Config

| Parameter | Value |
|-----------|-------|
| `PUBLISH_WORKERS` | 20 |
| `publishChanSize` | 500 |
| Chat server | t3.small |
| RabbitMQ instance | t3.micro |
| Accept mode | Async |
| `CONSUMER_RATE` | 0 (unlimited) |
| Peak queue depth | ~22 |
| Throughput | 2,077 msg/s |
| Failed messages | 0 |
| Failed connections | 0 |

---

## Run 13 — EC2 Direct | Pool=128 | Workers=20 | 500K msgs | Async Accept

| Parameter | Value |
|-----------|-------|
| `PoolSize` | 128 |
| `PUBLISH_WORKERS` | 20 |
| `publishChanSize` | 500 |
| `InitialCredits` | 1,000 |
| Accept mode | Async |
| Chat server | t3.small |
| RabbitMQ instance | t3.micro |

| Metric | Value |
| ------ | ----- |
| Runtime | 153.1s |
| Successful messages | 502,000 |
| Failed messages | 0 |
| Failed connections | 0 |
| Throughput | 3,278 msg/s |
| Mean latency | 9,543ms |
| Median latency | 10,491ms |
| 95th pct latency | 16,732ms |
| 99th pct latency | 18,655ms |
| Peak queue depth | ~35 |
| Median room throughput | 796.37 msg/s |

Notes: Worse than Pool=64 on throughput (3,278 vs 3,512 msg/s) and latency (9.5s vs 4.4s mean). More concurrent connections = more simultaneous burst = higher per-message queue pressure.

---

## Run 14 — EC2 Direct | Pool=256 | Workers=20 | 500K msgs | Async Accept

| Parameter | Value |
|-----------|-------|
| `PoolSize` | 256 |
| `PUBLISH_WORKERS` | 20 |
| `publishChanSize` | 500 |
| `InitialCredits` | 1,000 |
| Accept mode | Async |
| Chat server | t3.small |
| RabbitMQ instance | t3.micro |

| Metric | Value |
| ------ | ----- |
| Runtime | 173.8s |
| Successful messages | 502,000 |
| Failed messages | 0 |
| Failed connections | 0 |
| Throughput | 2,888 msg/s |
| Mean latency | 20,771ms |
| Median latency | 20,660ms |
| 95th pct latency | 30,432ms |
| 99th pct latency | 33,840ms |
| Peak queue depth | ~35 |
| Median room throughput | 406.51 msg/s |

Notes: Continues the downward trend — more concurrent connections = worse throughput and latency. Pool=64 remains best.

---

## Run 15 — EC2 Direct | Pool=512 | Workers=20 | 500K msgs | Async Accept

| Parameter | Value |
|-----------|-------|
| `PoolSize` | 512 |
| `PUBLISH_WORKERS` | 20 |
| `publishChanSize` | 500 |
| `InitialCredits` | 1,000 |
| Accept mode | Async |
| Chat server | t3.small |
| RabbitMQ instance | t3.micro |

| Metric | Value |
| ------ | ----- |
| Runtime | 207.3s |
| Successful messages | 502,000 |
| Failed messages | 0 |
| Failed connections | 0 |
| Throughput | 2,421 msg/s |
| Mean latency | 47,776ms |
| Median latency | 47,456ms |
| 95th pct latency | 65,639ms |
| 99th pct latency | 68,871ms |
| Peak queue depth | ~22 |
| Median room throughput | 213.30 msg/s |

Notes: Continues downward trend. Pool=64 remains optimal by a large margin.

---

## Run 16 — EC2 Direct | Pool=32 | Workers=20 | 500K msgs | Async Accept

| Parameter | Value |
|-----------|-------|
| `PoolSize` | 32 |
| `PUBLISH_WORKERS` | 20 |
| `publishChanSize` | 500 |
| `InitialCredits` | 1,000 |
| Accept mode | Async |
| Chat server | t3.small |
| RabbitMQ instance | t3.micro |

| Metric | Value |
| ------ | ----- |
| Runtime | 129.4s |
| Successful messages | 502,000 |
| Failed messages | 0 |
| Failed connections | 0 |
| Throughput | 3,881 msg/s |
| Mean latency | 2,076ms |
| Median latency | 1,943ms |
| 95th pct latency | 3,482ms |
| 99th pct latency | 4,213ms |
| Peak queue depth | ~25 |
| Median room throughput | 2,891.11 msg/s |

Notes: New best — outperforms Pool=64 on all metrics. Smaller semaphore = more serialized connection ramp-up = lower burst pressure on server and RabbitMQ simultaneously.

---

## Pool Sweep Summary (Runs 12–16)

## Run 17 — EC2 Direct | Pool=16 | Workers=20 | 500K msgs | Async Accept

| Parameter | Value |
|-----------|-------|
| `PoolSize` | 16 |
| `PUBLISH_WORKERS` | 20 |
| `publishChanSize` | 500 |
| `InitialCredits` | 1,000 |
| Accept mode | Async |
| Chat server | t3.small |
| RabbitMQ instance | t3.micro |

| Metric | Value |
| ------ | ----- |
| Runtime | 119.1s |
| Successful messages | 502,000 |
| Failed messages | 0 |
| Failed connections | 0 |
| Throughput | 4,216 msg/s |
| Mean latency | 917ms |
| Median latency | 856ms |
| 95th pct latency | 1,460ms |
| 99th pct latency | 2,073ms |
| Peak queue depth | ~30 |
| Median room throughput | 4,147.92 msg/s |

Notes: Sub-second mean latency. New best on all metrics. Sawtooth more pronounced but queue peaks (~30) well under target.

---

## Pool Sweep Summary (Runs 12–17)

| PoolSize | Throughput | Mean Latency | Peak Queue | Meets Target |
| -------- | ---------- | ------------ | ---------- | ------------ |
| 16 | 4,216 msg/s | 917ms | ~30 | ✓ ← optimal so far |
| 32 | 3,881 msg/s | 2,076ms | ~25 | ✓ |
| 64 | 3,512 msg/s | 4,372ms | ~45 | ✓ |
| 128 | 3,278 msg/s | 9,543ms | ~35 | ✓ |
| 256 | 2,888 msg/s | 20,771ms | ~35 | ✓ |
| 512 | 2,421 msg/s | 47,776ms | ~22 | ✓ |
| 1,000 | 2,091 msg/s | 114,804ms | ~20 | ✓ |

---

## Run 18 — EC2 Direct | Pool=1024 | Workers=20 | 500K msgs | Async Accept (anomalous)

| Metric | Value |
| ------ | ----- |
| Runtime | 865.0s |
| Successful messages | 502,000 |
| Failed messages | 0 |
| Failed connections | 0 |
| Throughput | 580 msg/s |
| Mean latency | 127,135ms |

Notes: Anomalous result — dramatically worse than Run 8 (Pool=1000: 240s, 2,091 msg/s). Root cause: accumulated stale RabbitMQ queues from many prior test runs causing broker backpressure. Not representative of Pool=1024 performance. Discarded from sweep summary.

---

## Run 19 — 2 Servers (ALB) | Pool=64 | Workers=20 | 500K msgs | Async Accept

| Parameter | Value |
|-----------|-------|
| `PoolSize` | 64 |
| `PUBLISH_WORKERS` | 20 |
| Servers | 2 × t3.micro behind ALB |
| RabbitMQ | t3.micro |
| Accept mode | Async |

| Metric | Value |
| ------ | ----- |
| Runtime | 135.4s |
| Successful messages | 502,000 |
| Failed messages | 0 |
| Failed connections | 0 |
| Throughput | 3,708 msg/s |
| Mean latency | 4,182ms |
| Median latency | 4,078ms |
| 95th pct latency | 7,251ms |
| 99th pct latency | 10,381ms |
| Peak queue depth | ~130 |
| Median room throughput | 1,647.40 msg/s |

Notes: Only +5.6% throughput gain over single server (3,708 vs 3,512 msg/s). With Pool=64 the client is the bottleneck — connections ramp up serially so adding a 2nd server doesn't help much. Queue doubles (~130 vs ~45) since both servers publish to the same queues simultaneously. For Pool=64, single server is essentially equivalent.

---

## Run 20 — 2 Servers (ALB) | Pool=64 | Workers=10 | 500K msgs | Async Accept

| Parameter | Value |
|-----------|-------|
| `PoolSize` | 64 |
| `PUBLISH_WORKERS` | 10 |
| Servers | 2 × t3.micro behind ALB |
| RabbitMQ | t3.micro |
| Accept mode | Async |

| Metric | Value |
| ------ | ----- |
| Runtime | 175.3s |
| Successful messages | 502,000 |
| Failed messages | 0 |
| Failed connections | 0 |
| Throughput | 2,864 msg/s |
| Mean latency | 5,050ms |
| Median latency | 4,935ms |
| 95th pct latency | 8,339ms |
| 99th pct latency | 9,729ms |
| Peak queue depth | ~40–50 |
| Median room throughput | 1,476.79 msg/s |

Notes: Halving workers (10 per server = 20 total) reduced queue from ~130 to ~40–50 but degraded throughput 23% (2,864 vs 3,708 msg/s) and increased runtime 29%. Fewer publishers create more AMQP back-pressure in the server's publishChan, slowing the entire pipeline. **Workers=20 per server (Run 19) remains the best 2-server config.**

---

## 2-Server Scaling Summary (Runs 19–20)

| Workers/server | Total publishers | Throughput | Peak Queue | Notes |
| -------------- | ---------------- | ---------- | ---------- | ----- |
| 20 | 40 | 3,708 msg/s | ~130 | best throughput |
| 10 | 20 | 2,864 msg/s | ~40–50 | lower queue but 23% slower |

---

## Run 21 — 2 Servers (ALB) | Pool=256 | Workers=20 | 500K msgs | Async Accept

| Parameter | Value |
|-----------|-------|
| `PoolSize` | 256 |
| `PUBLISH_WORKERS` | 20 |
| Servers | 2 × t3.micro behind ALB |
| RabbitMQ | t3.micro |
| Accept mode | Async |

| Metric | Value |
| ------ | ----- |
| Runtime | 172.9s |
| Successful messages | 502,000 |
| Failed messages | 0 |
| Failed connections | 0 |
| Throughput | 2,904 msg/s |
| Mean latency | 21,335ms |
| Median latency | 25,463ms |
| 95th pct latency | 32,601ms |
| 99th pct latency | 35,217ms |
| Peak queue depth | ~100 |
| Median room throughput | 413.87 msg/s |

Notes: Essentially identical to single-server Pool=256 (2,888 msg/s, 173.8s). Adding a second server yields only +0.6% throughput gain. Queue doubled (~35→~100) vs single server because both servers publish simultaneously. With Pool=256 the client sends enough burst that both servers stay at similar utilization to a single server — adding capacity doesn't help when messages arrive faster than the pipeline drains. **Run 19 (Pool=64, Workers=20) remains the best 2-server config.**

---

## Run 22 — 2 Servers t3.small (ALB) | Pool=1000 | Workers=20 | 500K msgs | Async Accept

| Parameter | Value |
|-----------|-------|
| `PoolSize` | 1,000 |
| `PUBLISH_WORKERS` | 20 |
| Servers | 2 × t3.small behind ALB |
| RabbitMQ | t3.micro |
| Accept mode | Async |

| Metric | Value |
| ------ | ----- |
| Runtime | 244.4s |
| Successful messages | 502,000 |
| Failed messages | 0 |
| Failed connections | 0 |
| Throughput | 2,054 msg/s |
| Mean latency | 114,410ms |
| Median latency | 117,385ms |
| 95th pct latency | 128,744ms |
| 99th pct latency | 133,892ms |
| Peak queue depth | ~300 |
| Median room throughput | 107.24 msg/s |

Notes: Effectively identical to single-server Pool=1000 (2,091 msg/s, 240s, Run 8). Adding a second server yields -1.8% throughput (slightly worse due to doubled publisher contention). Queue spiked to ~300 (vs ~20 single server) from 40 total publishers hitting RabbitMQ. t3.micro crashed with this load; t3.small (2GB RAM) handles 500 connections each without OOM. **Confirms: with Pool=1000, the bottleneck is RabbitMQ fan-out, not the chat server — adding servers provides zero scaling benefit.**

---

## Final Summary — 2-Server Scaling (All Runs)

| PoolSize | Servers | Instance | Throughput | Mean Latency | Peak Queue |
| -------- | ------- | -------- | ---------- | ------------ | ---------- |
| 1,000 | 1 | t3.small | 2,091 msg/s | 114,804ms | ~20 |
| 1,000 | 2 | t3.small | 2,054 msg/s | 114,410ms | ~300 |
| 256 | 1 | t3.small | 2,888 msg/s | 20,771ms | ~35 |
| 256 | 2 | t3.micro | 2,904 msg/s | 21,335ms | ~100 |
| 64 | 1 | t3.small | 3,512 msg/s | 4,372ms | ~45 |
| 64 | 2 | t3.micro | 3,708 msg/s | 4,182ms | ~130 |

**Key insight**: Horizontal scaling only helps when the bottleneck is per-server compute. With RabbitMQ fan-out (each message broadcast to all 50 room members), doubling servers doubles publisher load on the broker — queue pressure negates any per-server gain. Smaller PoolSize (64) constrains burst and shows modest scaling; larger PoolSize (1000) saturates RabbitMQ equally regardless of server count.

---

## Scaling Comparison (Fixed Config: Pool=1000, Workers=5/server)

Runs 23–25 use a fixed config across 1/2/4 servers to measure horizontal scaling under identical per-server settings. Workers=5/server keeps total publishers proportional (5/10/20) and within the safe queue zone.

---

## Run 23 — 1 Server (Direct) | Pool=1000 | Workers=5 | 500K msgs | t3.small

| Parameter | Value |
| --------- | ----- |
| `PoolSize` | 1,000 |
| `PUBLISH_WORKERS` | 5 |
| Servers | 1 × t3.small (direct, no ALB) |
| RabbitMQ | t3.medium |
| Accept mode | Async |

| Metric | Value |
| ------ | ----- |
| Runtime | 317.2s |
| Successful messages | 502,000 |
| Failed messages | 0 |
| Failed connections | 0 |
| Throughput | 1,582.7 msg/s |
| Mean latency | 156,188ms |
| Median latency | 160,226ms |
| 95th pct latency | 163,206ms |
| 99th pct latency | 163,707ms |
| Min latency | 63,301ms |
| Max latency | 163,898ms |
| Peak queue depth | ~10 |
| Median room throughput | 81.55 msg/s |

Notes: 1-server baseline for the fixed-config scaling comparison. Workers=5 throttles publish throughput (1,583 vs 2,091 msg/s with Workers=20) but queue stays very flat (~10 peak). 0 failures, 0 reconnections.

---

## Run 24 — 2 Servers (ALB) | Pool=1000 | Workers=5/server | 500K msgs | t3.small

| Parameter | Value |
| --------- | ----- |
| `PoolSize` | 1,000 |
| `PUBLISH_WORKERS` | 5 |
| Servers | 2 × t3.small behind ALB |
| RabbitMQ | t3.medium |
| Accept mode | Async |

| Metric | Value |
| ------ | ----- |
| Runtime | 328.3s |
| Successful messages | 502,000 |
| Failed messages | 0 |
| Failed connections | 0 |
| Throughput | 1,529.0 msg/s |
| Mean latency | 152,741ms |
| Median latency | 156,417ms |
| 95th pct latency | 163,297ms |
| 99th pct latency | 163,836ms |
| Min latency | 49,769ms |
| Max latency | 164,465ms |
| Peak queue depth | ~80–100 |
| Median room throughput | 79.22 msg/s |

Notes: 2-server result is -3.4% throughput vs 1 server (1,529 vs 1,583 msg/s). Queue spikes 8–10× higher (~80–100 vs ~10) from 10 total publishers hitting the same exchanges. Runtime also slightly longer (328s vs 317s). Confirms the fixed-config pattern: doubling servers doubles RabbitMQ fan-out pressure, negating any per-server compute gain.

---

## Run 25 — 4 Servers (ALB) | Pool=1000 | Workers=5/server | 500K msgs | t3.small

| Parameter | Value |
| --------- | ----- |
| `PoolSize` | 1,000 |
| `PUBLISH_WORKERS` | 5 |
| Servers | 4 × t3.small behind ALB |
| RabbitMQ | t3.medium |
| Accept mode | Async |

| Metric | Value |
| ------ | ----- |
| Runtime | 538.1s |
| Successful messages | 502,000 |
| Failed messages | 0 |
| Failed connections | 0 |
| Throughput | 933.0 msg/s |
| Mean latency | 254,301ms |
| Median latency | 258,805ms |
| 95th pct latency | 267,535ms |
| 99th pct latency | 268,033ms |
| Min latency | 78,839ms |
| Max latency | 268,416ms |
| Peak queue depth | ~130 |
| Median room throughput | 47.54 msg/s |

Notes: Dramatic degradation vs 2 servers (-39% throughput, +69% latency). 4 servers × 5 workers = 20 total AMQP publishers all competing on the same exchanges simultaneously. Each published message is fanned out to 4 queues (one per server per room) — doubling from 2 to 4 servers doubles the fan-out work. RabbitMQ cannot drain fast enough, creating a systemic pipeline slowdown that worsens with each server added. Queue depth (~130) is modest but publish latency per message grows linearly with fan-out width.

---

## Fixed-Config Scaling Summary (Runs 23–25)

| Servers | Total publishers | Runtime | Throughput | vs 1S | Mean latency | Peak queue |
| ------- | ---------------- | ------- | ---------- | ----- | ------------ | ---------- |
| 1 | 5 | 317.2s | 1,582.7 msg/s | — | 156,188ms | ~10 |
| 2 | 10 | 328.3s | 1,529.0 msg/s | -3.4% | 152,741ms | ~80–100 |
| 4 | 20 | 538.1s | 933.0 msg/s | -41.0% | 254,301ms | ~130 |

**Key finding**: With a RabbitMQ fan-out architecture and Pool=1000 (high concurrent connections), adding servers monotonically degrades performance. Each server adds 5 more concurrent AMQP publishers. With 4 servers, each message must be published to 4 queues simultaneously — RabbitMQ fan-out overhead scales as O(servers²) relative to single-server baseline. The single server is the optimal configuration for this workload.

---

## Next Steps

- [x] Worker sweep (Workers=15/20/40/80) on single server — optimal: 20
- [x] Pool sweep (Pool=16/32/64/128/256/512) on single server — optimal: 16 (throughput) or 512+ (flat queue graph)
- [x] 2-server ALB setup with Pool=64, Workers=20 — +5.6% vs single server
- [x] 2-server ALB setup with Pool=64, Workers=10 — worse throughput (23% drop)
- [x] 2-server ALB setup with Pool=256, Workers=20 — no scaling gain (+0.6%)
- [x] 2-server ALB setup with Pool=1000, t3.small — no scaling gain (-1.8%)
- [x] Fixed-config 1-server baseline: Pool=1000, Workers=5 (Run 23)
- [x] Fixed-config 2-server: Pool=1000, Workers=5/server, ALB (Run 24) — -3.4% vs 1 server
- [x] Fixed-config 4-server: Pool=1000, Workers=5/server, ALB (Run 25) — -41% vs 1 server
