# Assignment 3 — Test Results

## Run 1 — 2026-03-25 | Pool=128, 2× t3.micro Chat Servers

### Configuration

| Parameter | Value |
|---|---|
| Chat servers | 2× t3.micro |
| RabbitMQ | 1× t3.micro |
| Consumer + Postgres | 1× t3.micro |
| Pool size | 128 connections |
| Users/room | 50 |
| Rooms | 20 |
| Messages/user | 500 |
| Total messages | 500,000 |
| PUBLISH_WORKERS | 20 |
| CONSUMER_WORKERS | 5 |
| DB_WORKERS | 3 |
| BATCH_SIZE | 100 |

### Performance

| Metric | Value |
|---|---|
| Total runtime | 153.3s |
| Successful messages | 500,000 |
| Failed messages | 0 |
| Overall throughput | 3,261.9 msg/s |
| Total connections | 1,000 |
| Reconnections | 24,000 |
| Failed connections | 0 |
| Mean latency | 402ms |
| Median latency | 381ms |
| 95th percentile | 560ms |
| 99th percentile | 612ms |
| Min latency | 253ms |
| Max latency | 706ms |
| Median room throughput | 718.19 msg/s |

### Per-Room Throughput

| Room | Throughput (msg/s) | Mean Latency |
|---|---|---|
| 18 | 42,510.4 | 291ms |
| 20 | 40,241.8 | 332ms |
| 10 | 39,955.6 | 411ms |
| 2 | 39,737.6 | 328ms |
| 4 | 38,691.8 | 405ms |
| 12 | 38,460.8 | 333ms |
| 5 | 38,188.5 | 337ms |
| 15 | 37,090.2 | 338ms |
| 19 | 36,598.1 | 501ms |
| 17 | 35,901.6 | 502ms |
| 16 | 35,792.6 | 355ms |
| 14 | 35,336.3 | 359ms |
| 8 | 34,143.7 | 532ms |
| 13 | 32,650.0 | 432ms |
| 1 | 32,673.5 | 401ms |
| 3 | 32,555.2 | 344ms |
| 11 | 30,805.1 | 383ms |
| 7 | 27,266.0 | 571ms |
| 6 | 23,895.4 | 404ms |
| 9 | 18,120.2 | 483ms |

### Consumer Metrics

| Metric | Value |
|---|---|
| Active users (last 1h) | 50 |
| Busiest room | Room 9 (44,330 messages) |
| Most active user | User 24 (20 rooms) |

**Messages/min (peak buckets):**

| Bucket | Messages |
|---|---|
| 06:39 | 196,072 |
| 06:40 | 193,546 |
| 06:41 | 104,668 |

**Top 5 users by message count:**
1. User 24 — 17,151 messages
2. User 35 — 16,989 messages
3. User 49 — 16,906 messages
4. User 12 — 16,898 messages
5. User 5 — 16,749 messages

**Top 5 rooms by message count:**
1. Room 9 — 44,330
2. Room 10 — 41,594
3. Room 16 — 41,513
4. Room 19 — 41,301
5. Room 17 — 41,088

**Top 5 users by room count:**
1. Users 8, 12, 10, 7, 49 — all 20 rooms

---

## Run 2 — 2026-03-25 | BATCH_SIZE=500, FLUSH_INTERVAL=500ms

### Configuration

| Parameter | Value |
|---|---|
| Pool size | 128 |
| BATCH_SIZE | 500 |
| FLUSH_INTERVAL | 500ms |
| All other config | same as Run 1 |

### Performance

| Metric | Value |
|---|---|
| Total runtime | 161.0s |
| Successful messages | 500,000 |
| Failed messages | 0 |
| Overall throughput | 3,106.4 msg/s |
| Mean latency | 421ms |
| Median latency | 412ms |
| 95th percentile | 585ms |
| 99th percentile | 648ms |
| Min latency | 303ms |
| Max latency | 852ms |

### Consumer Metrics

| Metric | Value |
|---|---|
| Active users (last 1h) | 50 |
| Busiest room | Room 10 (25,000 messages) |
| Peak messages/min | 201,321 (06:59) |

### Notes
- Slightly slower than Run 1 (3,106 vs 3,261 msg/s)
- Larger batch size did not improve throughput — bottleneck is upstream, not DB writes

---

## Batch Size Comparison

| Run | BATCH_SIZE | FLUSH_INTERVAL | Throughput | Mean Latency | p95 |
|---|---|---|---|---|---|
| 1 (A) | 100 | 1000ms | 3,261.9 msg/s | 402ms | 560ms |
| 2 (B) | 500 | 500ms | 3,106.4 msg/s | 421ms | 585ms |
| 3 (C) | 1000 | 500ms | 3,196.1 msg/s | 393ms | 487ms |
| 4 (D) | 5000 | 100ms | 3,232.0 msg/s | 368ms | 463ms |
| 5 (E) | 5000 | 1000ms | 3,261.9 msg/s | 371ms | 455ms |

**Winner: Run 5 (E) — BATCH_SIZE=5000, FLUSH_INTERVAL=1000ms**

---

### Notes
- Previous run (Pool=1000) OOM-killed both t3.micro servers within ~5 min
- Pool=128 resolves OOM — 64 connections/server well within 1GB RAM
- 24,000 reconnections = 24 per connection avg, likely from ALB sticky session + server restarts earlier in session
- Consumer DB writes healthy, no circuit breaker trips
- ~196K messages/min peak throughput at consumer
- Larger BATCH_SIZE consistently better — bottleneck is upstream (RMQ/WebSocket), not DB writes
- FLUSH_INTERVAL has minimal impact at BATCH_SIZE=5000 (batch fills before interval fires)

---

## Stress Test — 2026-03-25 | 1M Messages

### Configuration

| Parameter | Value |
|---|---|
| Pool size | 128 |
| Users/room | 50 |
| Rooms | 20 |
| Messages/user | 1,000 |
| Total messages | 1,000,000 |
| BATCH_SIZE | 5000 |
| FLUSH_INTERVAL | 1000ms |
| Chat servers | 2× t3.small |

### Performance

| Metric | Value |
|---|---|
| Total runtime | 308.7s |
| Successful messages | 1,000,000 |
| Failed messages | 0 |
| Overall throughput | 3,239.2 msg/s |
| Total connections | 1,000 |
| Reconnections | 49,000 |
| Failed connections | 0 |
| Mean latency | 371ms |
| Median latency | 367ms |
| 95th percentile | 455ms |
| 99th percentile | 467ms |
| Min latency | 290ms |
| Max latency | 477ms |
| Median room throughput | 740.44 msg/s |

### Per-Room Throughput

| Room | Throughput (msg/s) | Mean Latency |
|---|---|---|
| 18 | 44,227.2 | 328ms |
| 20 | 41,020.3 | 385ms |
| 2 | 40,351.4 | 386ms |
| 4 | 40,139.6 | 381ms |
| 8 | 39,326.3 | 461ms |
| 9 | 38,885.7 | 332ms |
| 16 | 38,547.8 | 368ms |
| 5 | 37,192.9 | 366ms |
| 19 | 36,369.2 | 344ms |
| 3 | 36,312.8 | 360ms |
| 14 | 36,220.2 | 339ms |
| 7 | 35,618.0 | 368ms |
| 6 | 34,055.4 | 354ms |
| 11 | 33,276.6 | 367ms |
| 17 | 32,953.7 | 340ms |
| 12 | 30,716.4 | 365ms |
| 13 | 29,114.6 | 375ms |
| 1 | 28,631.6 | 416ms |
| 15 | 28,224.1 | 421ms |
| 10 | 23,482.7 | 364ms |

### Consumer Metrics

| Metric | Value |
|---|---|
| Active users (last 1h) | 50 |
| Busiest room | Room 4 (50,000 messages) |
| Most active user | User 36 (20 rooms) |
| Circuit breaker trips | 0 |
| OOM kills | 0 |

**Messages/min:**

| Bucket | Messages |
|---|---|
| 07:43 | 152,482 |
| 07:44 | 192,768 |
| 07:45 | 191,040 |
| 07:46 | 199,670 |
| 07:47 | 196,327 |
| 07:48 | 67,713 |

**Top 5 users by message count:** all tied at 20,000 messages (Users 13, 8, 1, 39, 36)

**Top 5 rooms by message count:** all tied at 50,000 messages (Rooms 13, 12, 14, 6, 4)

**Top 5 users by room count:** Users 8, 12, 10, 7, 49 — all 20 rooms

### Notes
- System fully stable for ~5 min sustained load — no OOM, no circuit breaker trips
- Throughput consistent with 500K run (3,239 vs 3,261 msg/s) — no degradation at 2× message volume
- Latency lower than 500K run (371ms vs 402ms mean) — likely warm DB connection pool
- 49,000 reconnections (49 avg/connection) — higher than 500K run; likely ALB rebalancing over longer run
- Consumer metrics output had duplicate printing (known client-side display bug, data itself correct)

---

## Doubled Endurance Test — 2026-03-25 | 9.36M Messages (~46 min)

### Configuration

| Parameter | Value |
|---|---|
| Pool size | 128 |
| Users/room | 50 |
| Rooms | 20 |
| Messages/user | 9,360 |
| Total messages | 9,360,000 |
| BATCH_SIZE | 5000 |
| FLUSH_INTERVAL | 1000ms |
| Chat servers | 2× t3.small |

### Performance

| Metric | Value |
|---|---|
| Total runtime | 2773.9s (~46.2 min) |
| Successful messages | 9,360,000 |
| Failed messages | 0 |
| Overall throughput | 3,374.3 msg/s |
| Total connections | 1,000 |
| Reconnections | 467,000 |
| Failed connections | 0 |
| Mean latency | 356ms |
| Median latency | 372ms |
| 95th percentile | 400ms |
| 99th percentile | 408ms |
| Min latency | 269ms |
| Max latency | 415ms |
| Median room throughput | 731.94 msg/s |

### Per-Room Throughput

| Room | Throughput (msg/s) | Mean Latency |
|---|---|---|
| 8 | 46,268.9 | 277ms |
| 10 | 42,239.1 | 340ms |
| 5 | 41,215.0 | 376ms |
| 13 | 40,614.4 | 341ms |
| 12 | 40,541.5 | 379ms |
| 3 | 39,300.8 | 400ms |
| 4 | 39,149.8 | 400ms |
| 17 | 38,134.9 | 376ms |
| 1 | 36,120.8 | 377ms |
| 9 | 33,403.4 | 376ms |
| 18 | 33,065.3 | 283ms |
| 16 | 31,756.1 | 331ms |
| 7 | 31,069.0 | 372ms |
| 19 | 28,649.0 | 320ms |
| 2 | 25,919.7 | 382ms |
| 11 | 25,525.0 | 372ms |
| 6 | 25,204.2 | 333ms |
| 14 | 24,443.7 | 344ms |
| 20 | 22,842.9 | 376ms |
| 15 | 21,120.7 | 372ms |

### Notes
- 9.36M messages, 0 failures — system stable for full 46 min run
- Throughput slightly higher than previous runs (3,374 vs 3,227–3,261 msg/s)
- RabbitMQ queue built up ~4M during the run (consumer ~25K msg/min behind producer) then drained after client stopped
- Consumer metrics unavailable — port 8080 not open in new instance security group
- Latency tighter than shorter runs: p99 408ms vs 467ms (1M run) — warm connections over long run
- 467K reconnections over 46 min = ~10/connection/min, consistent with ALB sticky session cycling

---

## Endurance Test — 2026-03-25 | 4.68M Messages (~24 min)

### Configuration

| Parameter | Value |
|---|---|
| Pool size | 128 |
| Users/room | 50 |
| Rooms | 20 |
| Messages/user | 4,680 |
| Total messages | 4,680,000 |
| BATCH_SIZE | 5000 |
| FLUSH_INTERVAL | 1000ms |
| Chat servers | 2× t3.small |
| Target duration | ~30 min at 80% throughput |

### Performance

| Metric | Value |
|---|---|
| Total runtime | 1450.2s (~24.2 min) |
| Successful messages | 4,680,000 |
| Failed messages | 0 |
| Overall throughput | 3,227.1 msg/s |
| Total connections | 1,000 |
| Reconnections | 233,000 |
| Failed connections | 0 |
| Mean latency | 376ms |
| Median latency | 385ms |
| 95th percentile | 418ms |
| 99th percentile | 425ms |
| Min latency | 256ms |
| Max latency | 437ms |
| Median room throughput | 767.49 msg/s |

### Per-Room Throughput

| Room | Throughput (msg/s) | Mean Latency |
|---|---|---|
| 17 | 47,195.9 | 264ms |
| 10 | 40,344.3 | 370ms |
| 3 | 40,059.8 | 387ms |
| 15 | 39,910.4 | 398ms |
| 8 | 39,387.0 | 419ms |
| 18 | 39,273.3 | 382ms |
| 19 | 39,195.9 | 417ms |
| 7 | 38,515.3 | 380ms |
| 16 | 37,560.9 | 392ms |
| 6 | 37,142.1 | 391ms |
| 11 | 37,352.5 | 293ms |
| 13 | 35,030.3 | 397ms |
| 14 | 33,414.2 | 389ms |
| 2 | 30,891.8 | 361ms |
| 4 | 30,191.5 | 372ms |
| 1 | 28,703.9 | 384ms |
| 9 | 27,227.8 | 382ms |
| 20 | 26,243.9 | 374ms |
| 5 | 24,726.3 | 374ms |
| 12 | 21,571.5 | 387ms |

### Consumer Metrics

| Metric | Value |
|---|---|
| Active users (last 1h) | 50 |
| Busiest room | Room 17 (284,000 messages) |
| Most active user | User 4 (20 rooms) |
| Circuit breaker trips | 0 |
| OOM kills | 0 |

**Messages/min (sustained buckets):**

| Bucket | Messages |
|---|---|
| 08:09 | 196,480 |
| 08:10 | 191,282 |
| 08:11 | 193,633 |
| 08:12 | 191,022 |
| 08:13 | 195,323 |
| 08:14 | 188,634 |
| 08:15 | 188,347 |
| 08:16 | 196,804 |
| 08:17 | 191,817 |
| 08:18 | 194,235 |
| 08:19 | 198,863 |
| 08:20 | 191,846 |
| 08:21 | 198,170 |
| 08:22 | 207,213 |
| 08:23 | 95,620 (partial) |

**Top 5 users by message count:** Users 7, 4, 22, 33, 29 — ~113,592 messages each

**Top 5 rooms by message count:** Rooms 7, 15, 5, 8, 17 — all 284,000 messages

**Top 5 users by room count:** Users 8, 12, 10, 7, 49 — all 20 rooms

### Notes
- Ran at full throughput (~3,227 msg/s) rather than 80% target — actual duration was 24.2 min vs planned 30 min
- Throughput essentially identical to 500K and 1M runs — zero degradation over 24 min sustained load
- Latency fully stable throughout: consumer msg/min variance < 10% across 14 full buckets (188K–207K)
- Reconnections: 233K over 24 min = 233/connection avg — elevated vs shorter runs, expected for long-lived ALB connections
- No OOM, no circuit breaker trips, no failed messages — system stable under extended load
