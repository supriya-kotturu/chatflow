# ChatFlow Assignment 3 — Performance Report

## System Architecture

```
Client (local) → ALB → 2× Chat Server t3.small (port 3000) → RabbitMQ t3.micro
                                                                      ↓
                                                             Consumer-v3 t3.micro
                                                                      ↓
                                                             PostgreSQL 16 (localhost)
```

**Consumer pipeline:**
```
RabbitMQ persistence_queue
  → 5 consumer goroutines (unmarshal, fan-out)
      ↓ db_chan (buffer 10K)        ↓ stats_chan (non-blocking, drop ok)
  → 3 DB writer goroutines      → 3 stats aggregator goroutines
      ↓ CopyFrom batch (messages)       ↓ upsert (message_stats, user/room counters)
      ↓ upsert batch (user_rooms)
      ↓ circuit breaker → dlq_chan on failure
```

---

## 1. Write Performance

### Maximum Sustained Write Throughput

| Run | Messages | Duration | Client Throughput | Notes |
|---|---|---|---|---|
| Baseline (500K) | 500,000 | 153.3s | 3,261.9 msg/s | Clean DB |
| Stress (1M) | 1,000,000 | 308.7s | 3,239.2 msg/s | No degradation at 2× |
| Endurance (4.68M) | 4,680,000 | 1450.2s | 3,227.1 msg/s | 24 min sustained |
| Doubled Endurance (9.36M) | 9,360,000 | 2773.9s | 3,374.3 msg/s | 46 min sustained |

**Maximum sustained client throughput: 3,374 msg/s**
**Consumer DB write rate: ~150K–200K msg/min (~2,500–3,333 msg/s)**

### Latency Percentiles

| Run | Mean | p50 | p95 | p99 | Min | Max |
|---|---|---|---|---|---|---|
| Baseline (500K) | 402ms | 381ms | 560ms | 612ms | 253ms | 706ms |
| Stress (1M) | 371ms | 367ms | 455ms | 467ms | 290ms | 477ms |
| Endurance (4.68M) | 376ms | 385ms | 418ms | 425ms | 256ms | 437ms |
| Doubled Endurance (9.36M) | 356ms | 372ms | 400ms | 408ms | 269ms | 415ms |

Latency **improved** with longer runs — warm connection pools and JIT-compiled hot paths in the Go runtime.

---

## 2. Batch Size Optimization

All runs: 500K messages, 2× t3.small chat servers, Pool=128.

| Run | BATCH_SIZE | FLUSH_INTERVAL | Throughput | Mean Latency | p95 |
|---|---|---|---|---|---|
| A | 100 | 1000ms | 3,261.9 msg/s | 402ms | 560ms |
| B | 500 | 500ms | 3,106.4 msg/s | 421ms | 585ms |
| C | 1000 | 500ms | 3,196.1 msg/s | 393ms | 487ms |
| D | 5000 | 100ms | 3,232.0 msg/s | 368ms | 463ms |
| E | 5000 | 1000ms | 3,261.9 msg/s | 371ms | 455ms |

**Optimal configuration: BATCH_SIZE=5000, FLUSH_INTERVAL=1000ms**

**Key findings:**
- Larger batch sizes reduce per-row overhead in `CopyFrom` — fewer round-trips to Postgres per unit of data.
- FLUSH_INTERVAL has minimal impact at BATCH_SIZE=5000: batches fill before the interval fires.
- BATCH_SIZE=500 was worse than BATCH_SIZE=100 — smaller batches with shorter flush intervals created more frequent, smaller writes with higher overhead per message.
- The bottleneck is upstream (RabbitMQ → consumer → db_chan), not DB write latency. DB writes consistently complete in < 1s per 5000-row batch.

---

## 3. System Stability Under Load

### Queue Depth Behavior

| Test | Peak Queue Depth | Behavior |
|---|---|---|
| Baseline 500K | ~0 (stable) | Consumer kept up with producer |
| Stress 1M | ~0 (stable) | Consumer kept up with producer |
| Endurance 4.68M | ~0 (stable) | 24 min with no queue buildup |
| Doubled Endurance 9.36M | ~4M at peak | Consumer ~25K msg/min behind producer; drained after client stopped |

The consumer's DB write rate (~150K msg/min) is slightly below the producer's rate (~192K msg/min) over very long runs due to index update overhead on large tables. Queue fully drained after each test.

### Memory and Connections

- **Chat servers**: Pool=128 → 64 WebSocket connections/server. Well within t3.small 2GB RAM. Previous Pool=1000 caused OOM on t3.micro (1GB).
- **Consumer DB pool**: `MaxConns = DB_WORKERS + STATS_WORKERS + 2 = 8`. No connection exhaustion observed.
- **RabbitMQ**: No OOM. Queue depths stayed manageable. Stale queues cleaned between sessions.

### Circuit Breaker

- **0 circuit breaker trips** across all runs.
- Configured: trip after 5 consecutive DB failures, 30s open timeout, 5 requests in half-open.

### Reconnections

| Test | Reconnections | Rate |
|---|---|---|
| 500K (153s) | 24,000 | 156/min |
| 1M (309s) | 49,000 | 158/min |
| 4.68M (1450s) | 233,000 | 160/min |
| 9.36M (2774s) | 467,000 | 168/min |

Reconnection rate is **consistent across all run lengths** — confirms no progressive degradation. Rate driven by ALB sticky session cycling, not system instability.

---

## 4. Resource Utilization

### Consumer Instance (t3.micro, 1 vCPU, 1GB RAM)

| Metric | Observed |
|---|---|
| Disk usage (peak) | 80% of 8GB EBS (~6.4GB) |
| DB rows at peak | ~8.1M messages |
| Estimated row size | ~200–250 bytes/row with indexes |
| Estimated data size | ~1.9–2.0GB |

### Chat Server Instances (t3.small, 2 vCPU, 2GB RAM)

- Pool=128: ~64 concurrent WebSocket connections per server
- PUBLISH_WORKERS=20: 20 goroutines publishing to RabbitMQ
- No OOM events with t3.small

---

## 5. Bottleneck Analysis

### Primary Bottleneck: Consumer DB Write Rate

The t3.micro consumer instance (1 vCPU, 1GB RAM) is the system bottleneck:
- Producer sustained: ~3,200–3,374 msg/s
- Consumer drain rate: ~2,500 msg/s average (peaks to 3,333 msg/s with warm pool)
- Gap: ~25K msg/min at peak load (9.36M test)

**Root causes:**
1. Single-vCPU instance — DB writers, stats aggregators, and consumer goroutines compete for the same CPU.
2. Index update overhead grows with table size — `CopyFrom` is fast but the 3 B-tree indexes on `messages` add latency at scale.
3. `user_rooms` upsert is deduped at the application layer but still issues one upsert per batch.

**Proposed solutions:**
1. Upgrade consumer to t3.small (2 vCPU) — eliminates CPU contention, estimated 40–60% throughput gain.
2. Partition `messages` by month — keeps active partition small, reduces index depth.
3. Increase `DB_WORKERS` from 3 to 6 — more parallel CopyFrom batches.
4. Use `pg_partman` for automated partition management.

### Secondary Bottleneck: ALB Reconnections

~160 reconnections/min/1000 connections is higher than expected. ALB sticky sessions help but connection cycling still occurs due to:
- ALB idle connection timeout (60s default)
- Server restart during earlier sessions caused reconnection storms

**Proposed solution:** Tune ALB idle timeout to 300s; implement WebSocket ping/pong keep-alive.

### Trade-offs Made

| Decision | Trade-off |
|---|---|
| Pre-aggregated stats tables | Faster reads, eventual consistency (counts lag behind by up to one batch) |
| Non-blocking stats_chan (drop ok) | Never blocks consumer on analytics; minor stat undercounting acceptable |
| BATCH_SIZE=5000 | Lower write latency than smaller batches; ~5s max delay before message persisted |
| Co-located Postgres | Eliminates network RTT; limits horizontal scaling of DB layer |
| Async Accept() | Credits flow continuously; messages accepted before DB write confirms |

---

## 6. Metrics API Output (Client Log)

The consumer exposes a `/metrics` HTTP endpoint (port 8080) called automatically by the client at test completion. Sample output from Run 1 (500K baseline):

```
========== Consumer Metrics ==========

--- Core ---
  Active users (last 1h): 50
  Busiest room:           9 (44,330 messages)
  Most active user:       24 (20 rooms)
    room 18                   last active: 2026-03-25T06:39:21Z
    room 3                    last active: 2026-03-25T06:39:18Z
    ...

--- Messages/min (last 15 buckets) ---
  2026-03-25T06:39:00Z  196,072 msg
  2026-03-25T06:40:00Z  193,546 msg
  2026-03-25T06:41:00Z  104,668 msg

--- Top 5 users by message count ---
  1. 24 — 17,151 messages
  2. 35 — 16,989 messages
  3. 49 — 16,906 messages
  4. 12 — 16,898 messages
  5. 5  — 16,749 messages

--- Top 5 rooms by message count ---
  1. 9  — 44,330
  2. 10 — 41,594
  3. 16 — 41,513
  4. 19 — 41,301
  5. 17 — 41,088

--- Top 5 users by room count ---
  1. Users 8, 12, 10, 7, 49 — all 20 rooms
======================================
```

**API implementation:** `GET /metrics` on consumer port 8080 returns JSON with:
- `core.active_users` — COUNT(DISTINCT user_id) WHERE timestamp > now()-1h
- `core.room_messages` — busiest room from `room_message_stats`
- `core.user_rooms` — most active user + room list from `user_rooms`
- `analytics.messages_per_minute` — last 15 buckets from `message_stats`
- `analytics.top_users_by_message_count` — top 5 from `user_message_stats`
- `analytics.top_rooms_by_message_count` — top 5 from `room_message_stats`
- `analytics.top_users_by_room_count` — top 5 from `user_rooms` GROUP BY user_id

---

## 7. Innovations and Optimizations

1. **Binary COPY protocol** (`pgx CopyFrom`) — 3–5× faster bulk inserts than multi-row INSERT.
2. **Pre-aggregated analytics tables** — O(1) analytics queries regardless of message volume.
3. **Application-layer deduplication** of `user_rooms` before batch insert — reduces upsert conflicts and lock contention.
4. **Async Accept() before ConsumeMessage** — decouples RabbitMQ credit flow from DB write latency, preventing consumer deadlock under backpressure.
5. **Circuit breaker + DLQ** — failed batches redirected to in-memory DLQ, retried with backoff; system never drops messages silently.
