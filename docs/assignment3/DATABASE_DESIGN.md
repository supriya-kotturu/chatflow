# ChatFlow Assignment 3 — Database Design Document

## 1. Database Choice: PostgreSQL

**Chosen database:** PostgreSQL 16, co-located with consumer-v3 on a single t3.micro EC2 instance.

**Justification:**

| Criterion | PostgreSQL | Alternatives considered |
|---|---|---|
| ACID compliance | Full — ensures no message loss on crash | Cassandra: eventual consistency only |
| Bulk insert API | `COPY FROM` (binary protocol, fastest bulk path) | MySQL: no native COPY equivalent |
| Time-range queries | Native `TIMESTAMPTZ`, efficient B-tree range scans | Redis: not suitable for durable history |
| Upsert support | `ON CONFLICT DO UPDATE` — idempotent writes | MongoDB: upsert available but weaker consistency |
| Operational simplicity | Single binary, `yum install` on Amazon Linux | Cassandra: complex cluster setup for one node |

PostgreSQL's `pgx/v5` driver supports the **binary COPY protocol**, which eliminates per-row parsing overhead and is 3–5× faster than `INSERT` for bulk writes — critical for sustaining 3,000+ msg/s at the DB layer.

Co-locating Postgres with the consumer eliminates network RTT on every write (localhost socket vs. TCP hop).

---

## 2. Schema Design

```sql
CREATE SCHEMA IF NOT EXISTS chatflow;

-- Core message store
CREATE TABLE messages (
    message_id  UUID        PRIMARY KEY,
    user_id     TEXT        NOT NULL,
    room_id     TEXT        NOT NULL,
    server_id   TEXT        NOT NULL,
    username    TEXT        NOT NULL,
    content     TEXT        NOT NULL,
    message_type TEXT       NOT NULL,
    timestamp   TIMESTAMPTZ NOT NULL
);

-- Room participation tracking
CREATE TABLE user_rooms (
    user_id               TEXT        NOT NULL,
    room_id               TEXT        NOT NULL,
    last_activity_timestamp TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (user_id, room_id)
);

-- Pre-aggregated time-bucket stats (1-minute granularity)
CREATE TABLE message_stats (
    bucket        TIMESTAMPTZ PRIMARY KEY,
    message_count BIGINT DEFAULT 0
);

-- Pre-aggregated per-user message counts
CREATE TABLE user_message_stats (
    user_id       TEXT   PRIMARY KEY,
    message_count BIGINT DEFAULT 0
);

-- Pre-aggregated per-room message counts
CREATE TABLE room_message_stats (
    room_id       TEXT   PRIMARY KEY,
    message_count BIGINT DEFAULT 0
);
```

**Design rationale:**

- `messages` stores the raw event log — append-only, UUID primary key prevents duplicates.
- `user_rooms` is an upserted lookup table — deduped at the application layer before each batch, avoiding redundant DB writes.
- `message_stats`, `user_message_stats`, `room_message_stats` are **pre-aggregated counters** updated via `ON CONFLICT DO UPDATE ... SET count = count + excluded.count`. This avoids expensive `COUNT(*)` / `GROUP BY` queries at read time, keeping analytics queries O(1).

---

## 3. Indexing Strategy

```sql
CREATE INDEX idx_messages_room_id_timestamp ON messages(room_id, timestamp DESC);
CREATE INDEX idx_messages_user_id           ON messages(user_id, timestamp DESC);
CREATE INDEX idx_messages_timestamp         ON messages(timestamp);
```

| Index | Query served | Selectivity |
|---|---|---|
| `(room_id, timestamp DESC)` | Get messages in room by time range (Query 1) | High — room filters to ~5% of rows; timestamp narrows further |
| `(user_id, timestamp DESC)` | Get user message history (Query 2) | High — user filters to ~2% of rows |
| `(timestamp)` | Count active users in window (Query 3) | Medium — depends on window size; covering `user_id` scanned within range |

**Composite index decisions:**
- Including `timestamp DESC` in the room and user indexes allows index-only scans for time-range queries — avoids heap fetches for ordered results.
- `user_rooms(user_id, room_id)` is the primary key, so Query 4 (rooms per user) uses the PK index directly.
- No index on `message_type` — low selectivity (3 values), full scan is faster.

**Write impact:**
- 3 indexes on `messages` add ~20–30% overhead per batch insert vs. heap-only. Acceptable given the read performance gains.
- Stats tables have only PK indexes — minimal overhead.

---

## 4. Query Implementation

### Core Queries

**Q1 — Messages in room by time range:**
```sql
SELECT * FROM chatflow.messages
WHERE room_id = $1 AND timestamp BETWEEN $2 AND $3
ORDER BY timestamp ASC LIMIT 1000;
```
Uses `idx_messages_room_id_timestamp`. Target: < 100ms.

**Q2 — User message history:**
```sql
SELECT * FROM chatflow.messages
WHERE user_id = $1 ORDER BY timestamp DESC LIMIT 100;
```
Uses `idx_messages_user_id`. Target: < 200ms.

**Q3 — Active users in window:**
```sql
SELECT COUNT(DISTINCT user_id) FROM chatflow.messages
WHERE timestamp BETWEEN $1 AND $2;
```
Uses `idx_messages_timestamp`. Target: < 500ms.

**Q4 — Rooms per user:**
```sql
SELECT room_id, last_activity_timestamp FROM chatflow.user_rooms
WHERE user_id = $1 ORDER BY last_activity_timestamp DESC;
```
Uses PK index. Target: < 50ms.

### Analytics Queries
- Messages/min: `SELECT bucket, message_count FROM message_stats ORDER BY bucket DESC LIMIT 15`
- Top N users: `SELECT user_id, message_count FROM user_message_stats ORDER BY message_count DESC LIMIT 5`
- Top N rooms: `SELECT room_id, message_count FROM room_message_stats ORDER BY message_count DESC LIMIT 5`

All analytics queries are O(1) scans on small pre-aggregated tables.

---

## 5. Scaling Considerations

**Vertical scaling:** t3.micro (1 vCPU, 1GB RAM) sustained ~150K msg/min DB writes. Upgrading to t3.small (2 vCPU, 2GB RAM) would double throughput and allow larger `shared_buffers`.

**Horizontal scaling:** For production, partition `messages` by `room_id` (hash) or `timestamp` (range) using PostgreSQL declarative partitioning. Each partition pruned at query time. Read replicas via streaming replication for analytics queries.

**Table growth:** At 3,200 msg/s sustained, `messages` grows ~280M rows/day. Time-based partitioning with automated partition drop (retain 30 days) keeps table size bounded.

**Connection pooling:** `pgxpool` with `MaxConns = DB_WORKERS + STATS_WORKERS + 2` (8 connections total). PgBouncer recommended for production to multiplex hundreds of app connections onto a small Postgres connection pool.

---

## 6. Backup and Recovery

**Strategy:**
- `pg_dump` nightly to S3 (logical backup) — point-in-time recovery for data corruption.
- WAL archiving to S3 for continuous archiving — RPO < 5 min.
- For this assignment: manual `pg_dump` before each test run as checkpoint.

**Idempotent writes:** `message_id` UUID is the primary key — duplicate deliveries from RabbitMQ are silently ignored by the PK constraint. No data corruption on retry.

**Dead letter queue:** Failed batches (after circuit breaker opens) written to `dlq_chan` and retried with exponential backoff (1s, 2s, 4s, 8s) once the circuit closes.
