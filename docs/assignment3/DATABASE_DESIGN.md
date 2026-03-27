# ChatFlow Assignment 3 — Database Design Document

## 1. Database Choice: PostgreSQL 16

**Chosen database:** PostgreSQL 16, co-located with consumer-v3 on a t3.micro EC2 instance.

### Justification

| Criterion | PostgreSQL | Alternatives |
|---|---|---|
| Bulk insert API | Native `COPY FROM` binary protocol — 3–5× faster than `INSERT` | MySQL: no native COPY; Cassandra: batch writes only |
| Upsert | `ON CONFLICT DO UPDATE` — idempotent per-key increment | MongoDB: `$inc` upsert available; MySQL: `ON DUPLICATE KEY` (weaker) |
| Time-range queries | `TIMESTAMPTZ`, B-tree range scans, `date_trunc` bucketing | Redis: no durable history; DynamoDB: requires sort key design upfront |
| ACID compliance | Full — no message loss on crash or retry | Cassandra: eventual consistency; tunable but complex |
| Operational simplicity | Single binary, `yum install postgresql16` | Cassandra: cluster setup even for one node |

### PostgreSQL-specific features used

**Binary COPY protocol (`pgx CopyFrom`):**
The `pgx` driver's `CopyFrom` uses PostgreSQL's binary wire format, bypassing per-row SQL parsing and executor overhead. At 5,000 rows/batch this sustains ~150K–200K msg/min on a single t3.micro vCPU — roughly 3–5× what equivalent `INSERT` batches achieve.

**`unnest()` for parameterized bulk inserts (alternative pattern):**
When `CopyFrom` is unavailable (e.g., inside a transaction with other statements), PostgreSQL supports array unnesting to send one round-trip with typed arrays:

```sql
INSERT INTO messages (message_id, user_id, room_id, username, content, timestamp)
SELECT * FROM unnest(
    $1::uuid[], $2::text[], $3::text[], $4::text[], $5::text[], $6::timestamptz[]
) AS t(message_id, user_id, room_id, username, content, timestamp)
ON CONFLICT (message_id) DO NOTHING;
```

This is not available in standard ANSI SQL or most other databases and avoids the N×parameter explosion of row-by-row inserts.

**`ON CONFLICT DO UPDATE` for atomic counter increments:**
Pre-aggregated stats use upsert to atomically accumulate counts without read-modify-write races:

```sql
INSERT INTO message_stats (bucket, message_count)
VALUES (date_trunc('minute', $1), $2)
ON CONFLICT (bucket) DO UPDATE
    SET message_count = message_stats.message_count + excluded.message_count;
```

`date_trunc('minute', timestamp)` normalizes any timestamp to its minute boundary in a single expression — used for all time-bucket aggregation.

**`TIMESTAMPTZ`:**
Stores timestamps as UTC internally regardless of the inserting server's timezone. Prevents silent data corruption when chat servers run in different regions.

---

## 2. Schema Design

```sql
-- Core message store (append-only event log)
CREATE TABLE messages (
    message_id   UUID        PRIMARY KEY,
    user_id      TEXT        NOT NULL,
    room_id      TEXT        NOT NULL,
    server_id    TEXT        NOT NULL,
    username     TEXT        NOT NULL,
    content      TEXT        NOT NULL,
    message_type TEXT        NOT NULL,
    timestamp    TIMESTAMPTZ NOT NULL
);

-- Room participation tracking (upserted per batch, deduped at app layer)
CREATE TABLE user_rooms (
    user_id                 TEXT        NOT NULL,
    room_id                 TEXT        NOT NULL,
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

- `messages` is an append-only log. UUID PK silently rejects duplicate RabbitMQ deliveries — idempotent by construction with no extra application logic.
- `user_rooms` composite PK `(user_id, room_id)` covers Q4 directly. Application-layer deduplication before each batch avoids redundant upsert conflicts and lock contention on the index.
- `message_stats`, `user_message_stats`, `room_message_stats` are pre-aggregated counters updated via `ON CONFLICT DO UPDATE`. Analytics queries become O(1) scans on small tables instead of `COUNT(*) GROUP BY` over tens of millions of rows.

---

## 3. Indexing Strategy

```sql
CREATE INDEX idx_messages_room_id_timestamp ON messages(room_id, timestamp DESC);
CREATE INDEX idx_messages_user_id           ON messages(user_id, timestamp DESC);
CREATE INDEX idx_messages_timestamp         ON messages(timestamp);
```

| Index | Query served | Selectivity |
| --- | --- | --- |
| `(room_id, timestamp DESC)` | Q1: messages in room by time range | High — room filters ~5% of rows; timestamp narrows further |
| `(user_id, timestamp DESC)` | Q2: user message history | High — user filters ~2% of rows |
| `(timestamp)` | Q3: COUNT(DISTINCT user_id) in window | Medium — depends on window size |
| `(user_id, room_id)` PK on user_rooms | Q4: rooms per user | Exact — PK B-tree lookup |

**Composite index decisions:**

- Including `timestamp DESC` in room/user indexes enables ordered index scans — results come out sorted with no separate sort step, and `LIMIT` queries terminate early.
- No index on `message_type` — 3 distinct values, full scan is faster than an index lookup at this cardinality.
- Stats tables carry only their PK index — they're tiny (one row per minute/user/room), heap scan always wins.

**Write overhead:** 3 indexes on `messages` add ~20–30% overhead per `CopyFrom` batch vs. heap-only, which is acceptable given the read query requirements.

---

## 4. Scaling Considerations

**Vertical (immediate):** The t3.micro consumer (1 vCPU, 1GB RAM) is the current bottleneck at ~2,500 msg/s average write rate. A t3.small (2 vCPU) is estimated to add 40–60% throughput by eliminating CPU contention between consumer goroutines, DB writers, and stats aggregators sharing one core.

**Table partitioning (medium-term):** At 3,200 msg/s sustained, `messages` grows ~280M rows/day. Declarative range partitioning by month keeps active partition size bounded, reduces index depth, and allows cheap partition drops for retention policy:

```sql
CREATE TABLE messages (...) PARTITION BY RANGE (timestamp);
CREATE TABLE messages_2026_03 PARTITION OF messages
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');
```

**Read replicas:** Stream WAL to a standby for analytics queries — decouples the write path from reporting load with no schema changes required.

**Connection pooling:** Current `pgxpool` with `MaxConns = 8` (DB_WORKERS + STATS_WORKERS + 2). For production with many app instances, PgBouncer in transaction mode multiplexes hundreds of client connections onto a small Postgres connection pool without exhausting `max_connections`.

---

## 5. Backup and Recovery

**Idempotent writes:** `message_id` UUID PK rejects duplicate deliveries silently. No special deduplication logic is needed on recovery — re-replaying RabbitMQ messages is safe.

**Point-in-time recovery:** WAL archiving to S3 (`archive_mode = on`) provides continuous archiving with RPO < 5 minutes. Combined with a nightly `pg_dump` logical backup for fast restore of last-known-good state without WAL replay.

**For this assignment:** Manual `pg_dump` checkpoint before each test run:

```bash
pg_dump -Fc chatflow > chatflow_$(date +%Y%m%d_%H%M).dump
```

**Dead letter queue:** Failed DB batches (after circuit breaker opens) are redirected to an in-memory `dlq_chan` and retried with exponential backoff (1s → 2s → 4s → 8s) once the circuit closes. No messages are silently dropped — worst case is delayed persistence, not data loss.
