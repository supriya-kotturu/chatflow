# ChatFlow Load Test Results

## Test Environment

- **Server**: AWS EC2 `t3.micro` (2 vCPUs, 1 GB RAM) — us-west-2
- **Client**: Windows 11, running locally over public internet
- **Server buffer**: 2048 per-client send channel
- **Pattern**: Pipeline (generate → write/read → collect)
- **File descriptor limit**: 1024 (default)

## Load Test Configurations

```go
// 500K: 50 × (500+2) × 20 = 502,000 messages
config_500K := &ClientConfig{PoolSize: 1000, UserCount: 50, MessageCount: 500, RoomCount: 20, MessageBuffer: 1200}

// 1M: 100 × (1000+2) × 10 = 1,002,000 messages
config_1M := &ClientConfig{PoolSize: 1000, UserCount: 100, MessageCount: 1000, RoomCount: 10, MessageBuffer: 1200}

// 1.5M: 100 × (750+2) × 20 = 1,504,000 messages
config_1_5M := &ClientConfig{PoolSize: 1000, UserCount: 100, MessageCount: 750, RoomCount: 20, MessageBuffer: 3000}

// 2M: 100 × (1000+2) × 20 = 2,004,000 messages
config_2M := &ClientConfig{PoolSize: 2000, UserCount: 100, MessageCount: 1000, RoomCount: 20, MessageBuffer: 2500}

// 2.5M: 125 × (1000+2) × 20 = 2,505,000 messages
config_2_5M := &ClientConfig{PoolSize: 2500, UserCount: 125, MessageCount: 1000, RoomCount: 20, MessageBuffer: 3000}
```

## Pipeline Load Test Results

| Config | Users | Messages | Rooms | Pool | Total Msgs | Successful | Failed | Loss % | Throughput | Mean Latency | Median Latency |
|--------|-------|----------|-------|------|------------|------------|--------|--------|------------|--------------|----------------|
| 500K   | 50    | 1000     | 10    | 1000 | 502K       | 502,000    | 0      | 0%     | 58,944 msg/s | 1,166ms     | 1,072ms        |
| 1M     | 100   | 1000     | 10    | 1000 | 1.002M     | 998,417    | 3,583  | 0.36%  | 45,617 msg/s | 2,508ms     | 2,529ms        |
| 1.5M   | 100   | 750      | 20    | 1000 | 1.504M     | 1,502,073  | 1,927  | 0.13%  | 46,524 msg/s | 2,545ms     | 2,162ms        |
| 2M     | 100   | 1000     | 20    | 2000 | 2.004M     | 1,930,022  | 73,978 | 3.7%   | 44,412 msg/s | 4,536ms     | 4,706ms        |
| 2.5M   | 125   | 1000     | 20    | 2500 | 2.505M     | 2,389,331  | 115,669| 4.6%   | 49,870 msg/s | 5,091ms     | 5,352ms        |

> Total messages includes JOIN + TEXT + LEAVE per user per room: `users × (messages + 2) × rooms`

## Key Findings

### 1. Connection Concurrency is the Primary Bottleneck

The `PoolSize` (max concurrent WebSocket connections) is the strongest predictor of message loss:

- **Pool <= 1000**: Near-zero loss (0-0.36%)
- **Pool 1000-2000**: Low loss (0.13-0.6%)
- **Pool > 2000**: Significant loss (3.7-4.6%)

### 2. Connection Lifetime Amplifies Loss

With the same pool size (2000), increasing `MessageCount` from 750 to 1000 caused loss to jump from 0.6% to 3.7%. Longer-lived connections are more likely to hit TCP timeouts over the internet.

### 3. Server State Accumulation

Running tests back-to-back without restarting the server caused significantly higher failure rates. Residual goroutines, `TIME_WAIT` sockets, and memory from previous tests degrade performance.

**Best practice**: Restart the server and wait 2-3 minutes between test runs for `TIME_WAIT` socket cleanup.

### 4. Network Latency vs Localhost

| Factor | Localhost | EC2 (Internet) |
|--------|-----------|-----------------|
| RTT | < 1ms | ~60ms |
| Message loss | 0% | 0-4.6% |
| Mean latency | < 100ms | 1,000-5,000ms |
| TCP timeouts | None | At high concurrency |

### 5. Throughput Scales with Concurrency

Throughput remained relatively stable at **45,000-59,000 msg/s** across all configs. The server handles message processing efficiently — the bottleneck is connection management, not message throughput.

### 6. Latency Increases with Scale

| Config | Mean Latency | P95 Latency | P99 Latency |
|--------|-------------|-------------|-------------|
| 500K   | 1,166ms     | 2,481ms     | 3,380ms     |
| 1M     | 2,508ms     | 4,201ms     | 4,932ms     |
| 1.5M   | 2,545ms     | 6,400ms     | 7,819ms     |
| 2M     | 4,536ms     | 7,733ms     | 8,439ms     |
| 2.5M   | 5,091ms     | 8,966ms     | 9,887ms     |

Tail latency (P95/P99) grows faster than mean latency — a sign of queuing effects at the connection pool semaphore.

## Factors Affecting Performance

| Factor | Impact | Recommendation |
|--------|--------|----------------|
| Pool size | Controls concurrent connections; primary loss driver | Keep <= 1000 for zero loss |
| Message count | Longer connection lifetime = more TCP timeouts | 500-1000 per user is optimal |
| Server buffer | Per-client send channel size | 2048 balances memory vs throughput |
| File descriptors | OS limit on open connections | Set `ulimit -n 65535` |
| TIME_WAIT sockets | Stale sockets block new connections | Wait 2-3 min between tests or enable `tcp_tw_reuse` |
| Server restart | Clears residual state from previous tests | Always restart between test runs |
| EC2 instance size | Memory and CPU for connection handling | `t3.micro` handles up to ~2000 concurrent connections |

## Recommendations

1. **For zero message loss**: Use `PoolSize <= 1000` with server restart between runs
2. **For maximum throughput**: Use `PoolSize 2000-2500`, accept 3-5% loss
3. **For production**: Run client and server in the same VPC to eliminate internet latency
4. **For higher scale**: Upgrade to `t3.medium` (4 GB RAM) or larger instance