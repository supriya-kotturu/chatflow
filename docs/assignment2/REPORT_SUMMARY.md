# ChatFlow v2 — Performance Summary

Test config (all runs): 502,000 messages, 50 users/room, 500 msgs/user, 20 rooms, 1,000 client connections.

---

## Table 1 — Varying `PUBLISH_WORKERS`

_Pool=1000, 1 server (t3.small), RabbitMQ t3.medium_

| Workers | Throughput | Mean latency | Peak queue | Meets target (<1,000) |
|---------|------------|-------------|------------|----------------------|
| 5 | 1,583 msg/s | 156,188ms | ~10 | ✓ |
| 15 | 1,982 msg/s | 114,636ms | ~20 | ✓ |
| 20 | 2,091 msg/s | 114,804ms | ~20 | ✓ ← optimal |
| 40 | 2,079 msg/s | 115,779ms | ~35 | ✓ |
| 80 | 2,037 msg/s | 117,041ms | ~10,000 | ✗ |

**Finding**: Workers=20 is the sweet spot. Fewer workers under-saturate AMQP publishers; more workers flood RabbitMQ with concurrent publishes, spiking queue depth and risking AMQP credit exhaustion deadlock.

---

## Table 2 — Varying `PoolSize` (Client Connection Semaphore)

_Workers=20, 1 server (t3.small), RabbitMQ t3.micro_

| PoolSize | Throughput | Mean latency | Median latency | Peak queue |
|----------|------------|-------------|----------------|------------|
| 16 | 4,216 msg/s | 917ms | 856ms | ~30 |
| 32 | 3,881 msg/s | 2,076ms | 1,943ms | ~25 |
| 64 | 3,512 msg/s | 4,372ms | 4,130ms | ~45 |
| 128 | 3,278 msg/s | 9,543ms | 10,491ms | ~35 |
| 256 | 2,888 msg/s | 20,771ms | 20,660ms | ~35 |
| 512 | 2,421 msg/s | 47,776ms | 47,456ms | ~22 |
| 1,000 | 2,091 msg/s | 114,804ms | 117,544ms | ~20 |

**Finding**: Smaller pool = dramatically better throughput and lower latency. Pool=16 gives 4,216 msg/s (2× better than Pool=1000). Serializing connection ramp-up prevents simultaneous burst that overwhelms `publishChan` and RabbitMQ at the same time. Pool=64 is the best balance of throughput, latency, and queue stability.

---

## Table 3 — Varying Server Count, Fixed Config (Pool=1000, Workers=5/server)

_All servers t3.small, RabbitMQ t3.medium, via ALB (except 1-server direct)_

| Servers | Total workers | Runtime | Throughput | vs 1 server | Mean latency | Peak queue |
|---------|--------------|---------|------------|-------------|-------------|------------|
| 1 | 5 | 317.2s | 1,583 msg/s | — | 156,188ms | ~10 |
| 2 | 10 | 328.3s | 1,529 msg/s | -3.4% | 152,741ms | ~80–100 |
| 4 | 20 | 538.1s | 933 msg/s | -41.0% | 254,301ms | ~130 |

**Finding**: Throughput degrades monotonically with server count. With 4 servers, throughput is 41% lower and latency is 63% higher than 1 server. Each additional server adds publishers faster than it adds consumers — RabbitMQ fan-out overhead is the binding constraint.

---

## Table 4 — Varying Server Count at Different Pool Sizes (Workers=20/server)

_t3.small/micro instances, via ALB_

| Pool | Servers | Throughput | vs 1 server | Mean latency | Peak queue |
|------|---------|------------|-------------|-------------|------------|
| 64 | 1 | 3,512 msg/s | — | 4,372ms | ~45 |
| 64 | 2 | 3,708 msg/s | +5.6% | 4,182ms | ~130 |
| 256 | 1 | 2,888 msg/s | — | 20,771ms | ~35 |
| 256 | 2 | 2,904 msg/s | +0.6% | 21,335ms | ~100 |
| 1,000 | 1 | 2,091 msg/s | — | 114,804ms | ~20 |
| 1,000 | 2 | 2,054 msg/s | -1.8% | 114,410ms | ~300 |

**Finding**: Horizontal scaling provides negligible benefit at all pool sizes. The only positive gain (+5.6% at Pool=64) is because the small pool constrains the client — connections ramp up serially so neither server is fully saturated. At Pool=1,000, adding a second server actually reduces throughput (-1.8%) while spiking queue depth 15×. RabbitMQ fan-out overhead offsets any per-server compute savings.

---

## Summary — Key Findings

| Dimension | Optimal value | Effect of increasing |
|-----------|--------------|---------------------|
| `PUBLISH_WORKERS` | 20 | >40 floods broker; <20 under-saturates publishers |
| `PoolSize` | 16–64 | Higher = more burst pressure; lower = better throughput and latency |
| Server count | 1 | Each additional server adds fan-out overhead; net negative at Pool=1000 |
| Server instance | t3.small (single) | t3.micro OOMs at 500+ concurrent connections; t3.small required |
