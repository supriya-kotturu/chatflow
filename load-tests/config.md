# Load Test Configurations

## Client Configuration (client-part2/main.go)

| Parameter | Baseline | Stress | Endurance | Doubled Endurance |
|---|---|---|---|---|
| PoolSize | 128 | 128 | 128 | 128 |
| UserCount | 50 | 50 | 50 | 50 |
| RoomCount | 20 | 20 | 20 | 20 |
| MessageCount | 500 | 1000 | 4680 | 9360 |
| MessageBuffer | 60000 | 120000 | 560000 | 1120000 |
| Total messages | 500,000 | 1,000,000 | 4,680,000 | 9,360,000 |

## Consumer Configuration (.env)

| Parameter | Value |
|---|---|
| CONSUMER_WORKERS | 5 |
| DB_WORKERS | 3 |
| STATS_WORKERS | 3 |
| BATCH_SIZE | 5000 |
| FLUSH_INTERVAL | 1000ms |
| METRICS_PORT | 8080 |

## Chat Server Configuration (.env)

| Parameter | Value |
|---|---|
| PUBLISH_WORKERS | 20 |
| ROOM_COUNT | 20 |
| PORT | 3000 |

## Infrastructure

| Role | Instance | Count |
|---|---|---|
| Chat Server | t3.small | 2 |
| RabbitMQ | t3.micro | 1 |
| Consumer + Postgres | t3.micro | 1 |
| Load Balancer | ALB (sticky sessions) | 1 |

## Batch Size Sweep Configuration

| Run | BATCH_SIZE | FLUSH_INTERVAL |
|---|---|---|
| A | 100 | 1000ms |
| B | 500 | 500ms |
| C | 1000 | 500ms |
| D | 5000 | 100ms |
| E | 5000 | 1000ms |

See [../docs/assignment3/RESULTS.md](../docs/assignment3/RESULTS.md) for full results.
