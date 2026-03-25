# ChatFlow v3 — Deployment Guide

## Architecture

```
Client → ALB (port 80) → Chat Server 1..N (port 3000) → RabbitMQ (port 5672)
                                                                ↓
                                                        Consumer-v3 (port 8080 metrics)
                                                                ↓
                                                        PostgreSQL (port 5432, localhost)
```

Messages flow from chat servers → `chat.exchange` (topic, `room.#`) → `persistence_queue` → consumer-v3 → PostgreSQL.

---

## EC2 Infrastructure

| Role | Type | Count |
|---|---|---|
| Chat Server | `t3.small` | 2 |
| RabbitMQ | `t3.micro` | 1 |
| Consumer + Postgres | `t3.micro` | 1 |

All instances in the same VPC (Oregon `us-west-2`). Services communicate over **private IPs**.

---

## Prerequisites

- SSH key pair at `../keys/chatflow-key.pem`
- Go installed locally (for cross-compilation)
- Docker Desktop (for local dev only)

---

## Stage 0 — Build Locally (Linux binaries)

```bash
make build-all
```

Produces:
- `bin/chat-server-v2` — chat server
- `bin/consumer-v3` — persistence consumer

> **Note**: SCP will fail with `dest open: Failure` if the target service is still running (Linux locks the binary). Always stop the service before redeploying.

---

## Stage 1 — RabbitMQ

Refer to [assignment2/DEPLOYMENT.md](../assignment2/DEPLOYMENT.md) Stage 1 for full install steps (Erlang + RabbitMQ 4.0+ RPMs, `rabbitmq.conf`, user creation).

**Security group** must allow:
- Port `5672` from all chat server and consumer private IPs (or VPC CIDR `172.31.0.0/16`)
- Port `15672` from your IP (management UI)

Note the **private IP** — all other services connect to it.

---

## Stage 2 — PostgreSQL on Consumer Instance

### Install

```bash
ssh -i ../keys/chatflow-key.pem ec2-user@<CONSUMER-PUBLIC-IP>

sudo yum install -y postgresql16-server
sudo postgresql-setup --initdb
sudo systemctl enable postgresql
sudo systemctl start postgresql
```

### Create user and database

```bash
sudo -u postgres psql << 'EOF'
CREATE USER chatflow WITH PASSWORD 'chatflow-pwd';
CREATE DATABASE chatflow OWNER chatflow;
\c chatflow
GRANT ALL ON DATABASE chatflow TO chatflow;
EOF
```

### Apply schema

```bash
# From local machine
scp -i ../keys/chatflow-key.pem database/schema.sql ec2-user@<CONSUMER-PUBLIC-IP>:~/
ssh -i ../keys/chatflow-key.pem ec2-user@<CONSUMER-PUBLIC-IP> \
  "sudo -u postgres psql -d chatflow -f ~/schema.sql"
```

### Grant permissions (if tables owned by postgres)

```bash
ssh -i ../keys/chatflow-key.pem ec2-user@<CONSUMER-PUBLIC-IP> \
  "sudo -u postgres psql -d chatflow -c \"
    GRANT ALL ON SCHEMA chatflow TO chatflow;
    GRANT ALL ON ALL TABLES IN SCHEMA chatflow TO chatflow;
    GRANT ALL ON ALL SEQUENCES IN SCHEMA chatflow TO chatflow;\""
```

### Verify TCP auth (`pg_hba.conf`)

`host all all 127.0.0.1/32 md5` must be present. Consumer connects via TCP to `127.0.0.1:5432`.

---

## Stage 3 — Deploy Chat Servers

### First deploy

```bash
ssh -i ../keys/chatflow-key.pem ec2-user@<SERVER-PUBLIC-IP> "mkdir -p ~/chatflow/bin/server/html"
scp -i ../keys/chatflow-key.pem bin/chat-server-v2 .env ec2-user@<SERVER-PUBLIC-IP>:~/chatflow/bin/
chmod +x ~/chatflow/bin/chat-server-v2
```

### Redeploy

```bash
ssh -i ../keys/chatflow-key.pem ec2-user@<SERVER-PUBLIC-IP> \
  "sudo systemctl stop chatflow && rm -f ~/chatflow/bin/chat-server-v2"
scp -i ../keys/chatflow-key.pem bin/chat-server-v2 .env ec2-user@<SERVER-PUBLIC-IP>:~/chatflow/bin/
ssh -i ../keys/chatflow-key.pem ec2-user@<SERVER-PUBLIC-IP> \
  "chmod +x ~/chatflow/bin/chat-server-v2 && sudo systemctl start chatflow"
```

### `.env`

```
NAME=PRODUCTION
PORT=3000
RABBIT_HOST=<RABBITMQ-PRIVATE-IP>
RABBIT_PORT=5672
RABBIT_USER=chatflow
RABBIT_PASSWORD=chatflow-pwd
ROOM_COUNT=20
PUBLISH_WORKERS=20
```

### systemd service

```bash
sudo vi /etc/systemd/system/chatflow.service
```

```ini
[Unit]
Description=ChatFlow WebSocket Server v2
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ec2-user
WorkingDirectory=/home/ec2-user/chatflow/bin
EnvironmentFile=/home/ec2-user/chatflow/.env
ExecStart=/home/ec2-user/chatflow/bin/chat-server-v2
Restart=on-failure
RestartSec=5
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload && sudo systemctl enable chatflow && sudo systemctl start chatflow
```

---

## Stage 4 — Deploy Consumer-v3

### First deploy

```bash
ssh -i ../keys/chatflow-key.pem ec2-user@<CONSUMER-PUBLIC-IP> "mkdir -p ~/chatflow/bin"
scp -i ../keys/chatflow-key.pem bin/consumer-v3 ec2-user@<CONSUMER-PUBLIC-IP>:~/chatflow/bin/
ssh -i ../keys/chatflow-key.pem ec2-user@<CONSUMER-PUBLIC-IP> \
  "chmod +x ~/chatflow/bin/consumer-v3"
```

### Redeploy

```bash
ssh -i ../keys/chatflow-key.pem ec2-user@<CONSUMER-PUBLIC-IP> \
  "sudo systemctl stop consumer-v3 && rm -f ~/chatflow/bin/consumer-v3"
scp -i ../keys/chatflow-key.pem bin/consumer-v3 ec2-user@<CONSUMER-PUBLIC-IP>:~/chatflow/bin/
ssh -i ../keys/chatflow-key.pem ec2-user@<CONSUMER-PUBLIC-IP> \
  "chmod +x ~/chatflow/bin/consumer-v3 && sudo systemctl start consumer-v3"
```

### `.env`

```
NAME=PRODUCTION
RABBIT_HOST=<RABBITMQ-PRIVATE-IP>
RABBIT_PORT=5672
RABBIT_USER=chatflow
RABBIT_PASSWORD=chatflow-pwd
DB_HOST=127.0.0.1
DB_PORT=5432
DB_USER=chatflow
DB_PASSWORD=chatflow-pwd
DB_NAME=chatflow
CONSUMER_WORKERS=5
DB_WORKERS=3
STATS_WORKERS=3
BATCH_SIZE=100
FLUSH_INTERVAL=1000
METRICS_PORT=8080
```

### systemd service

```bash
sudo vi /etc/systemd/system/consumer-v3.service
```

```ini
[Unit]
Description=ChatFlow Consumer v3
After=network.target postgresql.service

[Service]
Type=simple
User=ec2-user
WorkingDirectory=/home/ec2-user/chatflow/bin
EnvironmentFile=/home/ec2-user/chatflow/.env
ExecStart=/home/ec2-user/chatflow/bin/consumer-v3
Restart=always
RestartSec=5
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload && sudo systemctl enable consumer-v3 && sudo systemctl start consumer-v3
```

---

## Stage 5 — ALB

Refer to [assignment2/DEPLOYMENT.md](../assignment2/DEPLOYMENT.md) Stage 3.

Target group: port `3000`, health check `/health`, stickiness enabled.

---

## Local Development

```bash
# Start Postgres + RabbitMQ in Docker
make docker-up

# Run all 3 services (3 terminals)
make run-consumer-v3
make run-server-v2
make run-client2

# Check DB
docker exec chat-flow-postgres-1 psql -U chatflow -d chatflow \
  -c "SET search_path TO chatflow; SELECT COUNT(*) FROM messages;"

# Check metrics
curl -s http://localhost:8080/metrics | python -m json.tool
```

`.env.local` for local dev:

```
PORT=3000
SERVER_HOST=127.0.0.1
RABBIT_HOST=127.0.0.1
RABBIT_USER=chatflow
RABBIT_PASSWORD=chatflow-pwd
DB_HOST=127.0.0.1
CONSUMER_HOST=127.0.0.1
METRICS_PORT=8080
```

---

## Useful Commands

```bash
# Consumer logs
ssh -i ../keys/chatflow-key.pem ec2-user@<CONSUMER-PUBLIC-IP> \
  "sudo journalctl -u consumer-v3 -f"

# Check metrics from EC2 consumer
curl -s http://<CONSUMER-PUBLIC-IP>:8080/metrics | python -m json.tool

# RabbitMQ queue depth
ssh -i ../keys/chatflow-key.pem ec2-user@<RABBITMQ-PUBLIC-IP> \
  "sudo rabbitmqctl list_queues name messages consumers"

# Delete stale queues (0 consumers) between runs
ssh -i ../keys/chatflow-key.pem ec2-user@<RABBITMQ-PUBLIC-IP> \
  "sudo rabbitmqctl list_queues name consumers | awk '\$2 == \"0\" {print \$1}' | xargs -r -I{} sudo rabbitmqctl delete_queue {}"

# Check Postgres row counts
ssh -i ../keys/chatflow-key.pem ec2-user@<CONSUMER-PUBLIC-IP> \
  "sudo -u postgres psql -d chatflow -c 'SET search_path TO chatflow; SELECT COUNT(*) FROM messages;'"
```
