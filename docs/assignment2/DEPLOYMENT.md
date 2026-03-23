# ChatFlow v2 — Deployment Guide

## Architecture

```
Client → ALB (port 80) → Chat Server 1..N (port 3000) → RabbitMQ (port 5672)
```

Each chat server publishes messages to `chat.exchange` (topic exchange) and consumes from its own per-room auto-delete queues, enabling cross-server fan-out.

---

## Prerequisites

- AWS account with EC2 + ELB access
- SSH key pair (`.pem`)
- Go installed locally for cross-compilation

---

## Stage 0 — Build Locally

```bash
# macOS/Linux
GOOS=linux GOARCH=amd64 make build-server-v2

# Windows CMD
set GOOS=linux && set GOARCH=amd64 && make build-server-v2
```

Produces `bin/chat-server-v2` and `bin/server/html/`.

---

## Stage 1 — Deploy RabbitMQ

### Launch EC2

| Setting | Value |
|---------|-------|
| AMI | Amazon Linux 2023 |
| Type | `t3.medium` |
| Storage | 20 GB gp3 |

**Security Group** (`chatflow-rabbitmq-sg`):

| Port | Source | Purpose |
|------|--------|---------|
| 22 | Your IP | SSH |
| 5672 | `chatflow-server-sg` | AMQP |
| 15672 | Your IP | Management UI |

### Install RabbitMQ

> Amazon Linux 2023 has no RabbitMQ/Erlang in default repos. Install RPMs directly.
> RabbitMQ 4.0+ is required — the Go client uses AMQP 1.0 (RabbitMQ 3.x returns `unexpected protocol version 0.9.1`).

```bash
ssh -i your-key.pem ec2-user@<RABBITMQ-PUBLIC-IP>

sudo yum install -y wget socat logrotate

# Erlang (el9 build — el8 fails on AL2023 due to OpenSSL version)
wget https://github.com/rabbitmq/erlang-rpm/releases/download/v26.2.5/erlang-26.2.5-1.el9.x86_64.rpm
sudo rpm -ivh erlang-26.2.5-1.el9.x86_64.rpm

# RabbitMQ 4.0.3 (el8 noarch — el9 noarch not published for this version)
wget https://github.com/rabbitmq/rabbitmq-server/releases/download/v4.0.3/rabbitmq-server-4.0.3-1.el8.noarch.rpm
sudo rpm -ivh rabbitmq-server-4.0.3-1.el8.noarch.rpm

# Listen on all interfaces
sudo mkdir -p /etc/rabbitmq
echo 'listeners.tcp.default = 0.0.0.0:5672' | sudo tee /etc/rabbitmq/rabbitmq.conf

sudo systemctl enable rabbitmq-server
sudo systemctl start rabbitmq-server
sudo rabbitmq-plugins enable rabbitmq_management
sudo systemctl restart rabbitmq-server

# Create user (guest only works on localhost by default)
sudo rabbitmqctl add_user chatflow <PASSWORD>
sudo rabbitmqctl set_user_tags chatflow administrator
sudo rabbitmqctl set_permissions -p / chatflow ".*" ".*" ".*"
```

Verify: `http://<RABBITMQ-PUBLIC-IP>:15672` (login: chatflow / \<PASSWORD\>)

Note the **private IP** — chat servers connect via private IP.

---

## Stage 2 — Deploy Chat Server

### Launch EC2

| Setting | Value |
|---------|-------|
| AMI | Amazon Linux 2023 |
| Type | `t3.medium` |
| Storage | 8 GB |

**Security Group** (`chatflow-server-sg`):

| Port | Source | Purpose |
|------|--------|---------|
| 22 | Your IP | SSH |
| 3000 | `0.0.0.0/0` | WebSocket |

### Deploy (first time)

```bash
ssh -i your-key.pem ec2-user@<SERVER-PUBLIC-IP> "mkdir -p ~/chatflow/bin/server/html"
scp -i your-key.pem bin/chat-server-v2 ec2-user@<SERVER-PUBLIC-IP>:~/chatflow/bin/
scp -i your-key.pem -r bin/server/html/ ec2-user@<SERVER-PUBLIC-IP>:~/chatflow/bin/server/html/
```

### Redeploy (binary update — service must be stopped first)

> Linux locks the running binary; SCP will fail with `dest open: Failure` if the service is still up.

```bash
# Windows bash (cross-compile)
GOOS=linux GOARCH=amd64 make build-server-v2

ssh -i your-key.pem ec2-user@<SERVER-PUBLIC-IP> "sudo systemctl stop chatflow"
scp -i your-key.pem bin/chat-server-v2 ec2-user@<SERVER-PUBLIC-IP>:~/chatflow/bin/
ssh -i your-key.pem ec2-user@<SERVER-PUBLIC-IP> "sudo systemctl start chatflow && sleep 3 && sudo systemctl is-active chatflow"
```

### Configure

```bash
ssh -i your-key.pem ec2-user@<SERVER-PUBLIC-IP>

cat > ~/chatflow/.env << 'EOF'
NAME=PRODUCTION
PORT=3000
SERVER_HOST=0.0.0.0
RABBIT_HOST=<RABBITMQ-PRIVATE-IP>
RABBIT_PORT=5672
RABBIT_USER=chatflow
RABBIT_PASSWORD=<PASSWORD>
ROOM_COUNT=20
EOF

# Also copy to parent dir (binary looks there first)
cp ~/chatflow/.env ~/chatflow/bin/../.env 2>/dev/null || true
```

### Run as systemd Service

> **Important**: Use `After=network-online.target` (not `network.target`) — the server starts before
> routing is ready otherwise and fails to reach RabbitMQ with "network is unreachable".
> Write the service file with `sudo vi` or `sudo nano` to avoid heredoc `EOF` artifacts.

```bash
sudo vi /etc/systemd/system/chatflow.service
```

Paste:

```ini
[Unit]
Description=ChatFlow WebSocket Server v2
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ec2-user
WorkingDirectory=/home/ec2-user/chatflow/bin
ExecStart=/home/ec2-user/chatflow/bin/chat-server-v2
EnvironmentFile=/home/ec2-user/chatflow/.env
Restart=on-failure
RestartSec=5
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable chatflow
sudo systemctl start chatflow
```

Verify: `curl http://<SERVER-PUBLIC-IP>:3000/health`

### OS Tuning

```bash
ulimit -n 65535
sudo sysctl -w net.ipv4.tcp_tw_reuse=1
sudo sysctl -w net.ipv4.tcp_fin_timeout=15
```

---

## Stage 3 — Load Balanced (2 or 4 Instances)

1. Repeat Stage 2 for each additional instance (same `.env`, same RabbitMQ private IP).
2. Create an **Application Load Balancer**:
   - Scheme: Internet-facing, port 80
   - Security group: TCP 80 from `0.0.0.0/0`
3. Create a **Target Group**:
   - Protocol: HTTP, Port: 3000
   - Health check path: `/health`
   - Enable **stickiness** (1 day) — keeps WebSocket sessions on one backend
   - Register all chat server instances
4. Update local `.env`: `SERVER_HOST=<ALB-DNS-NAME>`, `PORT=80`

For 4-instance test: add 2 more instances to the same target group, no ALB changes needed.

---

## Running Tests

```bash
# Single server (point directly at EC2, bypass ALB)
SERVER_HOST=<SERVER-PUBLIC-IP> PORT=3000 make run-client2

# Load balanced
SERVER_HOST=<ALB-DNS-NAME> PORT=80 make run-client2
```

Restart servers between runs: `sudo systemctl restart chatflow`
Wait 2–3 minutes between runs for `TIME_WAIT` sockets to clear.

---

## Useful Commands

```bash
# Server logs
sudo journalctl -u chatflow -f

# Check server is listening
ss -tlnp | grep 3000

# Kill server (if not using systemd)
pkill chat-server-v2

# RabbitMQ queue stats
sudo rabbitmqctl list_queues name messages consumers

# RabbitMQ connection count
sudo rabbitmqctl list_connections | wc -l

# Monitor CPU/memory during test
watch -n 5 "top -bn1 -p $(pgrep chat-server-v2) | tail -3"

# Active connections
watch -n 5 "ss -s"
```
