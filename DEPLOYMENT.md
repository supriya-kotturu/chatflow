# Deploying ChatFlow Server on AWS EC2

## Prerequisites

- AWS account with EC2 access
- SSH key pair (`.pem` file)
- Go installed locally for cross-compilation

## 1. Build the Server Binary

Cross-compile for Linux and copy template files using the Makefile:

```bash
set GOOS=linux
set GOARCH=amd64
make build-server
```

This builds `bin/chat-server` and copies `server/html/*` to `bin/server/html/`.

The resulting `bin/` directory structure:

```text
bin/
├── chat-server
└── server/
    └── html/
        ├── index.html
        ├── style.css
        └── script.js
```

## 2. Launch an EC2 Instance

- **AMI**: Amazon Linux 2023 or Ubuntu 24.04
- **Instance type**: `t3.micro` (free tier) or `t3.medium` for higher load
- **Key pair**: Select or create an SSH key pair
- **Storage**: Default 8 GB is sufficient

## 3. Configure Security Group

Create a security group with the following **inbound rules**:

| Type       | Protocol | Port | Source          | Description         |
|------------|----------|------|-----------------|---------------------|
| SSH        | TCP      | 22   | Your IP (`x.x.x.x/32`) | SSH access  |
| Custom TCP | TCP      | 3000 | `0.0.0.0/0`    | WebSocket server    |

**Outbound rules**: Allow all traffic (default).

## 4. Deploy to EC2

```bash
# Create the deployment directory on EC2
ssh -i your-key.pem ec2-user@<EC2-PUBLIC-IP> "mkdir -p ~/chatflow"

# Copy the binary, templates, and .env file
scp -i your-key.pem -r bin/ ec2-user@<EC2-PUBLIC-IP>:~/chatflow/
scp -i your-key.pem .env ec2-user@<EC2-PUBLIC-IP>:~/chatflow/
```

## 5. Configure the Environment

SSH into the instance and update the `.env` file:

```bash
ssh -i your-key.pem ec2-user@<EC2-PUBLIC-IP>
```

Edit `~/chatflow/.env`:

```
NAME=PRODUCTION
PORT=3000
SERVER_HOST=0.0.0.0
```

## 6. Run the Server

### Option A: Run directly

```bash
# Increase file descriptor limit for high connection counts
ulimit -n 65535

# Enable faster socket reuse between test runs
sudo sysctl -w net.ipv4.tcp_tw_reuse=1
sudo sysctl -w net.ipv4.tcp_fin_timeout=15

# Run from the bin directory so template paths resolve correctly
cd ~/chatflow/bin && ./chat-server
```

### Option B: Run as a systemd service

Create `/etc/systemd/system/chatflow.service`:

```ini
[Unit]
Description=ChatFlow WebSocket Server
After=network.target

[Service]
Type=simple
User=ec2-user
WorkingDirectory=/home/ec2-user/chatflow/bin
ExecStart=/home/ec2-user/chatflow/bin/chat-server
EnvironmentFile=/home/ec2-user/chatflow/.env
Restart=on-failure
RestartSec=5
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable chatflow
sudo systemctl start chatflow
sudo systemctl status chatflow
```

## 7. Verify

```bash
# Health check
curl http://<EC2-PUBLIC-IP>:3000/health

# Check server is listening
ss -tlnp | grep 3000
```

## 8. Run Load Tests

Update the `.env` on your **local machine** to point to the EC2 server:

```
SERVER_HOST=<EC2-PUBLIC-IP>
PORT=3000
```

Then run the load test:

```bash
make run-client2
```

## 9. Monitoring

```bash
# Live memory usage
watch -n 1 free -h

# Server process stats
top -p $(pgrep chat-server)

# View logs (systemd)
sudo journalctl -u chatflow -f

# Check active connections
ss -s

# Check TIME_WAIT sockets
ss -o state time-wait | wc -l
```

## Important Notes

- **Restart the server between load test runs** to clear residual connections and memory
- **Wait 2-3 minutes between tests** for `TIME_WAIT` sockets to clear on both client and server
- **Pool size > 1000** concurrent connections may cause message loss over the public internet 
