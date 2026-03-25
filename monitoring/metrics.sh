#!/usr/bin/env bash
# ChatFlow monitoring scripts
# Usage: source these commands or run individually

CONSUMER_IP="54.218.54.179"
RABBITMQ_IP="52.40.118.132"
KEY="../keys/chatflow-key.pem"

# --- Consumer metrics API ---
metrics() {
  curl -s "http://${CONSUMER_IP}:8080/metrics" | python3 -m json.tool
}

# --- Postgres row counts ---
db_count() {
  ssh -i "$KEY" ec2-user@"${CONSUMER_IP}" \
    "sudo -u postgres psql -d chatflow -c 'SET search_path TO chatflow; SELECT COUNT(*) FROM messages;'"
}

# --- Postgres active connections ---
db_connections() {
  ssh -i "$KEY" ec2-user@"${CONSUMER_IP}" \
    "sudo -u postgres psql -d chatflow -c \"SELECT count(*), state FROM pg_stat_activity GROUP BY state;\""
}

# --- Disk usage on consumer ---
disk_usage() {
  ssh -i "$KEY" ec2-user@"${CONSUMER_IP}" "df -h /"
}

# --- Consumer logs (follow) ---
consumer_logs() {
  ssh -i "$KEY" ec2-user@"${CONSUMER_IP}" "sudo journalctl -u consumer-v3 -f --no-pager"
}

# --- RabbitMQ queue depth ---
queue_depth() {
  ssh -i "$KEY" ec2-user@"${RABBITMQ_IP}" \
    "sudo rabbitmqctl list_queues name messages consumers"
}

# --- Delete stale queues (0 consumers) ---
clean_queues() {
  ssh -i "$KEY" ec2-user@"${RABBITMQ_IP}" \
    "sudo rabbitmqctl list_queues name consumers | awk '\$2 == \"0\" {print \$1}' | xargs -r -I{} sudo rabbitmqctl delete_queue {}"
}

# --- Truncate DB between runs ---
truncate_db() {
  ssh -i "$KEY" ec2-user@"${CONSUMER_IP}" \
    "sudo -u postgres psql -d chatflow -c 'SET search_path TO chatflow; TRUNCATE messages, user_rooms, message_stats, user_message_stats, room_message_stats;'"
}

# --- Postgres DB size ---
db_size() {
  ssh -i "$KEY" ec2-user@"${CONSUMER_IP}" \
    "sudo -u postgres psql -d chatflow -c \"SELECT pg_size_pretty(pg_database_size('chatflow'));\""
}

# --- Postgres table sizes ---
table_sizes() {
  ssh -i "$KEY" ec2-user@"${CONSUMER_IP}" \
    "sudo -u postgres psql -d chatflow -c \"
      SELECT schemaname, tablename,
             pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
      FROM pg_tables
      WHERE schemaname = 'chatflow'
      ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;\""
}
