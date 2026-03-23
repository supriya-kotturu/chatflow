CREATE SCHEMA IF NOT EXISTS chatflow;

SET search_path TO chatflow, public;

CREATE TABLE IF NOT EXISTS messages (
    message_id UUID PRIMARY KEY,
    user_id TEXT NOT NULL,
    room_id TEXT NOT NULL,
    server_id TEXT NOT NULL,
    username TEXT NOT NULL,
    content TEXT NOT NULL,
    message_type TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS user_rooms (
    user_id TEXT NOT NULL,
    room_id TEXT NOT NULL,
    last_activity TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (user_id, room_id)
);

CREATE TABLE IF NOT EXISTS message_stats(
    bucket TIMESTAMPTZ  PRIMARY KEY,
    message_count BIGINT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS user_stats(
    user_id TEXT PRIMARY KEY,
    message_count BIGINT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS room_stats(
    room_id TEXT PRIMARY KEY,
    message_count BIGINT DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_messages_room_id_timestamp ON messages(room_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_messages_user_id ON messages(user_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp);