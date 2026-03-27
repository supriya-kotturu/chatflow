package metrics

import (
	"context"
	"time"
)

func (s *Server) getActiveUsersCount(ctx context.Context) (int, error) {
	var count int
	err := s.db.QueryRow(ctx, `SELECT COUNT (DISTINCT user_id) FROM messages WHERE timestamp > Now() - INTERVAL '1 hour'`).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *Server) getTopRoomsByMessageCount(ctx context.Context, n int) ([]RoomMessageCount, error) {
	rooms := make([]RoomMessageCount, 0, n)
	rows, err := s.db.Query(ctx, `SELECT room_id, message_count FROM room_message_stats ORDER BY message_count DESC LIMIT $1`, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var r RoomMessageCount
		if err := rows.Scan(&r.RoomId, &r.MessageCount); err != nil {
			return nil, err
		}
		rooms = append(rooms, r)
	}

	return rooms, rows.Err()
}

func (s *Server) getTopUsersByMessageCount(ctx context.Context, n int) ([]UserMessageCount, error) {
	users := make([]UserMessageCount, 0, n)
	rows, err := s.db.Query(ctx, `SELECT user_id, message_count FROM user_message_stats ORDER BY message_count DESC LIMIT $1`, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var u UserMessageCount
		if err := rows.Scan(&u.UserId, &u.MessageCount); err != nil {
			return nil, err
		}
		users = append(users, u)
	}

	return users, rows.Err()
}

func (s *Server) getTopUsersByRoomCount(ctx context.Context, n int) ([]UserRoomCount, error) {
	users := make([]UserRoomCount, 0, n)
	rows, err := s.db.Query(ctx, `SELECT user_id, COUNT(room_id) AS room_count FROM user_rooms GROUP BY user_id ORDER BY room_count DESC LIMIT $1`, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var u UserRoomCount
		if err := rows.Scan(&u.UserId, &u.RoomCount); err != nil {
			return nil, err
		}
		users = append(users, u)
	}

	return users, rows.Err()
}

func (s *Server) getMessagesPerMinute(ctx context.Context, n int) ([]MessagePerMinute, error) {
	messageStats := make([]MessagePerMinute, 0, n)
	rows, err := s.db.Query(ctx, `SELECT bucket, message_count FROM message_stats ORDER BY bucket DESC LIMIT $1`, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var m MessagePerMinute
		if err := rows.Scan(&m.Bucket, &m.Count); err != nil {
			return nil, err
		}
		messageStats = append(messageStats, m)
	}

	return messageStats, rows.Err()
}

func (s *Server) getMostActiveUser(ctx context.Context) (UserMessageCount, error) {
	var user UserMessageCount
	err := s.db.QueryRow(ctx, `SELECT user_id, message_count FROM user_message_stats ORDER BY message_count DESC LIMIT 1`).Scan(&user.UserId, &user.MessageCount)
	if err != nil {
		return user, err
	}
	return user, nil
}

func (s *Server) getUserRooms(ctx context.Context, userId string) (UserRooms, error) {
	var userRooms UserRooms
	var history []RoomActivity
	rows, err := s.db.Query(ctx, `SELECT room_id, last_activity_timestamp FROM user_rooms WHERE user_id = $1 ORDER BY last_activity_timestamp DESC`, userId)
	if err != nil {
		return userRooms, err
	}
	defer rows.Close()

	for rows.Next() {
		var h RoomActivity
		if err := rows.Scan(&h.RoomId, &h.LastActivity); err != nil {
			return userRooms, err
		}
		history = append(history, h)
	}

	userRooms.UserId = userId
	userRooms.Rooms = history
	return userRooms, rows.Err()
}

// Q1: messages in a room within a time range, ordered by timestamp.
func (s *Server) getRoomMessages(ctx context.Context, roomId string, start, end time.Time, limit int) ([]Message, error) {
	rows, err := s.db.Query(ctx,
		`SELECT message_id, user_id, room_id, username, content, timestamp
		 FROM messages
		 WHERE room_id = $1 AND timestamp BETWEEN $2 AND $3
		 ORDER BY timestamp ASC LIMIT $4`,
		roomId, start, end, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []Message
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.MessageID, &m.UserID, &m.RoomID, &m.Username, &m.Content, &m.Timestamp); err != nil {
			return nil, err
		}
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
}

// Q2: a user's message history across all rooms, most recent first.
func (s *Server) getUserMessageHistory(ctx context.Context, userId string, limit int) ([]Message, error) {
	rows, err := s.db.Query(ctx,
		`SELECT message_id, user_id, room_id, username, content, timestamp
		 FROM messages
		 WHERE user_id = $1
		 ORDER BY timestamp DESC LIMIT $2`,
		userId, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []Message
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.MessageID, &m.UserID, &m.RoomID, &m.Username, &m.Content, &m.Timestamp); err != nil {
			return nil, err
		}
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
}
