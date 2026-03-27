package client

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	ws "supriyakotturu.github.com/chatflow/client/ws"
)

type userMessageCount struct {
	UserId       string `json:"user_id"`
	MessageCount int    `json:"message_count"`
}

type roomMessageCount struct {
	RoomId       string `json:"room_id"`
	MessageCount int    `json:"message_count"`
}

type activeUsers struct {
	Count int `json:"count"`
}

type messagePerMinute struct {
	Bucket string `json:"bucket"`
	Count  int    `json:"count"`
}

type roomActivity struct {
	RoomId       string `json:"room_id"`
	LastActivity string `json:"last_activity"`
}

type userRooms struct {
	UserId string         `json:"user_id"`
	Rooms  []roomActivity `json:"rooms"`
}

type userRoomCount struct {
	UserId    string `json:"user_id"`
	RoomCount int    `json:"room_count"`
}

type message struct {
	MessageID string `json:"message_id"`
	UserID    string `json:"user_id"`
	RoomID    string `json:"room_id"`
	Username  string `json:"username"`
	Content   string `json:"content"`
	Timestamp string `json:"timestamp"`
}

type metricsResponse struct {
	Core struct {
		RoomMessages roomMessageCount `json:"room_messages"`
		ActiveUsers  activeUsers      `json:"active_users"`
		UserRooms    userRooms        `json:"user_rooms"`
		RoomHistory  []message        `json:"room_history"`
		UserHistory  []message        `json:"user_history"`
	} `json:"core"`
	Analytics struct {
		MessagesPerMinute      []messagePerMinute `json:"messages_per_minute"`
		TopUsersByMessageCount []userMessageCount `json:"top_users_by_message_count"`
		TopRoomsByMessageCount []roomMessageCount `json:"top_rooms_by_message_count"`
		TopUsersByRoomCount    []userRoomCount    `json:"top_users_by_room_count"`
	} `json:"analytics"`
}

// FetchAndPrintMetrics calls the consumer metrics API and prints the results.
func FetchAndPrintMetrics(c *ws.Client) {
	url := fmt.Sprintf("http://%s:%d/metrics", c.ConsumerHost, c.MetricsPort)

	httpClient := &http.Client{Timeout: 10 * time.Second}
	resp, err := httpClient.Get(url)
	if err != nil {
		fmt.Printf("\n[metrics] failed to reach %s: %v\n", url, err)
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("\n[metrics] failed to read response: %v\n", err)
		return
	}

	var m metricsResponse
	if err := json.Unmarshal(body, &m); err != nil {
		fmt.Printf("\n[metrics] failed to parse response: %v\n", err)
		return
	}

	fmt.Println("\n========== Consumer Metrics ==========")

	fmt.Println("\n--- Core ---")
	fmt.Printf("  Active users (last 1h): %d\n", m.Core.ActiveUsers.Count)
	fmt.Printf("  Busiest room:           %s (%d messages)\n", m.Core.RoomMessages.RoomId, m.Core.RoomMessages.MessageCount)
	fmt.Printf("  Most active user:       %s (%d rooms)\n", m.Core.UserRooms.UserId, len(m.Core.UserRooms.Rooms))
	for _, r := range m.Core.UserRooms.Rooms {
		fmt.Printf("    room %-20s last active: %s\n", r.RoomId, r.LastActivity)
	}

	fmt.Printf("\n--- Q1: Recent messages in room %s (last 1h, up to 5) ---\n", m.Core.RoomMessages.RoomId)
	for _, msg := range m.Core.RoomHistory {
		fmt.Printf("  [%s] user=%s: %s\n", msg.Timestamp, msg.Username, msg.Content)
	}

	fmt.Printf("\n--- Q2: Message history for user %s (last 5) ---\n", m.Core.UserRooms.UserId)
	for _, msg := range m.Core.UserHistory {
		fmt.Printf("  [%s] room=%s: %s\n", msg.Timestamp, msg.RoomID, msg.Content)
	}

	fmt.Println("\n--- Messages/min (last 15 buckets) ---")
	for _, mp := range m.Analytics.MessagesPerMinute {
		fmt.Printf("  %s  %d msg\n", mp.Bucket, mp.Count)
	}

	fmt.Println("\n--- Top 5 users by message count ---")
	for i, u := range m.Analytics.TopUsersByMessageCount {
		fmt.Printf("  %d. %s — %d messages\n", i+1, u.UserId, u.MessageCount)
	}

	fmt.Println("\n--- Top 5 rooms by message count ---")
	for i, r := range m.Analytics.TopRoomsByMessageCount {
		fmt.Printf("  %d. %s — %d messages\n", i+1, r.RoomId, r.MessageCount)
	}

	fmt.Println("\n--- Top 5 users by room count ---")
	for i, u := range m.Analytics.TopUsersByRoomCount {
		fmt.Printf("  %d. %s — %d rooms\n", i+1, u.UserId, u.RoomCount)
	}

	fmt.Println("======================================")
}
