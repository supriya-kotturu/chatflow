package metrics

import (
	"encoding/json"
	"log"
	"net/http"
	"time"
)

type UserMessageCount struct {
	UserId       string `json:"user_id"`
	MessageCount int    `json:"message_count"`
}

type RoomMessageCount struct {
	RoomId       string `json:"room_id"`
	MessageCount int    `json:"message_count"`
}

type ActiveUsers struct {
	Count int `json:"count"`
}

type MessagePerMinute struct {
	Bucket time.Time `json:"bucket"`
	Count  int       `json:"count"`
}

type RoomActivity struct {
	RoomId       string    `json:"room_id"`
	LastActivity time.Time `json:"last_activity"`
}

type UserRooms struct {
	UserId string         `json:"user_id"`
	Rooms  []RoomActivity `json:"rooms"`
}

type UserRoomCount struct {
	UserId    string `json:"user_id"`
	RoomCount int    `json:"room_count"`
}

type Message struct {
	MessageID string    `json:"message_id"`
	UserID    string    `json:"user_id"`
	RoomID    string    `json:"room_id"`
	Username  string    `json:"username"`
	Content   string    `json:"content"`
	Timestamp time.Time `json:"timestamp"`
}

type Metrics struct {
	Core struct {
		RoomMessages   RoomMessageCount `json:"room_messages"`
		ActiveUsers    ActiveUsers      `json:"active_users"`
		UserRooms      UserRooms        `json:"user_rooms"`
		RoomHistory    []Message        `json:"room_history"`
		UserHistory    []Message        `json:"user_history"`
	} `json:"core"`
	Analytics struct {
		MessagesPerMinute      []MessagePerMinute `json:"messages_per_minute"`
		TopUsersByMessageCount []UserMessageCount `json:"top_users_by_message_count"`
		TopRoomsByMessageCount []RoomMessageCount `json:"top_rooms_by_message_count"`
		TopUsersByRoomCount    []UserRoomCount    `json:"top_users_by_room_count"`
	} `json:"analytics"`
}

func (s *Server) MetricsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var metrics Metrics

	count, err := s.getActiveUsersCount(r.Context())
	if err != nil {
		log.Printf("Error getting active users count: %v", err)
	}
	metrics.Core.ActiveUsers.Count = count

	roomMessages, err := s.getTopRoomsByMessageCount(r.Context(), 1)
	if err != nil {
		log.Printf("Error getting top rooms by message count: %v", err)
	} else if len(roomMessages) > 0 {
		metrics.Core.RoomMessages = roomMessages[0]
	}

	mostActiveUser, err := s.getMostActiveUser(r.Context())
	if err != nil {
		log.Printf("Error getting most active user: %v", err)
	} else {
		userRooms, err := s.getUserRooms(r.Context(), mostActiveUser.UserId)
		if err != nil {
			log.Printf("Error getting user rooms for %s: %v", mostActiveUser.UserId, err)
		}
		metrics.Core.UserRooms = userRooms

		// Q2: user message history for the most active user (last 5 messages)
		userHistory, err := s.getUserMessageHistory(r.Context(), mostActiveUser.UserId, 5)
		if err != nil {
			log.Printf("Error getting user message history for %s: %v", mostActiveUser.UserId, err)
		}
		metrics.Core.UserHistory = userHistory
	}

	// Q1: recent messages in the busiest room (last 1 hour, up to 5)
	if metrics.Core.RoomMessages.RoomId != "" {
		end := time.Now()
		start := end.Add(-1 * time.Hour)
		roomHistory, err := s.getRoomMessages(r.Context(), metrics.Core.RoomMessages.RoomId, start, end, 5)
		if err != nil {
			log.Printf("Error getting room messages for %s: %v", metrics.Core.RoomMessages.RoomId, err)
		}
		metrics.Core.RoomHistory = roomHistory
	}

	messagesPerMin, err := s.getMessagesPerMinute(r.Context(), 15)
	if err != nil {
		log.Printf("Error getting messages per minute: %v", err)
	}
	metrics.Analytics.MessagesPerMinute = messagesPerMin

	topUsersByMessageCount, err := s.getTopUsersByMessageCount(r.Context(), 5)
	if err != nil {
		log.Printf("Error getting top users by message count: %v", err)
	}
	metrics.Analytics.TopUsersByMessageCount = topUsersByMessageCount

	topRoomsByMessageCount, err := s.getTopRoomsByMessageCount(r.Context(), 5)
	if err != nil {
		log.Printf("Error getting top rooms by message count: %v", err)
	}
	metrics.Analytics.TopRoomsByMessageCount = topRoomsByMessageCount

	topUsersByRoomCount, err := s.getTopUsersByRoomCount(r.Context(), 5)
	if err != nil {
		log.Printf("Error getting top users by room count: %v", err)
	}
	metrics.Analytics.TopUsersByRoomCount = topUsersByRoomCount

	resp, err := json.Marshal(metrics)
	if err != nil {
		http.Error(w, "failed to marshal metrics", http.StatusInternalServerError)
		return
	}

	if _, err := w.Write(resp); err != nil {
		log.Printf("Error writing metrics response: %v", err)
	}
}
