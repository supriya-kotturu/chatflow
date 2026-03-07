package internal

import (
	"encoding/json"
	"net/http"
)

// roomMetrics is the JSON shape for a single room in the /metrics response.
type roomMetrics struct {
	Users            []string `json:"users"`
	DroppedBroadcast int64    `json:"dropped_broadcast"`
	DroppedSend      int64    `json:"dropped_send"`
}

// metricsResponse is the top-level /metrics JSON payload.
type metricsResponse struct {
	Rooms             map[string]roomMetrics `json:"rooms"`
	UserSessions      map[string][]string    `json:"user_sessions"`
	TotalConnections  int                    `json:"total_connections"`
	SuccessfulMsgs    int64                  `json:"successful_msgs"`
	FailedMsgs        int64                  `json:"failed_msgs"`
	RabbitDropped     int64                  `json:"rabbit_dropped,omitempty"`
	CircuitState      string                 `json:"circuit_state,omitempty"`
}

// MetricsHandler returns a JSON snapshot of active rooms, participants,
// drop counters, and user-session mappings.
func (s *Server) MetricsHandler(w http.ResponseWriter, r *http.Request) {
	s.Mu.RLock()
	rooms := make(map[string]roomMetrics, len(s.Rooms))
	totalConns := 0
	for id, room := range s.Rooms {
		room.Mu.RLock()
		users := make([]string, 0, len(room.Users))
		for uid := range room.Users {
			users = append(users, uid)
		}
		room.Mu.RUnlock()
		totalConns += len(users)
		rooms[id] = roomMetrics{
			Users:            users,
			DroppedBroadcast: room.DroppedBroadcast.Load(),
			DroppedSend:      room.DroppedSend.Load(),
		}
	}
	s.Mu.RUnlock()

	s.UserRoomMu.RLock()
	sessions := make(map[string][]string, len(s.UserRooms))
	for uid, roomList := range s.UserRooms {
		cp := make([]string, len(roomList))
		copy(cp, roomList)
		sessions[uid] = cp
	}
	s.UserRoomMu.RUnlock()

	resp := metricsResponse{
		Rooms:            rooms,
		UserSessions:     sessions,
		TotalConnections: totalConns,
		SuccessfulMsgs:   s.Stats.SuccessfulRequests.Load(),
		FailedMsgs:       s.Stats.FailedRequests.Load(),
	}

	if s.Rabbit != nil {
		resp.RabbitDropped = s.Rabbit.DroppedMessages()
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
