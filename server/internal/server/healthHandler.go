package internal

import (
	"encoding/json"
	"net/http"
	"time"
)

// HealthHandler handles health check requests and updates stats.
func (s *Server) HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	response := map[string]interface{}{
		"failedRequests":     s.Stats.FailedRequests.Load(),
		"successfulRequests": s.Stats.SuccessfulRequests.Load(),
		"timestamp":          time.Now().Unix(),
	}

	json.NewEncoder(w).Encode(response)
}
