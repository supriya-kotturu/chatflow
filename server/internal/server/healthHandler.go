package internal

import (
	"net/http"
)

// HealthHandler handles health check requests and updates stats.
func (s *Server) HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
}
