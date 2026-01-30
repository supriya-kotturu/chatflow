package models

import (
	"net/http"
	"time"
)

// Response represents the server response after processing a request.
type Response struct {
	Timestamp time.Time `json:"timestamp"`
	Status    int       `json:"status"`
	Message   string    `json:"message,omitempty"`
	Data      any       `json:"data,omitempty"`
}

// NewResponse creates a new Response based on the provided Message.
func NewResponse(message Message) *Response {
	err := message.Validate()
	if err != nil {
		return &Response{
			Timestamp: time.Now(),
			Status:    http.StatusUnprocessableEntity,
			Message:   "Incorrect message format",
			Data:      err,
		}
	}

	return &Response{
		Timestamp: time.Now(),
		Status:    http.StatusOK,
		Message:   "Message processed successfully",
		Data:      message,
	}
}
