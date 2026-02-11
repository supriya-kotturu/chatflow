package models

import (
	"net/http"
	"time"
)

// Response represents the server response after processing a request.
type Response struct {
	Message
	ServerTimestamp int64  `json:"serverTimestamp"`
	Status          int    `json:"status"`
	Error           string `json:"error,omitempty"`
	Data            any    `json:"data,omitempty"`
}

// NewResponse creates a new Response based on the provided Message.
func NewResponse(message Message) *Response {
	err := message.Validate()
	if err != nil {
		return &Response{
			ServerTimestamp: time.Now().UnixMilli(),
			Status:          http.StatusUnprocessableEntity,
			Error:           "Incorrect message format",
			Data:            err,
		}
	}

	return &Response{
		ServerTimestamp: time.Now().UnixMilli(),
		Status:          http.StatusOK,
		Message:         message,
		Data:            message,
	}
}
