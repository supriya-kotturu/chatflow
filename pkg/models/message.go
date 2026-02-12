// Package models defines data structures for the chat application.
package models

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// MessageType represents the type of message being sent.
type MessageType string

const (
	minUserId         = 1
	maxUserId         = 100000
	minUsernameLength = 3
	maxUsernameLength = 20
	minMessageLength  = 1
	maxMessageLength  = 500
)

// Message type constants.
const (
	// MessageTypeJoin indicates a user joining the chat.
	MessageTypeJoin MessageType = "JOIN"
	// MessageTypeText indicates a regular text message.
	MessageTypeText MessageType = "TEXT"
	// MessageTypeLeave indicates a user leaving the chat.
	MessageTypeLeave MessageType = "LEAVE"
)

// String returns the string representation of MessageType.
func (t MessageType) String() string {
	return string(t)
}

// Message represents a chat message with all required fields.
type Message struct {
	UserId      string      `json:"userId"`
	Username    string      `json:"username"`
	Message     string      `json:"message"`
	Timestamp   string      `json:"timestamp"`
	MessageType MessageType `json:"messageType"`
}

// NewMessage creates a new message with the given parameters.
// It automatically sets the current timestamp.
func NewMessage(userId string, username string, message string, messageType MessageType) *Message {
	return &Message{
		UserId:      userId,
		Username:    username,
		Message:     message,
		MessageType: messageType,
	}
}

// validateUserId validates the user ID field.
func (m *Message) validateUserId(errorMap map[string]string) {
	if id, err := strconv.Atoi(m.UserId); err != nil || id < minUserId || id > maxUserId {
		if err != nil {
			errorMap["UserId"] = "UserId must be a valid integer"
		} else if id < minUserId {
			errorMap["UserId"] = fmt.Sprintf("UserId must be greater than %d", minUserId)
		} else if id > maxUserId {
			errorMap["UserId"] = fmt.Sprintf("UserId must be less than %d", maxUserId)
		}
	}
}

// validateUsername validates the username field.
func (m *Message) validateUsername(errorMap map[string]string) {
	if len(m.Username) < minUsernameLength || len(m.Username) > maxUsernameLength {
		if len(m.Username) < minUsernameLength {
			errorMap["Username"] = fmt.Sprintf("Username must be at least %d characters long", minUsernameLength)
		} else if len(m.Username) > maxUsernameLength {
			errorMap["Username"] = fmt.Sprintf("Username must be less than %d characters long", maxUsernameLength)
		}
	}
}

// validateMessage validates the message content field.
func (m *Message) validateMessage(errorMap map[string]string) {
	if len(m.Message) < minMessageLength || len(m.Message) > maxMessageLength {
		if len(m.Message) < minMessageLength {
			errorMap["Message"] = "Message cannot be empty"
		} else if len(m.Message) > maxMessageLength {
			errorMap["Message"] = fmt.Sprintf("Message must be less than %d characters long", maxMessageLength)
		}
	}
}

// validateTimestamp validates the timestamp field.
func (m *Message) validateTimestamp(errorMap map[string]string) {
	if m.Timestamp == "" {
		errorMap["Timestamp"] = "Timestamp cannot be empty"
	}
}

// validateMessageType validates the message type field.
func (m *Message) validateMessageType(errorMap map[string]string) {
	if m.MessageType != MessageTypeJoin && m.MessageType != MessageTypeText && m.MessageType != MessageTypeLeave {
		errorMap["MessageType"] = "Invalid MessageType"
	}
}

// ValidationError represents multiple validation errors.
type ValidationError struct {
	Errors map[string]string
}

// Error implements the error interface.
func (ve ValidationError) Error() string {
	var msgs []string
	for field, msg := range ve.Errors {
		msgs = append(msgs, fmt.Sprintf("%s: %s", field, msg))
	}
	return strings.Join(msgs, "; ")
}

// Validate performs comprehensive validation on all message fields.
// It returns a ValidationError containing all validation failures, or nil if valid.
func (m *Message) Validate() error {
	if m == nil {
		return errors.New("message cannot be nil")
	}

	errorMap := make(map[string]string)

	m.validateUserId(errorMap)
	m.validateUsername(errorMap)
	m.validateMessageType(errorMap)

	if m.MessageType == MessageTypeText {
		m.validateMessage(errorMap)
	}

	if len(errorMap) > 0 {
		return ValidationError{Errors: errorMap}
	}
	return nil
}
