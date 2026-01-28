package models

import "time"

type MessageType string

const (
	MessageTypeJoin  MessageType = "JOIN"
	MessageTypeText  MessageType = "TEXT"
	MessageTypeLeave MessageType = "LEAVE"
)

func (t MessageType) String() string {
	return string(t)
}

// Message defines the response sent back from the websocket
type Message struct {
	UserId      string      `json:"userId"`
	Username    string      `json:"username"`
	Message     string      `json:"message"`
	Timestamp   time.Time   `json:"timestamp"`
	MessageType MessageType `json:"messageType"`
	// ID      uint   `gorm:"primaryKey;autoIncrement" json:"id"`
	// Content string `gorm:"type:text;not null" json:"content"`
}

func NewMessage(userName string, message string, messageType MessageType) *Message {
	return &Message{
		UserId:      "1",
		Username:    userName,
		Message:     message,
		Timestamp:   time.Now(),
		MessageType: messageType,
	}
}
