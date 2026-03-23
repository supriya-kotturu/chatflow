package models

type QueueMessage struct {
	MessageID   string      `json:"messageId"`
	RoomID      string      `json:"roomId"`
	Username    string      `json:"username"`
	Message     Message     `json:"message"`
	Timestamp   string      `json:"timestamp"`
	MessageType MessageType `json:"messageType"`
	ServerID    string      `json:"serverId"`
	ClientIp    string      `json:"clientIp"`
}
