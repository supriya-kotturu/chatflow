package models

type QueueMessage struct {
	MessageId   string      `json:"messageId"`
	RoomId      string      `json:"roomId"`
	Username    string      `json:"username"`
	Message     Message     `json:"message"`
	Timestamp   string      `json:"timestamp"`
	MessageType MessageType `json:"messageType"`
	ServerId    string      `json:"serverId"`
	ClientIp    string      `json:"clientIp"`
}
