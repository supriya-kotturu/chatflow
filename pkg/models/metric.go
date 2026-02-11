package models

import "strconv"

type Metric struct {
	Timestamp   string
	MessageType MessageType
	Latency     int64
	StatusCode  int
	RoomId      string
}

func NewMetric(timestamp string, messageType MessageType, latency int64, statusCode int, roomId string) *Metric {
	return &Metric{
		Timestamp:   timestamp,
		MessageType: messageType,
		Latency:     latency,
		StatusCode:  statusCode,
		RoomId:      roomId,
	}
}

func (m *Metric) String() []string {
	return []string{
		m.Timestamp,
		string(m.MessageType),
		strconv.Itoa(int(m.Latency)),
		strconv.Itoa(m.StatusCode),
		m.RoomId,
	}
}
