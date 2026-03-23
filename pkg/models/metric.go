package models

import "strconv"

// Metric represents a single recorded data point for CSV export.
// Latency is in milliseconds.
type Metric struct {
	Timestamp   string
	MessageType MessageType
	Latency     int64
	StatusCode  int
	RoomID      string
}

// NewMetric creates a Metric with the given fields.
func NewMetric(timestamp string, messageType MessageType, latency int64, statusCode int, roomId string) *Metric {
	return &Metric{
		Timestamp:   timestamp,
		MessageType: messageType,
		Latency:     latency,
		StatusCode:  statusCode,
		RoomID:      roomId,
	}
}

// String returns the metric as a CSV-ready string slice.
func (m *Metric) String() []string {
	return []string{
		m.Timestamp,
		string(m.MessageType),
		strconv.Itoa(int(m.Latency)),
		strconv.Itoa(m.StatusCode),
		m.RoomID,
	}
}
