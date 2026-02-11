package models

type RoomStats struct {
	RoomId              string
	UserId              string
	MessageCount        int
	MeanLatency         int64
	MedianLatency       int64
	Percentile95Latency int64
	Percentile99Latency int64
	MinLatency          int64
	MaxLatency          int64
	ThroughPut          float64
	MessageTypes        map[MessageType]int
}
