package models

// RoomStats holds aggregated latency and throughput statistics for one
// user's session in a room. All latency values are in milliseconds.
type RoomStats struct {
	RoomID              string
	UserID              string
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
