// Package ws implements the ChatFlow load-testing client.
// It generates concurrent users that join rooms, send messages,
// and collect per-room latency and throughput statistics.
package ws

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"path"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"supriyakotturu.github.com/chatflow/pkg/env"
	"supriyakotturu.github.com/chatflow/pkg/generate"
	"supriyakotturu.github.com/chatflow/pkg/models"
	"supriyakotturu.github.com/chatflow/pkg/utils"
)

// ConnElement groups a user's connection and pre-generated sub-sessions for one room.
// Each sub-session is a slice of messages starting with JOIN and ending with LEAVE.
type ConnElement struct {
	UserID      string
	RoomID      string
	SubSessions [][]*models.Message
	Conn        *WsClient
}

// Client orchestrates the load test: connection pooling, message generation,
// writing, reading, metrics collection, and stats aggregation.
type Client struct {
	Pool             *Pool
	RoomIDs          []string
	UserIDs          []string
	ConsumerHost     string
	MetricsPort      int
	MessageCount     atomic.Int32
	expectedMessages atomic.Int32
	receivedMessages atomic.Int32
	roomChan         chan *ConnElement
	Wg               *sync.WaitGroup
	mu               *sync.RWMutex
	collectMetrics   bool
	CSVWriter        *utils.CSVWriter
	metricChan       chan *models.Metric
	statsChan        chan *models.RoomStats
}

// ClientConfig holds the load test parameters.
type ClientConfig struct {
	PoolSize       int
	UserCount      int
	MessageCount   int
	RoomCount      int
	MessageBuffer  int
	CollectMetrics bool
	LogMessages    bool
	OutputFolder   string
	OutputFile     string // if set, used directly as the CSV path instead of OutputFolder/metrics.csv
}

// NewClient initializes a Client with a connection pool, users, rooms, and optional CSV metrics.
func NewClient(cf *ClientConfig) *Client {
	e, err := env.LoadServerEnv()
	if err != nil {
		log.Fatalf("Error loading the environment variables: %+v", err)
	}
	pool := NewWsClientPool(cf.PoolSize, e.ServerHost, e.Port, cf.MessageBuffer)

	client := &Client{
		Pool:        pool,
		RoomIDs:     generate.NewRooms(cf.RoomCount),
		UserIDs:     generate.NewUsers(cf.UserCount),
		ConsumerHost: e.ConsumerHost,
		MetricsPort: e.MetricsPort,
		roomChan:    make(chan *ConnElement, cf.MessageBuffer),
		Wg:          &sync.WaitGroup{},
		mu:          &sync.RWMutex{},
	}

	if cf.CollectMetrics {
		csvPath := cf.OutputFile
		if csvPath == "" {
			csvPath = path.Join(cf.OutputFolder, "metrics.csv")
		}
		csvWriter, err := utils.NewCSVWriter(csvPath)
		if err != nil {
			log.Fatalf("Error creating CSV writer: %+v", err)
		}
		csvWriter.WriteHeader()
		client.collectMetrics = true
		client.CSVWriter = csvWriter
		client.metricChan = make(chan *models.Metric, cf.MessageBuffer)
	}

	client.MessageCount.Store(int32(cf.MessageCount))
	client.expectedMessages.Store(int32(cf.UserCount * (cf.MessageCount + 2)))
	client.statsChan = make(chan *models.RoomStats, cf.MessageBuffer)

	return client
}

// GenerateConnElements creates a ConnElement per room for the given user.
// Messages follow a 5-90-5 JOIN/TEXT/LEAVE distribution split into sub-sessions,
// each bounded by JOIN…LEAVE so the server connection lifecycle is respected.
func (c *Client) GenerateConnElements(userId string) {
	for _, roomId := range c.RoomIDs {
		userConn, err := c.Pool.GetOrCreateNewWsClient(userId, roomId)
		if err != nil {
			log.Printf("User %s failed to connect to room %s: %+v", userId, roomId, err)
			continue
		}

		flat := generate.NewSessionMessages(userId, int(c.MessageCount.Load()))
		subSessions := splitIntoSubSessions(flat)

		conn := &ConnElement{
			UserID:      userId,
			RoomID:      roomId,
			SubSessions: subSessions,
			Conn:        userConn,
		}

		c.roomChan <- conn
	}
}

// splitIntoSubSessions partitions a flat message slice into sub-slices, each
// ending at a LEAVE message. This mirrors the server's connection lifecycle:
// LEAVE closes the handler, so each sub-session needs its own connection.
func splitIntoSubSessions(msgs []*models.Message) [][]*models.Message {
	var sessions [][]*models.Message
	var current []*models.Message
	for _, m := range msgs {
		current = append(current, m)
		if m.MessageType == models.MessageTypeLeave {
			sessions = append(sessions, current)
			current = nil
		}
	}
	if len(current) > 0 {
		sessions = append(sessions, current)
	}
	return sessions
}

// GenerateMessages spawns a goroutine per user to generate ConnElements
// concurrently, then closes roomChan when all users are done.
func (c *Client) GenerateMessages() {
	defer close(c.roomChan)
	var wg sync.WaitGroup

	for _, userId := range c.UserIDs {
		wg.Add(1)

		go func(userId string) {
			defer wg.Done()
			c.GenerateConnElements(userId)
		}(userId)
	}

	wg.Wait()
}

// sessionResult holds stats collected by one per-session reader goroutine.
type sessionResult struct {
	latencies []int64
	total     int64
	received  int
	msgTypes  map[models.MessageType]int
	usersSeen map[string]struct{}
}

// WriteMessages consumes ConnElements from roomChan. For each element it
// iterates sub-sessions: spawns a per-session reader, writes all messages,
// waits for the reader to finish (connection closes after LEAVE), then
// reconnects for the next sub-session. Stats are aggregated across all
// sub-sessions and sent once to statsChan.
func (c *Client) WriteMessages(ctx context.Context) {
	for room := range c.roomChan {
		c.Wg.Add(1)

		go func(room *ConnElement) {
			defer c.Wg.Done()
			defer c.Pool.Remove(room.UserID, room.RoomID)

			startTime := time.Now()
			var allLatencies []int64
			var totalLatency int64
			allReceived := 0
			messageTypes := make(map[models.MessageType]int)
			usersSeenSending := make(map[string]struct{})

		sessionLoop:
			for i, session := range room.SubSessions {
				if i > 0 {
					newConn, err := c.Pool.Reconnect(room.UserID, room.RoomID)
					if err != nil {
						log.Printf("User %s reconnect failed for room %s: %v", room.UserID, room.RoomID, err)
						break sessionLoop
					}
					room.Conn = newConn
				}

				resultCh := make(chan sessionResult, 1)
				conn := room.Conn

				// Per-session reader: drains Send until the connection closes
				// (server closes after LEAVE). Writer owns termination via
				// the connection lifecycle — no LEAVE-echo detection needed.
				go func(wsConn *WsClient) {
					res := sessionResult{
						msgTypes:  make(map[models.MessageType]int),
						usersSeen: make(map[string]struct{}),
					}
					for resp := range wsConn.Send {
						res.received++
						res.msgTypes[resp.MessageType]++
						if resp.MessageType == models.MessageTypeJoin {
							res.usersSeen[resp.UserID] = struct{}{}
						}
						sendTime, err := time.Parse(time.RFC3339Nano, resp.Timestamp)
						if err == nil {
							latency := time.Since(sendTime).Milliseconds()
							res.latencies = append(res.latencies, latency)
							res.total += latency
							if c.collectMetrics {
								c.WriteMetricToChan(resp.Message.Timestamp, resp.Message.MessageType, latency, resp.Status, room.RoomID)
							}
						}
					}
					resultCh <- res
				}(conn)

				maxRetries := 5
				writeOk := true

			writeLoop:
				for _, m := range session {
					backoff := 2 * time.Second

					for attempt := 0; attempt < maxRetries; attempt++ {
						m.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)
						err := room.Conn.Write(m)

						if err == nil {
							c.Pool.SuccessfulMessages.Add(1)
							break
						}

						var netErr *net.OpError
						if errors.As(err, &netErr) || websocket.IsCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure, websocket.CloseNormalClosure) {
							c.Pool.FailedConnections.Add(1)
							log.Printf("User %s connection to room %s lost: %+v", room.UserID, room.RoomID, err)
							writeOk = false
							break writeLoop
						}

						fmt.Printf("attempt %d failed message write: %v, retrying in %v...\n", attempt+1, err, backoff)

						select {
						case <-time.After(backoff):
						case <-ctx.Done():
							writeOk = false
							break writeLoop
						}

						backoff *= 2
						if attempt == maxRetries-1 {
							c.Pool.FailedMessages.Add(1)
							log.Printf("User %s write to room %s error: %+v", room.UserID, room.RoomID, err)
						}
					}
				}

				// Always wait for the reader — even on write failure the connection
				// will close and drain, preventing a goroutine leak.
				res := <-resultCh

				allLatencies = append(allLatencies, res.latencies...)
				totalLatency += res.total
				allReceived += res.received
				for mt, count := range res.msgTypes {
					messageTypes[mt] += count
				}
				for uid := range res.usersSeen {
					usersSeenSending[uid] = struct{}{}
				}

				if !writeOk {
					break sessionLoop
				}
			}

			n := len(allLatencies)
			if n == 0 {
				return
			}
			sort.Slice(allLatencies, func(i, j int) bool { return allLatencies[i] < allLatencies[j] })
			stats := &models.RoomStats{
				RoomID:              room.RoomID,
				UserID:              room.UserID,
				MessageCount:        allReceived,
				MeanLatency:         totalLatency / int64(n),
				MedianLatency:       allLatencies[n/2],
				Percentile95Latency: allLatencies[int(float64(n)*0.95)],
				Percentile99Latency: allLatencies[int(float64(n)*0.99)],
				MinLatency:          allLatencies[0],
				MaxLatency:          allLatencies[n-1],
				ThroughPut:          float64(n) / time.Since(startTime).Seconds(),
				MessageTypes:        messageTypes,
			}
			c.statsChan <- stats
			log.Printf("DONE: User %s in room %s: received %d across %d sessions",
				room.UserID, room.RoomID, allReceived, len(room.SubSessions))
		}(room)
	}
}

// WriteMetricToChan sends a metric to the metrics channel for CSV recording.
func (c *Client) WriteMetricToChan(timestamp string, messageType models.MessageType, latency int64, statusCode int, roomId string) {
	metric := models.NewMetric(timestamp, messageType, latency, statusCode, roomId)
	c.metricChan <- metric
}

// CollectMetrics drains metricChan and writes each metric to the CSV file.
func (c *Client) CollectMetrics(ctx context.Context) {
	for {
		select {
		case metric, ok := <-c.metricChan:
			if !ok {
				return
			}
			c.CSVWriter.Write(*metric)
		case <-ctx.Done():
			return
		}
	}
}

// CloseChannels closes metricChan and statsChan, unblocking any range loops.
func (c *Client) CloseChannels() {
	if c.metricChan != nil {
		close(c.metricChan)
	}
	close(c.statsChan)
}

// GetPerformanceMetricsSummary prints the summary of performance metrics
func (c *Client) GetPerformanceMetricsSummary(wallTime time.Duration) {
	successful := c.Pool.SuccessfulMessages.Load()
	failed := c.Pool.FailedMessages.Load()
	totalConns := c.Pool.TotalConnections.Load()
	reconnections := c.Pool.Reconnections.Load()
	failedConns := c.Pool.FailedConnections.Load()

	fmt.Println("\n=== Performance Metrics ===")
	fmt.Printf("Total runtime (wall time):  %.1fs\n", wallTime.Seconds())
	fmt.Printf("Successful messages sent:   %d\n", successful)
	fmt.Printf("Failed messages:            %d\n", failed)
	fmt.Printf("Overall throughput:         %.1f msg/s\n", float64(successful)/wallTime.Seconds())
	fmt.Printf("Total connections:          %d\n", totalConns)
	fmt.Printf("Reconnections:              %d\n", reconnections)
	fmt.Printf("Failed connections:         %d\n", failedConns)
}

// GetOverAllStats drains statsChan and prints aggregate latency percentiles,
// throughput, and per-room breakdowns with message type distribution.
func (c *Client) GetOverAllStats() {
	roomStatsMap := make(map[string][]*models.RoomStats)
	allThroughputs := []float64{}
	allLatencies := []int64{}

	for rs := range c.statsChan {
		roomStatsMap[rs.RoomID] = append(roomStatsMap[rs.RoomID], rs)
		allLatencies = append(allLatencies, rs.MeanLatency)
		allThroughputs = append(allThroughputs, rs.ThroughPut)
	}

	n := len(allLatencies)
	totalLatency := int64(0)

	if n == 0 {
		fmt.Println("No stats collected")
		return
	}

	sort.Slice(allLatencies, func(i, j int) bool {
		return allLatencies[i] < allLatencies[j]
	})

	sort.Slice(allThroughputs, func(i, j int) bool {
		return allThroughputs[i] < allThroughputs[j]
	})

	for _, l := range allLatencies {
		totalLatency += l
	}

	meanLatency := totalLatency / int64(n)
	medianLatency := allLatencies[n/2]
	percentile95Latency := allLatencies[int(float64(n)*0.95)]
	percentile99Latency := allLatencies[int(float64(n)*0.99)]
	minLatency := allLatencies[0]
	maxLatency := allLatencies[n-1]
	medianThroughput := allThroughputs[n/2]

	fmt.Printf("Mean Latency across all rooms: %dms\n", meanLatency)
	fmt.Printf("Median Latency across all rooms: %dms\n", medianLatency)
	fmt.Printf("95th Percentile Latency across all rooms: %dms\n", percentile95Latency)
	fmt.Printf("99th Percentile Latency across all rooms: %dms\n", percentile99Latency)
	fmt.Printf("Min Latency across all rooms: %dms\n", minLatency)
	fmt.Printf("Max Latency across all rooms: %dms\n", maxLatency)
	fmt.Printf("Median Throughput across all rooms: %.2f msg/s\n", medianThroughput)

	fmt.Println("Throughput across rooms: ")

	for roomId, stats := range roomStatsMap {
		var roomThroughput float64
		var roomLatencies []int64
		totalMsgTypes := make(map[models.MessageType]int)

		for _, rs := range stats {
			roomThroughput += rs.ThroughPut
			roomLatencies = append(roomLatencies, rs.MeanLatency)

			for mt, count := range rs.MessageTypes {
				totalMsgTypes[mt] += count
			}
		}
		sort.Slice(roomLatencies, func(i, j int) bool { return roomLatencies[i] < roomLatencies[j] })
		rn := len(roomLatencies)
		var totalRoomLatency int64
		for _, l := range roomLatencies {
			totalRoomLatency += l
		}
		fmt.Printf("\nRoom %s | users: %d | throughput: %.1f msg/s | mean latency: %dms | median latency: %dms\n",
			roomId, rn, roomThroughput, totalRoomLatency/int64(rn), roomLatencies[rn/2])

		fmt.Printf("  Message Types: ")
		for mt, count := range totalMsgTypes {
			fmt.Printf("%s: %d  ", mt, count)
		}
		fmt.Println()
	}
}
