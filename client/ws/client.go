// Package client implements the ChatFlow load-testing client.
// It generates concurrent users that join rooms, send messages,
// and collect per-room latency and throughput statistics.
package client

import (
	"context"
	"fmt"
	"log"
	"path"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"supriyakotturu.github.com/chatflow/pkg/env"
	"supriyakotturu.github.com/chatflow/pkg/generate"
	"supriyakotturu.github.com/chatflow/pkg/models"
	"supriyakotturu.github.com/chatflow/pkg/utils"
)

// ConnElement groups a user's connection and pre-generated messages for one room.
type ConnElement struct {
	UserId   string
	RoomId   string
	Messages []*models.Message
	Conn     *WsClient
}

// Client orchestrates the load test: connection pooling, message generation,
// writing, reading, metrics collection, and stats aggregation.
type Client struct {
	Pool             *Pool
	RoomIds          []string
	UserIds          []string
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
}

// NewClient initializes a Client with a connection pool, users, rooms, and optional CSV metrics.
func NewClient(cf *ClientConfig) *Client {
	e, err := env.LoadEnv()
	fileName := "metrics.csv"

	if err != nil {
		log.Fatalf("Error loading the environment variables: %+v", err)
	}
	pool := NewWsClientPool(cf.PoolSize, e.ServerHost, e.Port)

	client := &Client{
		Pool:     pool,
		RoomIds:  generate.NewRooms(cf.RoomCount),
		UserIds:  generate.NewUsers(cf.UserCount),
		roomChan: make(chan *ConnElement, cf.MessageBuffer),
		Wg:       &sync.WaitGroup{},
		mu:       &sync.RWMutex{},
	}

	if cf.CollectMetrics {
		csvWriter, err := utils.NewCSVWriter(path.Join(cf.OutputFolder, fileName))
		if err != nil {
			log.Fatalf("Error creating CSV writer: %+v", err)
		}
		csvWriter.WriteHeader()
		client.collectMetrics = true
		client.CSVWriter = csvWriter
		client.metricChan = make(chan *models.Metric, cf.MessageBuffer)
	}

	client.MessageCount.Store(int32(cf.MessageCount))
	client.expectedMessages.Store(int32(cf.MessageCount + 2))
	client.statsChan = make(chan *models.RoomStats, cf.MessageBuffer)

	return client
}

// GenerateConnElements creates a ConnElement per room for the given user
// and sends each to the roomChan for processing.
func (c *Client) GenerateConnElements(userId string) {
	for _, roomId := range c.RoomIds {
		userConn, err := c.Pool.GetOrCreateNewWsClient(userId, roomId)
		if err != nil {
			log.Printf("User %s failed to connect to room %s: %+v", userId, roomId, err)
			continue
		}

		messages := []*models.Message{}

		joinMsg := generate.NewJoinMessage(userId, roomId)
		leaveMsg := generate.NewLeaveMessage(userId, roomId)

		messages = append(messages, joinMsg)
		for i := 0; i < int(c.MessageCount.Load()); i++ {
			messages = append(messages, generate.NewMessage(userId))
		}
		messages = append(messages, leaveMsg)

		conn := &ConnElement{
			UserId:   userId,
			RoomId:   roomId,
			Messages: messages,
			Conn:     userConn,
		}

		c.roomChan <- conn
	}
}

// GenerateMessages spawns a goroutine per user to generate ConnElements
// concurrently, then closes roomChan when all users are done.
func (c *Client) GenerateMessages() {
	defer close(c.roomChan)
	var wg sync.WaitGroup

	for _, userId := range c.UserIds {
		wg.Add(1)

		go func(userId string) {
			defer wg.Done()
			c.GenerateConnElements(userId)
		}(userId)
	}

	wg.Wait()
}

// WriteMessages consumes ConnElements from roomChan. For each element it
// starts a reader goroutine that collects latency stats, then writes all
// messages to the server and waits for the reader to finish.
func (c *Client) WriteMessages(ctx context.Context) {
	for room := range c.roomChan {
		c.Wg.Add(1)

		go func(room *ConnElement) {
			defer c.Wg.Done()
			defer c.Pool.Remove(room.UserId, room.RoomId)

			// Read messages sent from the server
			readDone := make(chan struct{})
			go func() {
				defer close(readDone)
				expected := len(room.Messages)
				received := 0
				var totalLatency int64
				latencies := make([]int64, 0, expected)
				startTime := time.Now()
				messageTypes := make(map[models.MessageType]int)

				sendStats := func() {
					n := len(latencies)
					if n == 0 {
						return
					}
					sort.Slice(latencies, func(i, j int) bool {
						return latencies[i] < latencies[j]
					})
					stats := &models.RoomStats{
						RoomId:              room.RoomId,
						UserId:              room.UserId,
						MessageCount:        received,
						MeanLatency:         totalLatency / int64(n),
						MedianLatency:       latencies[n/2],
						Percentile95Latency: latencies[int(float64(n)*0.95)],
						Percentile99Latency: latencies[int(float64(n)*0.99)],
						MinLatency:          latencies[0],
						MaxLatency:          latencies[n-1],
						ThroughPut:          float64(n) / time.Since(startTime).Seconds(),
						MessageTypes:        messageTypes,
					}
					c.statsChan <- stats
					// log.Printf("User %s in room %s avg latency: %dms (%d/%d received)",
					// 	room.UserId, room.RoomId, totalLatency/int64(n), received, expected)
				}

				for received < expected {
					select {
					case resp, ok := <-room.Conn.Send:
						if !ok {
							sendStats()
							return
						}

						received++
						sendTime, sendTimeErr := time.Parse(time.RFC3339Nano, resp.Timestamp)
						messageTypes[resp.MessageType]++

						if sendTimeErr != nil {
							continue
						}

						latency := time.Since(sendTime).Milliseconds()
						latencies = append(latencies, latency)

						// Uncomment below line to log messages
						// log.Printf("User %s in room %s echoed: %s | %d", room.UserId, room.RoomId, resp.MessageType, latency)

						if c.collectMetrics {
							c.WriteMetricToChan(resp.Message.Timestamp, resp.Message.MessageType, latency, resp.Status, room.RoomId)
						}

						totalLatency += latency

						if received >= expected {
							sendStats()
							return
						}
					case <-ctx.Done():
						log.Printf("User %s in room %s timed out: %d/%d",
							room.UserId, room.RoomId, received, expected)
						sendStats()
						return
					}
				}
			}()

			// Write messages to the server
			for _, m := range room.Messages {
				m.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)
				if err := room.Conn.Write(m); err != nil {
					c.Pool.FailedMessages.Add(1)
					log.Printf("User %s write to room %s error: %+v", room.UserId, room.RoomId, err)
				} else {
					c.Pool.SuccessfulMessages.Add(1)
				}
			}

			<-readDone
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
	close(c.metricChan)
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
		roomStatsMap[rs.RoomId] = append(roomStatsMap[rs.RoomId], rs)
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
