package client

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"supriyakotturu.github.com/chatflow/pkg/env"
	"supriyakotturu.github.com/chatflow/pkg/generate"
	"supriyakotturu.github.com/chatflow/pkg/models"
	"supriyakotturu.github.com/chatflow/pkg/utils"
)

type ConnElement struct {
	UserId   string
	RoomId   string
	Messages []*models.Message
	Conn     *WsClient
}

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

type ClientConfig struct {
	PoolSize       int
	UserCount      int
	MessageCount   int
	RoomCount      int
	MessageBuffer  int
	CollectMetrics bool
	LogMessages    bool
}

func NewClient(cf *ClientConfig) *Client {
	e, err := env.LoadEnv()
	if err != nil {
		log.Fatalf("Error loading the environment variables: %+v", err)
	}
	pool := NewWsClientPool(cf.PoolSize, e.Port)

	client := &Client{
		Pool:     pool,
		RoomIds:  generate.NewRooms(cf.RoomCount),
		UserIds:  generate.NewUsers(cf.UserCount),
		roomChan: make(chan *ConnElement, cf.MessageBuffer),
		Wg:       &sync.WaitGroup{},
		mu:       &sync.RWMutex{},
	}

	if cf.CollectMetrics {
		csvWriter, err := utils.NewCSVWriter("results/metrics.csv")
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

func (c *Client) WriteMessages(ctx context.Context) {
	for room := range c.roomChan {
		c.Wg.Add(1)

		go func(room *ConnElement) {
			defer c.Wg.Done()
			defer c.Pool.Remove(room.UserId, room.RoomId)

			// Read messages sent from the server
			done := make(chan struct{})
			go func() {
				defer close(done)
				expected := len(room.Messages)
				received := 0
				var totalLatency int64
				latencies := make([]int64, 0, expected)
				startTime := time.Now()
				messageTypes := make(map[models.MessageType]int)

				for received < expected {
					select {
					case resp, ok := <-room.Conn.Send:
						if !ok {
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
							sort.Slice(latencies, func(i, j int) bool {
								return latencies[i] < latencies[j]
							})
							n := len(latencies)
							stats := &models.RoomStats{
								RoomId:              room.RoomId,
								UserId:              room.UserId,
								MessageCount:        expected,
								MeanLatency:         (totalLatency) / int64(n),
								MedianLatency:       (latencies[n/2]),
								Percentile95Latency: (latencies[int(float64(n)*0.95)]),
								Percentile99Latency: (latencies[int(float64(n)*0.99)]),
								MinLatency:          latencies[0],
								MaxLatency:          latencies[n-1],
								ThroughPut:          float64(n) / time.Since(startTime).Seconds(),
								MessageTypes:        messageTypes,
							}
							c.statsChan <- stats

							log.Printf("User %s in room %s avg latency: %dms (%d/%d received)",
								room.UserId, room.RoomId, totalLatency/int64(n), received, expected)
							return
						}
					case <-ctx.Done():
						log.Printf("User %s in room %s timed out: %d/%d",
							room.UserId, room.RoomId, received, expected)
						return
					}
				}
			}()

			// Write messages to the server
			for _, m := range room.Messages {
				m.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)
				if err := room.Conn.Write(m); err != nil {
					log.Printf("User %s write to room %s error: %+v", room.UserId, room.RoomId, err)
				}
			}

			<-done
		}(room)
	}
}

// Writes metrics to the Metrics Chan
func (c *Client) WriteMetricToChan(timestamp string, messageType models.MessageType, latency int64, statusCode int, roomId string) {
	metric := models.NewMetric(timestamp, messageType, latency, statusCode, roomId)
	c.metricChan <- metric
}

// Collects the metrics in the CSV file (records/metrics.csv)
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

func (c *Client) CloseChannels() {
	close(c.metricChan)
	close(c.statsChan)
}

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
		fmt.Printf("Room %s | users: %d | throughput: %.1f msg/s | mean latency: %dms | median latency: %dms\n",
			roomId, rn, roomThroughput, totalRoomLatency/int64(rn), roomLatencies[rn/2])

		fmt.Println("Message Type Distribution: ")
		for mt, count := range totalMsgTypes {
			fmt.Printf("  %s: %d\n", mt, count)
		}
	}
}
