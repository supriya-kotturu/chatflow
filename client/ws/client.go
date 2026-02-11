package client

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"supriyakotturu.github.com/chatflow/pkg/env"
	"supriyakotturu.github.com/chatflow/pkg/generate"
	"supriyakotturu.github.com/chatflow/pkg/models"
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
}

type ClientConfig struct {
	PoolSize      int
	UserCount     int
	MessageCount  int
	RoomCount     int
	MessageBuffer int
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

	client.MessageCount.Store(int32(cf.MessageCount))
	client.expectedMessages.Store(int32(cf.MessageCount + 2))

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
				var serverProcessingTimes []int64

				for received < expected {
					select {
					case resp, ok := <-room.Conn.Send:
						if !ok {
							return
						}
						// processTime := resp.ServerTimestamp - resp.Timestamp
						// log.Printf("User %s in room %s echoed: %s | %d - %d | %d", room.UserId, room.RoomId, resp.MessageType, resp.ServerTimestamp, resp.Timestamp, processTime)
						received++
						processingTime := resp.ServerTimestamp - resp.Timestamp
						totalLatency += time.Now().UnixMilli() - resp.Timestamp

						serverProcessingTimes = append(serverProcessingTimes, processingTime)

						if received >= expected {
							log.Printf("User %s in room %s avg latency: %dms (%d/%d received)",
								room.UserId, room.RoomId, totalLatency/int64(received), received, expected)
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
				m.Timestamp = time.Now().UnixMilli()
				if err := room.Conn.Write(m); err != nil {
					log.Printf("User %s write to room %s error: %+v", room.UserId, room.RoomId, err)
				}
			}

			<-done
		}(room)
	}
}
