package client

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	ws "supriyakotturu.github.com/chatflow/client/ws"
	"supriyakotturu.github.com/chatflow/pkg/env"
	"supriyakotturu.github.com/chatflow/pkg/generate"
)

// RunFanOutLoadTest runs a load test using the fan-out pattern: each user
// goroutine directly manages its own rooms, connections, and message I/O.
func RunFanOutLoadTest(config *ws.ClientConfig) {
	e, err := env.LoadEnv()
	if err != nil {
		log.Fatalf("Error loading environment variables: %+v", err)
	}

	pool := ws.NewWsClientPool(config.PoolSize, e.ServerHost, e.Port)
	defer pool.CloseAll()

	var wg sync.WaitGroup

	fmt.Println("Starting fan-out load test...")
	start := time.Now()

	for uid := 0; uid < config.UserCount; uid++ {
		wg.Add(1)
		go func(userId int, cf *ws.ClientConfig) {
			defer wg.Done()

			// Same user joins multiple rooms
			roomIds := generate.NewRooms(cf.RoomCount)
			for _, roomId := range roomIds {
				// Get separate connection for each room
				c, err := pool.GetOrCreateNewWsClient(strconv.Itoa(userId), roomId)
				if err != nil {
					log.Printf("User %d failed to connect to room %s: %+v", userId, roomId, err)
					continue
				}

				// Channel to signal when all messages are received
				msgDone := make(chan struct{})
				readerDone := make(chan struct{})
				expectedMsgs := cf.MessageCount + 2 // JOIN + TEXT + LEAVE
				receivedMsgs := 0

				// Log the Response Messages sent from server
				go func(room string, conn *ws.WsClient) {
					defer close(readerDone)
					defer func() {
						if r := recover(); r != nil {
							log.Printf("Reader goroutine panic: %v", r)
						}
					}()

					for _ = range conn.Send {
						// log.Printf("User %d in room %s received: %s | %s", userId, room, resp.MessageType, resp.Message.Message)
						receivedMsgs++
						if receivedMsgs >= expectedMsgs {
							close(msgDone)
							return
						}
					}
				}(roomId, c)

				// Join room
				joinMsg := generate.NewJoinMessage(strconv.Itoa(userId), roomId)
				if err := c.Write(joinMsg); err != nil {
					pool.FailedMessages.Add(1)
					log.Printf("User %d join room %s error: %+v", userId, roomId, err)
					continue
				} else {
					pool.SuccessfulMessages.Add(1)
				}

				// Send messages
				doneCh := make(chan struct{})

				go func(doneCh chan struct{}) {
					for i := 0; i < cf.MessageCount; i++ {
						id := strconv.Itoa(userId)
						m := generate.NewMessage(id)
						m.Timestamp = time.Now().Format(time.RFC3339Nano)
						if err := c.Write(m); err != nil {
							pool.FailedMessages.Add(1)
							log.Printf("User %s write to room %s error: %+v", id, roomId, err)
						} else {
							pool.SuccessfulMessages.Add(1)
						}
					}
					doneCh <- struct{}{}
				}(doneCh)

				<-doneCh

				// Leave room
				leaveMsg := generate.NewLeaveMessage(strconv.Itoa(userId), roomId)
				if err := c.Write(leaveMsg); err != nil {
					pool.FailedMessages.Add(1)
					log.Printf("User %d leave room %s error: %+v", userId, roomId, err)
				} else {
					pool.SuccessfulMessages.Add(1)
				}

				// Wait for all messages to be received with timeout
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

				select {
				case <-msgDone:
					// All messages received
				case <-ctx.Done():
					// Timeout - close anyway
					log.Printf("User %d in room %s timed out: %d/%d", userId, roomId, receivedMsgs, expectedMsgs)
				}

				cancel()
				// Close connection and wait for reader to drain
				pool.Remove(strconv.Itoa(userId), roomId)
				<-readerDone
				log.Printf("DONE: User %d in room %s: %d/%d received", userId, roomId, receivedMsgs, expectedMsgs)
			}
		}(uid, config)
	}
	wg.Wait()
	wallTime := time.Since(start)

	successful := pool.SuccessfulMessages.Load()
	failed := pool.FailedMessages.Load()

	fmt.Println("\n=== Performance Metrics - Warm up ===")
	fmt.Printf("Total runtime (wall time):  %.1fs\n", wallTime.Seconds())
	fmt.Printf("Successful messages sent:   %d\n", successful)
	fmt.Printf("Failed messages:            %d\n", failed)
	fmt.Printf("Overall throughput:         %.1f msg/s\n", float64(successful)/wallTime.Seconds())
	fmt.Printf("Total connections:          %d\n", pool.TotalConnections.Load())
	fmt.Printf("Reconnections:              %d\n", pool.Reconnections.Load())
	fmt.Printf("Failed connections:         %d\n", pool.FailedConnections.Load())
}
