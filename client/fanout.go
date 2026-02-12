package client

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	client "supriyakotturu.github.com/chatflow/client/ws"
	"supriyakotturu.github.com/chatflow/pkg/env"
	"supriyakotturu.github.com/chatflow/pkg/generate"
)

// RunFanOutLoadTest runs a load test using the fan-out pattern: each user
// goroutine directly manages its own rooms, connections, and message I/O.
func RunFanOutLoadTest(config *client.ClientConfig) {
	e, err := env.LoadEnv()
	if err != nil {
		log.Fatalf("Error loading environment variables: %+v", err)
	}

	pool := client.NewWsClientPool(config.PoolSize, e.Port)
	defer pool.CloseAll()

	var wg sync.WaitGroup

	fmt.Println("Starting fan-out load test...")
	start := time.Now()

	for uid := 0; uid < config.UserCount; uid++ {
		wg.Add(1)
		go func(userId int, cf *client.ClientConfig) {
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
				expectedMsgs := cf.MessageCount + 2 // JOIN + TEXT + LEAVE
				receivedMsgs := 0

				// Log the Response Messages sent from server
				go func(room string, client *client.WsClient) {
					defer func() {
						if r := recover(); r != nil {
							log.Printf("Reader goroutine panic: %v", r)
						}
					}()
					for resp := range client.Send {
						log.Printf("User %d in room %s received: %s | %s", userId, room, resp.MessageType, resp.Message.Message)
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
					log.Printf("User %d join room %s error: %+v", userId, roomId, err)
					continue
				}

				// Send messages
				doneCh := make(chan struct{})

				go func(doneCh chan struct{}) {
					for i := 0; i < cf.MessageCount; i++ {
						id := strconv.Itoa(userId)
						m := generate.NewMessage(id)
						m.Timestamp = time.Now().Format(time.RFC3339Nano)
						if err := c.Write(m); err != nil {
							log.Printf("User %s write to room %s error: %+v", id, roomId, err)
						}
					}
					doneCh <- struct{}{}
				}(doneCh)

				<-doneCh

				// Leave room
				leaveMsg := generate.NewLeaveMessage(strconv.Itoa(userId), roomId)
				if err := c.Write(leaveMsg); err != nil {
					log.Printf("User %d leave room %s error: %+v", userId, roomId, err)
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
				log.Printf("DONE: User %d in room %s timed out: %d/%d", userId, roomId, receivedMsgs, expectedMsgs)
				// Close connection after timeout or completion
				pool.Remove(strconv.Itoa(userId), roomId)
			}
		}(uid, config)
	}
	wg.Wait()
	log.Println("Closing connection...")
	end := time.Since(start)
	fmt.Printf("Total time: %.1fs\n", end.Seconds())
}
