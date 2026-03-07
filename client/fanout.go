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
	"supriyakotturu.github.com/chatflow/pkg/models"
)

// RunFanOutLoadTest runs a load test using the fan-out pattern: each user
// goroutine directly manages its own rooms, connections, and message I/O.
func RunFanOutLoadTest(config *ws.ClientConfig) {
	e, err := env.LoadServerEnv()
	if err != nil {
		log.Fatalf("Error loading environment variables: %+v", err)
	}

	pool := ws.NewWsClientPool(config.PoolSize, e.ServerHost, e.Port, config.MessageBuffer)
	defer pool.CloseAll()

	var wg sync.WaitGroup

	fmt.Println("Starting fan-out load test...")
	start := time.Now()

	for uid := 1; uid <= config.UserCount; uid++ {
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
				// Average users per room = UserCount * RoomCount / TotalRooms.
				// This is an estimate — actual count varies per room; the 30s timeout is the fallback.
				expectedMsgs := (cf.MessageCount + 2) * cf.UserCount * cf.RoomCount / generate.TotalRooms
				expectedSent := cf.MessageCount + 2
				receivedMsgs := 0
				sentMsgs := 0
				usersSeenSending := make(map[string]struct{})

				// Read responses from the server; count JOINs to compute actual expected.
				go func(room string, conn *ws.WsClient) {
					defer close(readerDone)
					defer func() {
						if r := recover(); r != nil {
							log.Printf("Reader goroutine panic: %v", r)
						}
					}()

					for resp := range conn.Send {
						receivedMsgs++
						if resp.MessageType == models.MessageTypeJoin {
							usersSeenSending[resp.UserID] = struct{}{}
						}
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
					sentMsgs++
				}

				// Send messages
				doneCh := make(chan int, 1)

				go func(doneCh chan int) {
					count := 0
					for i := 0; i < cf.MessageCount; i++ {
						id := strconv.Itoa(userId)
						m := generate.NewMessage(id)
						m.Timestamp = time.Now().Format(time.RFC3339Nano)
						if err := c.Write(m); err != nil {
							pool.FailedMessages.Add(1)
							log.Printf("User %s write to room %s error: %+v", id, roomId, err)
						} else {
							pool.SuccessfulMessages.Add(1)
							count++
						}
					}
					doneCh <- count
				}(doneCh)

				sentMsgs += <-doneCh

				// Leave room
				leaveMsg := generate.NewLeaveMessage(strconv.Itoa(userId), roomId)
				if err := c.Write(leaveMsg); err != nil {
					pool.FailedMessages.Add(1)
					log.Printf("User %d leave room %s error: %+v", userId, roomId, err)
				} else {
					pool.SuccessfulMessages.Add(1)
					sentMsgs++
				}

				// Wait for all messages to be received with timeout
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

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
				// actualExpected is computed from JOINs seen: each JOIN = one co-present user
				// who will send (MessageCount + 2) messages total.
				actualExpected := len(usersSeenSending) * expectedSent
				log.Printf("DONE: User %d in room %s: sent %d/%d | received %d/%d", userId, roomId, sentMsgs, expectedSent, receivedMsgs, actualExpected)
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
