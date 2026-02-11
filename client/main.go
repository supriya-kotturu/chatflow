package main

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

func ParallelMessages(config *client.ClientConfig) {
	e, err := env.LoadEnv()
	if err != nil {
		log.Fatalf("Error loading environment variables: %+v", err)
	}

	pool := client.NewWsClientPool(config.PoolSize, e.Port)
	defer pool.CloseAll()

	var wg sync.WaitGroup

	fmt.Println("Starting parallel messages...")
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
				expectedMsgs := cf.MessageCount + 2 // JOIN + 2 TEXT + LEAVE
				receivedMsgs := 0

				go func(room string, client *client.WsClient) {
					defer func() {
						if r := recover(); r != nil {
							log.Printf("Reader goroutine panic: %v", r)
						}
					}()
					for resp := range client.Send {
						log.Printf("User %d in room %s received: %s | %s", userId, room, resp.Message.MessageType, resp.Message.Message)
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
	fmt.Println("Total time: ", end.Seconds())
}

func SequentialMessages(config *client.ClientConfig) {
	c := client.NewClient(config)
	defer c.Pool.CloseAll()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start := time.Now()

	fmt.Println("Starting sequential messages...")
	go c.GenerateMessages()
	c.WriteMessages(ctx)
	c.Wg.Wait()

	end := time.Since(start)
	fmt.Println("Total time: ", end.Seconds())
}

func main() {
	config := &client.ClientConfig{
		PoolSize:      320, // 32 * 10
		UserCount:     32,
		MessageCount:  1000,
		RoomCount:     10,
		MessageBuffer: 1250,
	}

	// ParallelMessages(config) //30.3093569
	SequentialMessages(config) //30.0860449
}
