package main

import (
	"log"
	"os"
	"os/signal"
	"sync"

	client "supriyakotturu.github.com/chatflow/client/ws"
	"supriyakotturu.github.com/chatflow/pkg/env"
	"supriyakotturu.github.com/chatflow/pkg/generate"
)

func main() {
	e, err := env.LoadEnv()
	if err != nil {
		log.Fatalf("Error loading environment variables: %+v", err)
	}

	pool, err := client.NewWsClientPool(10, e.Port)
	if err != nil {
		log.Fatalf("Error creating WebSocket client pool: %+v", err)
	}
	defer pool.CloseAll()

	var wg sync.WaitGroup

	for uid := 0; uid < 50000; uid++ {
		wg.Add(1)
		go func(userId int) {
			defer wg.Done()
			c := pool.Get()
			defer pool.Put(c)

			go func() {
				for msg := range c.ReadCh {
					log.Printf("%s received message: %+v", msg.Username, msg.MessageType)
				}
			}()

			for i := 0; i < 200; i++ {
				m := generate.NewMessage()
				if err := c.Write(m); err != nil {
					log.Printf("User %d write error: %+v", userId, err)
					return
				}
			}
		}(uid)
	}
	wg.Wait()

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	// Wait for interrupt signal
	<-interrupt
	log.Println("Closing connection...")
}
