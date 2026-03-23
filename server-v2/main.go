// Command server starts the ChatFlow WebSocket server.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/google/uuid"
	rabbitmq "supriyakotturu.github.com/chatflow/internal/rabbitmq"
	server "supriyakotturu.github.com/chatflow/internal/server"
	"supriyakotturu.github.com/chatflow/pkg/env"
)

const (
	// bufferSize controls the per-client Send channel and the per-room Broadcast channel.
	// 1M test: 100 users/room × 1000 messages = 100,000 per client; use 120_000 with margin.
	bufferSize = 120_000

	// tempBufferSize is the small circuit-breaker secondary buffer used during reconnection.
	tempBufferSize = 2_048

	// publishChanSize is kept small so the channel fills quickly under load,
	// causing Publish() to block and apply back-pressure to the WebSocket read
	// loops. This keeps RabbitMQ queue depth steady rather than spiking.
	publishChanSize = 500
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	serverId := uuid.NewString()

	e, err := env.LoadRabbitEnv()
	if err != nil {
		log.Println("Error loading env: ", err)
		return
	}

	rabbit, err := rabbitmq.NewRabbitMQ(ctx, serverId, tempBufferSize, publishChanSize)
	if err != nil {
		log.Println("Error connecting to rabbitMQ: ", err)
		return
	}

	config := &server.ServerConfig{
		BufferSize: bufferSize,
		Id:         serverId,
		Ctx:        ctx,
		Rabbit:     rabbit,
		MaxRooms:   e.RoomCount,
	}
	s := server.NewServerMux(config)
	s.StartMonitoring(ctx, 30*time.Second)
	s.Start()
}
