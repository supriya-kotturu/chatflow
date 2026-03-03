// Command server starts the ChatFlow WebSocket server.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/google/uuid"
	"supriyakotturu.github.com/chatflow/pkg/env"
	rabbitmq "supriyakotturu.github.com/chatflow/server/internal/rabbitmq"
	server "supriyakotturu.github.com/chatflow/server/internal/server"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	serverId := uuid.NewString()
	// bufferSize controls the per-client Send channel and the per-room Broadcast channel.
	// Peak per-client burst: UserCount * RoomsPerUser / TotalRooms * (MessageCount+2) ≈ 16,032.
	bufferSize := 20_000
	// tempBufferSize is the small circuit-breaker secondary buffer used during reconnection.
	tempBufferSize := 2_048
	// publishChanSize absorbs the peak burst before workers drain to AMQP.
	// 100_000 covers the default part-1 load (32u × 10r × 1002 ≈ 320K total).
	publishChanSize := 100_000

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
