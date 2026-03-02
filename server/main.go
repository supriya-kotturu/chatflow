// Command server starts the ChatFlow WebSocket server.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/google/uuid"
	"supriyakotturu.github.com/chatflow/pkg/env"
	rabbitmq "supriyakotturu.github.com/chatflow/server/internal/rabbitmq"
	server "supriyakotturu.github.com/chatflow/server/internal/server"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	serverId := uuid.NewString()
	bufferSize := 2048

	e, err := env.LoadRabbitEnv()
	if err != nil {
		log.Println("Error loading env: ", err)
		return
	}

	rabbit, err := rabbitmq.NewRabbitMQ(ctx, serverId, bufferSize)
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
	s.Start()
}
