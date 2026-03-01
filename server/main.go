// Command server starts the ChatFlow WebSocket server.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/google/uuid"
	rabbitmq "supriyakotturu.github.com/chatflow/server/internal/rabbitmq"
	server "supriyakotturu.github.com/chatflow/server/internal/server"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	serverId := uuid.NewString()
	bufferSize := 2048
	roomCount := 20

	rabbit, err := rabbitmq.NewRabbitMQ(ctx, serverId, bufferSize, roomCount)

	if err != nil {
		log.Println("Error connecting to rabbitMQ: ", err)
		return
	}

	config := &server.ServerConfig{
		BufferSize: bufferSize,
		Id:         serverId,
		Ctx:        ctx,
		Rabbit:     rabbit,
	}
	s := server.NewServerMux(config)
	s.Start()
}
