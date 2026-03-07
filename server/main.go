// Command server starts the ChatFlow WebSocket server.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/google/uuid"
	server "supriyakotturu.github.com/chatflow/internal/server"
	"supriyakotturu.github.com/chatflow/pkg/env"
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

	config := &server.ServerConfig{
		BufferSize: bufferSize,
		Id:         serverId,
		Ctx:        ctx,
		MaxRooms:   e.RoomCount,
	}
	s := server.NewServerMux(config)
	s.Start()
}
