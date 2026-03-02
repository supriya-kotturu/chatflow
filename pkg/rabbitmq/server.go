package rabbitmq

import (
	"context"

	"supriyakotturu.github.com/chatflow/pkg/models"
)

type RabbitMQServer interface {
	Publish(roomId string, message *models.QueueMessage) error
	Consume(ctx context.Context, roomId string, handler func([]byte)) error
	ServerID() string
}

// RabbitMQServer is the interface that callers depend on for message fan-out.
// Use server/internal/rabbitmq.NewRabbitMQ to obtain a concrete implementation.
//
// Example usage:
//
//	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
//	defer cancel()
//
//	rabbit, err := rabbitmq.NewRabbitMQ(ctx, serverID, bufferSize)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Subscribe to messages published by other servers for a room.
//	rabbit.Consume(ctx, "roomId", func(msg []byte) {
//	    log.Printf("Received: %s", msg)
//	})
//
//	// Publish a message to all other servers subscribed to the same room.
//	rabbit.Publish("roomId", queueMessage)
//
//	<-ctx.Done() // blocks until interrupt
