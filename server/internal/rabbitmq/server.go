// Package rabbitmq implements RabbitMQ-backed message fan-out for ChatFlow.
// It uses a topic exchange with per-server auto-delete queues so that messages
// published by one server are delivered to all other servers in the cluster.
package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/url"
	"sync/atomic"
	"time"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	"supriyakotturu.github.com/chatflow/pkg/env"
	"supriyakotturu.github.com/chatflow/pkg/models"
)

// pendingMessage holds a message that could not be published while the
// circuit was open, buffered for retry once the connection is restored.
type pendingMessage struct {
	roomId  string
	message *models.QueueMessage
}

// CircuitState represents the health of the RabbitMQ connection.
type CircuitState int32

const (
	CircuitClosed    CircuitState = 0 // connection healthy, publish directly
	CircuitBuffering CircuitState = 1 // reconnecting, buffer messages in tempBuffer
	CircuitOpen      CircuitState = 2 // connection lost or buffer full, drop messages
)

// Rabbit manages a single AMQP connection with a shared publisher and
// per-room consumers, plus a circuit breaker for connection failures.
type Rabbit struct {
	serverID      string
	exchangeName  string
	tempBuffer    chan *pendingMessage
	conn          *rmq.AmqpConnection
	consumers     map[string]*rmq.Consumer
	publisher     *rmq.Publisher
	circuitStatus atomic.Int32
	stateChanged  chan *rmq.StateChanged
}

// NewRabbitMQ connects to RabbitMQ, declares a topic exchange, and creates
// per-server auto-delete queues for each room (0..ROOM_COUNT-1). It also starts
// a goroutine that watches connection state changes to drive the circuit breaker.
func NewRabbitMQ(ctx context.Context, serverId string, size int) (*Rabbit, error) {
	e, err := env.LoadRabbitEnv()
	if err != nil {
		return nil, err
	}

	exchangeName := "chat.exchange"

	stateChanged := make(chan *rmq.StateChanged, 1)

	user := url.UserPassword(e.RabbitUser, e.RabbitPassword)
	addr := net.JoinHostPort(e.RabbitHost, e.RabbitPort)
	rmqUrl := url.URL{Scheme: "amqp", User: user, Host: addr, Path: "/"}
	consumers := make(map[string]*rmq.Consumer)
	roomCount := e.RoomCount

	// Create a RabbitMQ Connection
	environment := rmq.NewEnvironment(rmqUrl.String(), nil)
	conn, err := environment.NewConnection(ctx)
	if err != nil {
		rmq.Error("Error opening connection", err)
		return nil, err
	}

	// Subscribe to the RabbitMQ Status Change
	conn.NotifyStatusChange(stateChanged)

	// Create a Topic Exchange
	management := conn.Management()
	exchange := &rmq.TopicExchangeSpecification{
		Name: exchangeName,
	}
	_, err = management.DeclareExchange(ctx, exchange)

	if err != nil {
		rmq.Error("Error declaring exchange", err)
		return nil, err
	}

	// Bind Routing Keys with their respective Binding Keys
	for r := range roomCount {
		queueName := fmt.Sprintf("room.%d.%s", r, serverId)
		queue := &rmq.ClassicQueueSpecification{
			Name:         queueName,
			IsAutoDelete: true,
		}
		queueInfo, err := management.DeclareQueue(ctx, queue)
		if err != nil {
			rmq.Error("Error declaring queue [%s]: ", queue, err)
			return nil, err
		}

		bindingKey := fmt.Sprintf("room.%d", r)
		_, err = management.Bind(ctx, &rmq.ExchangeToQueueBindingSpecification{
			SourceExchange:   exchangeName,
			DestinationQueue: queueInfo.Name(),
			BindingKey:       bindingKey,
		})
		if err != nil {
			rmq.Error("Error binding queue", queueName, err)
			return nil, err
		}

		consumer, err := conn.NewConsumer(ctx, queueName, nil)
		if err != nil {
			rmq.Error("Error creating consumer for queue", queueName, err)
			return nil, err
		}
		consumers[bindingKey] = consumer
	}

	// Create a Publisher
	publisher, err := conn.NewPublisher(ctx, nil, nil)

	if err != nil {
		rmq.Error("Error creating publisher", err)
		return nil, err
	}

	rabbit := &Rabbit{
		serverID:     serverId,
		exchangeName: exchangeName,
		tempBuffer:   make(chan *pendingMessage, size),
		conn:         conn,
		stateChanged: stateChanged,
		publisher:    publisher,
		consumers:    consumers,
	}

	// Watch for RabbitMQ state changes
	go func(r *Rabbit) {
		for {
			select {
			case state := <-r.stateChanged:
				rmq.Info("RabbitMQ state changed: ", state)
				switch state.To.(type) {
				case *rmq.StateClosed:
					r.circuitStatus.Store(int32(CircuitOpen))
				case *rmq.StateOpen:
					r.circuitStatus.Store(int32(CircuitClosed))

					// Flush buffered messages
				drain:
					for {
						select {
						case msg := <-r.tempBuffer:
							if err := r.Publish(msg.roomId, msg.message); err != nil {
								rmq.Error("Error publishing buffered message", err)
							}
						default:
							break drain
						}
					}
				case *rmq.StateReconnecting:
					r.circuitStatus.Store(int32(CircuitBuffering))
				case *rmq.StateClosing:
					r.circuitStatus.Store(int32(CircuitOpen))
				}
			case <-ctx.Done():
				log.Println("RabbitMQ context cancelled.")
				return
			}
		}
	}(rabbit)

	return rabbit, nil
}

// ServerID returns the unique identifier of this server instance.
func (r *Rabbit) ServerID() string {
	return r.serverID
}

// Publish sends a QueueMessage to the topic exchange under routing key "room.{roomId}".
// Behavior depends on circuit state: publish directly when closed, buffer when
// reconnecting, or return an error when open (connection lost or buffer full).
func (r *Rabbit) Publish(roomId string, message *models.QueueMessage) error {
	currentCircuitStatus := r.circuitStatus.Load()

	switch CircuitState(currentCircuitStatus) {
	case CircuitClosed:
		routingKey := fmt.Sprintf("room.%s", roomId)
		exchangeAddress := &rmq.ExchangeAddress{
			Exchange: r.exchangeName,
			Key:      routingKey,
		}

		marshalledMsg, err := json.Marshal(message)
		if err != nil {
			rmq.Error("Error marshalling message", err)
			return err
		}

		msg, err := rmq.NewMessageWithAddress([]byte(marshalledMsg), exchangeAddress)
		if err != nil {
			rmq.Error("Error creating message with address", err)
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, err = r.publisher.Publish(ctx, msg)
		if err != nil {
			rmq.Error("Error publishing message", err)
			return err
		}
		return nil
	case CircuitBuffering:
		select {
		case r.tempBuffer <- &pendingMessage{
			roomId:  roomId,
			message: message,
		}:
		default:
			r.circuitStatus.Store(int32(CircuitOpen))
			return fmt.Errorf("buffer is full, cannot publish message")
		}
	case CircuitOpen:
		return fmt.Errorf("circuit is open, cannot publish message")
	}

	return nil
}

// Consume starts a goroutine that receives messages from the room's queue and
// calls handler with the raw message bytes. It exits cleanly when ctx is cancelled.
func (r *Rabbit) Consume(ctx context.Context, roomId string, handler func([]byte)) error {
	bindingKey := fmt.Sprintf("room.%s", roomId)
	consumer, ok := r.consumers[bindingKey]
	if !ok {
		return fmt.Errorf("no consumer found for room %s", roomId)
	}

	go func() {
		for {
			delivery, err := consumer.Receive(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				rmq.Error("Error receiving message for room", roomId, err)
				continue
			}

			handler(delivery.Message().GetData())

			if err := delivery.Accept(ctx); err != nil {
				rmq.Error("Error accepting message for room", roomId, err)
			}
		}
	}()

	return nil
}
