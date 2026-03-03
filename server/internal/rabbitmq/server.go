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

// Rabbit manages a single AMQP connection with per-worker publishers and
// per-room consumers, plus a circuit breaker for connection failures.
type Rabbit struct {
	serverID        string
	exchangeName    string
	tempBuffer      chan *pendingMessage // secondary buffer used during reconnection
	publishChan     chan *pendingMessage // primary async publish queue
	conn            *rmq.AmqpConnection
	consumers       map[string]*rmq.Consumer
	circuitStatus   atomic.Int32
	stateChanged    chan *rmq.StateChanged
	droppedMessages atomic.Int64
}

// NewRabbitMQ connects to RabbitMQ, declares a topic exchange, and creates
// per-server auto-delete queues for each room (1..ROOM_COUNT). It also starts
// a goroutine that watches connection state changes to drive the circuit breaker.
//
// tempBufferSize is the circuit-breaker secondary buffer (small, ~2048).
// publishChanSize is the primary async publish queue — size it to absorb the
// peak burst of incoming messages, e.g. UserCount * MessageCount * RoomsPerUser.
func NewRabbitMQ(ctx context.Context, serverId string, tempBufferSize int, publishChanSize int) (*Rabbit, error) {
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

	// Bind Routing Keys with their respective Binding Keys.
	// Client generates room IDs "1".."roomCount" (rand.Intn(N)+1), so
	// consumers must cover that same range: 1..roomCount inclusive.
	for r := 1; r <= roomCount; r++ {
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

	rabbit := &Rabbit{
		serverID:     serverId,
		exchangeName: exchangeName,
		tempBuffer:   make(chan *pendingMessage, tempBufferSize),
		publishChan:  make(chan *pendingMessage, publishChanSize),
		conn:         conn,
		stateChanged: stateChanged,
		consumers:    consumers,
	}

	// Start publish worker goroutines, each with its own dedicated publisher
	// (sender link). This gives each worker independent AMQP flow control and
	// avoids serialisation on a shared publisher's internal state.
	const numPublishWorkers = 12
	for range numPublishWorkers {
		pub, err := conn.NewPublisher(ctx, nil, nil)
		if err != nil {
			rmq.Error("Error creating publisher for worker", err)
			return nil, err
		}
		go rabbit.publishWorker(ctx, pub)
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

// DroppedMessages returns the total number of messages dropped because the
// publish channel was full (i.e. cross-server delivery was lost for these).
func (r *Rabbit) DroppedMessages() int64 {
	return r.droppedMessages.Load()
}

// Publish enqueues a message for async delivery. It returns immediately;
// the actual AMQP I/O is handled by the background publishWorker goroutines.
func (r *Rabbit) Publish(roomId string, message *models.QueueMessage) error {
	select {
	case r.publishChan <- &pendingMessage{roomId: roomId, message: message}:
		return nil
	default:
		r.droppedMessages.Add(1)
		return fmt.Errorf("publish channel full, dropping message for room %s", roomId)
	}
}

// publishWorker drains publishChan and calls publishDirect for each message.
// Each worker owns its own publisher to avoid serialisation on shared state.
// It exits when ctx is cancelled.
func (r *Rabbit) publishWorker(ctx context.Context, publisher *rmq.Publisher) {
	for {
		select {
		case pm := <-r.publishChan:
			r.publishDirect(ctx, publisher, pm.roomId, pm.message)
		case <-ctx.Done():
			return
		}
	}
}

// publishDirect performs the actual AMQP publish with circuit-breaker logic.
// Called only from publishWorker goroutines.
func (r *Rabbit) publishDirect(ctx context.Context, publisher *rmq.Publisher, roomId string, message *models.QueueMessage) {
	switch CircuitState(r.circuitStatus.Load()) {
	case CircuitClosed:
		routingKey := fmt.Sprintf("room.%s", roomId)
		exchangeAddress := &rmq.ExchangeAddress{
			Exchange: r.exchangeName,
			Key:      routingKey,
		}

		marshalledMsg, err := json.Marshal(message)
		if err != nil {
			rmq.Error("Error marshalling message", err)
			return
		}

		msg, err := rmq.NewMessageWithAddress(marshalledMsg, exchangeAddress)
		if err != nil {
			rmq.Error("Error creating message with address", err)
			return
		}

		pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err = publisher.Publish(pubCtx, msg)
		cancel()
		if err != nil {
			rmq.Error("Error publishing message", err)
		}

	case CircuitBuffering:
		select {
		case r.tempBuffer <- &pendingMessage{roomId: roomId, message: message}:
		default:
			r.circuitStatus.Store(int32(CircuitOpen))
			rmq.Error("Buffer full, dropping message for room", roomId)
		}

	case CircuitOpen:
		rmq.Error("Circuit open, dropping message for room", roomId)
	}
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
