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
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sony/gobreaker"
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


// Rabbit manages a single AMQP connection with per-worker publishers and
// per-room consumers, plus a circuit breaker for connection failures.
// Two separate AMQP connections are used: one for publishers, one for consumers.
// This prevents the 200 publish workers from starving consumer Accept acks on the
// same connection, which caused credit exhaustion and queue stalls.
type Rabbit struct {
	serverID        string
	exchangeName    string
	tempBuffer      chan *pendingMessage // secondary buffer used during reconnection
	publishChan     chan *pendingMessage // primary async publish queue
	conn            *rmq.AmqpConnection  // publisher connection
	consumerConn    *rmq.AmqpConnection  // separate consumer connection
	consumers       map[string]*rmq.Consumer
	handlers        map[string]func([]byte) // roomId → message handler, registered by Consume()
	handlersMu      sync.RWMutex
	cb              *gobreaker.CircuitBreaker
	stateChanged    chan *rmq.StateChanged
	droppedMessages atomic.Int64
	totalLagMs      atomic.Int64 // cumulative consumer lag (ms) since last log interval
	lagSampleCount  atomic.Int64 // number of lag samples since last log interval
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

	stateChanged := make(chan *rmq.StateChanged, 8)

	user := url.UserPassword(e.RabbitUser, e.RabbitPassword)
	addr := net.JoinHostPort(e.RabbitHost, e.RabbitPort)
	rmqUrl := url.URL{Scheme: "amqp", User: user, Host: addr, Path: "/"}
	consumers := make(map[string]*rmq.Consumer)
	roomCount := e.RoomCount

	// Create two AMQP connections: one for publishers, one for consumers.
	// Separating them prevents publish traffic from starving consumer ack roundtrips.
	environment := rmq.NewEnvironment(rmqUrl.String(), nil)
	conn, err := environment.NewConnection(ctx)
	if err != nil {
		rmq.Error("Error opening publisher connection", err)
		return nil, err
	}

	consumerConn, err := environment.NewConnection(ctx)
	if err != nil {
		rmq.Error("Error opening consumer connection", err)
		return nil, err
	}

	// Subscribe to state changes on both connections.
	// Used to flush tempBuffer on reconnect; gobreaker handles publish-failure detection.
	conn.NotifyStatusChange(stateChanged)
	consumerConn.NotifyStatusChange(stateChanged)

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

		consumer, err := consumerConn.NewConsumer(ctx, queueName, &rmq.ConsumerOptions{InitialCredits: 1000})
		if err != nil {
			rmq.Error("Error creating consumer for queue", queueName, err)
			return nil, err
		}
		consumers[bindingKey] = consumer
	}

	cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "rabbitmq-publish",
		MaxRequests: 5,                // requests allowed in half-open state
		Interval:    60 * time.Second, // rolling window for failure counts in closed state
		Timeout:     30 * time.Second, // how long to stay open before going half-open
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			// Trip after 5 consecutive publish failures
			return counts.ConsecutiveFailures >= 5
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			log.Printf("[circuit-breaker] %s: %v → %v", name, from, to)
		},
	})

	rabbit := &Rabbit{
		serverID:     serverId,
		exchangeName: exchangeName,
		tempBuffer:   make(chan *pendingMessage, tempBufferSize),
		publishChan:  make(chan *pendingMessage, publishChanSize),
		conn:         conn,
		consumerConn: consumerConn,
		stateChanged: stateChanged,
		consumers:    consumers,
		handlers:     make(map[string]func([]byte)),
		cb:           cb,
	}

	// Start publish worker goroutines, each with its own dedicated publisher
	// (sender link). Tuned via PUBLISH_WORKERS env var (default 30).
	// 30 workers balances WebSocket throughput against back-pressure.
	numPublishWorkers := e.PublishWorkers
	for range numPublishWorkers {
		pub, err := conn.NewPublisher(ctx, nil, nil)
		if err != nil {
			rmq.Error("Error creating publisher for worker", err)
			return nil, err
		}
		go rabbit.publishWorker(ctx, pub)
	}

	// Start one consumer goroutine per room. Each goroutine runs for the lifetime
	// of the server — it always calls Accept() so messages are never left Unacked,
	// even for rooms that have no local users yet. When a room is later created
	// via Consume(), its handler is registered and the goroutine routes messages
	// to it from that point forward.
	for roomId, consumer := range consumers {
		// roomId here is the bindingKey e.g. "room.1"; extract the numeric part
		// so it matches the key used in Consume() ("1", "2", …).
		shortId := roomId[len("room."):]
		go func(consumer *rmq.Consumer, shortId string) {
			for {
				delivery, err := consumer.Receive(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					rmq.Error("Error receiving message for room", shortId, err)
					continue
				}

				rabbit.handlersMu.RLock()
				h := rabbit.handlers[shortId]
				rabbit.handlersMu.RUnlock()

				// Measure consumer lag: time from publish to consume delivery.
				var lagMsg struct {
					Timestamp string `json:"timestamp"`
				}
				data := delivery.Message().GetData()
				if err := json.Unmarshal(data, &lagMsg); err == nil {
					if t, err := time.Parse(time.RFC3339Nano, lagMsg.Timestamp); err == nil {
						rabbit.totalLagMs.Add(time.Since(t).Milliseconds())
						rabbit.lagSampleCount.Add(1)
					}
				}

				if h != nil {
					h(data)
				}

				// Accept in a separate goroutine so the receive loop can immediately
				// call Receive() again, keeping AMQP credits flowing back to the broker.
				// Without this, Accept() blocks the loop and credits are exhausted after
				// InitialCredits messages, stalling the queue permanently.
				d := delivery
				go func() {
					if err := d.Accept(ctx); err != nil {
						rmq.Error("Error accepting message for room", shortId, err)
					}
				}()
			}
		}(consumer, shortId)
	}

	// Watch for RabbitMQ state changes and flush the temp buffer on reconnect.
	// The gobreaker handles publish-failure detection independently; this goroutine
	// only handles connection-level recovery (draining tempBuffer back into publishChan).
	go func(r *Rabbit) {
		for {
			select {
			case state := <-r.stateChanged:
				rmq.Info("RabbitMQ state changed: ", state)
				switch state.To.(type) {
				case *rmq.StateOpen:
					// Connection recovered — wait for gobreaker to also close before flushing.
					// If we drain tempBuffer while the breaker is still Open/HalfOpen,
					// publishDirect will re-buffer the messages (or drop them if tempBuffer is full).
					for r.cb.State() != gobreaker.StateClosed {
						select {
						case <-ctx.Done():
							return
						case <-time.After(1 * time.Second):
						}
					}
				drain:
					for {
						select {
						case msg := <-r.tempBuffer:
							if err := r.Publish(ctx, msg.roomId, msg.message); err != nil {
								rmq.Error("Error publishing buffered message", err)
							}
						default:
							break drain
						}
					}
				}
			case <-ctx.Done():
				log.Println("RabbitMQ context cancelled.")
				return
			}
		}
	}(rabbit)

	go rabbit.startMonitoring(ctx, e.RabbitHost, e.RabbitUser, e.RabbitPassword)

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

// Publish enqueues a message for async delivery. It blocks if the publish
// channel is full, applying back-pressure to the caller (WebSocket read loop).
// Returns an error only if ctx is cancelled before the message can be enqueued.
func (r *Rabbit) Publish(ctx context.Context, roomId string, message *models.QueueMessage) error {
	select {
	case r.publishChan <- &pendingMessage{roomId: roomId, message: message}:
		return nil
	case <-ctx.Done():
		r.droppedMessages.Add(1)
		return ctx.Err()
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

// publishDirect performs the actual AMQP publish through the gobreaker circuit breaker.
// When the breaker is open (too many consecutive failures), the message is buffered in
// tempBuffer for retry on reconnect. If tempBuffer is full the message is dropped.
// Called only from publishWorker goroutines.
func (r *Rabbit) publishDirect(ctx context.Context, publisher *rmq.Publisher, roomId string, message *models.QueueMessage) {
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

	_, err = r.cb.Execute(func() (interface{}, error) {
		pubCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		_, pubErr := publisher.Publish(pubCtx, msg)
		return nil, pubErr
	})

	if err != nil {
		if err == gobreaker.ErrOpenState || err == gobreaker.ErrTooManyRequests {
			// Breaker is open — buffer for retry on reconnect.
			select {
			case r.tempBuffer <- &pendingMessage{roomId: roomId, message: message}:
			default:
				r.droppedMessages.Add(1)
				rmq.Error("Circuit open and buffer full, dropping message for room", roomId)
			}
			return
		}
		rmq.Error("Error publishing message", err)
	}
}

// Consume registers handler for the given roomId. The consumer goroutine for
// this room was already started in NewRabbitMQ and will route incoming messages
// to handler from this point forward. Messages delivered before Consume is called
// are still accepted (to prevent stale Unacked entries) but are not forwarded.
func (r *Rabbit) Consume(ctx context.Context, roomId string, handler func([]byte)) error {
	bindingKey := fmt.Sprintf("room.%s", roomId)
	if _, ok := r.consumers[bindingKey]; !ok {
		return fmt.Errorf("no consumer found for room %s", roomId)
	}

	r.handlersMu.Lock()
	r.handlers[roomId] = handler
	r.handlersMu.Unlock()

	return nil
}

// startMonitoring logs consumer lag and polls RabbitMQ queue depths every 10s.
// Consumer lag = time from message publish to consume delivery (RFC3339Nano timestamp in QueueMessage).
// Queue depth is fetched from the RabbitMQ Management HTTP API (port 15672).
func (r *Rabbit) startMonitoring(ctx context.Context, host, user, password string) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	client := &http.Client{Timeout: 5 * time.Second}
	mgmtURL := fmt.Sprintf("http://%s:15672/api/queues", host)

	for {
		select {
		case <-ticker.C:
			// Log average consumer lag and reset accumulators atomically.
			// Swap(0) reads and resets in one operation, preventing loss of samples
			// added by consumer goroutines between Load() and Store(0).
			count := r.lagSampleCount.Swap(0)
			if count > 0 {
				avgLag := r.totalLagMs.Swap(0) / count
				log.Printf("[monitoring] consumer_lag avg=%dms samples=%d", avgLag, count)
			}

			// Poll RabbitMQ Management API for queue depths.
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, mgmtURL, nil)
			if err != nil {
				continue
			}
			req.SetBasicAuth(user, password)
			resp, err := client.Do(req)
			if err != nil {
				log.Printf("[monitoring] queue_depth poll error: %v", err)
				continue
			}

			var queues []struct {
				Name     string `json:"name"`
				Messages int64  `json:"messages"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&queues); err != nil {
				log.Printf("[monitoring] queue_depth decode error (status %s): %v", resp.Status, err)
			} else {
				var maxDepth, totalDepth int64
				for _, q := range queues {
					totalDepth += q.Messages
					if q.Messages > maxDepth {
						maxDepth = q.Messages
					}
				}
				log.Printf("[monitoring] queue_depth max=%d total=%d queues=%d", maxDepth, totalDepth, len(queues))
			}
			resp.Body.Close()

		case <-ctx.Done():
			return
		}
	}
}
