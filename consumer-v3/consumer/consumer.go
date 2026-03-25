package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/url"
	"time"

	"github.com/jackc/pgx/v5"
	pgxpool "github.com/jackc/pgx/v5/pgxpool"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	"github.com/sony/gobreaker"
	"supriyakotturu.github.com/chatflow/pkg/env"
	"supriyakotturu.github.com/chatflow/pkg/models"
)

type ConsumerConfig struct {
	Ctx              context.Context
	DLQBufferSize    int
	DBChanBufferSize int
}

type Consumer struct {
	Ctx             context.Context
	DBChan          chan *models.QueueMessage
	StatsChan       chan *models.QueueMessage
	DLQChan         chan []*models.QueueMessage
	RmqConsumers    []*rmq.Consumer
	DBConn          *pgxpool.Pool
	consumerWorkers int
	statsWorkers    int
	dbWorkers       int
	bulkSize        int
	flushInterval   int
	cb              *gobreaker.CircuitBreaker
}

type userRoomDetails struct {
	userIDs        []string
	roomIDs        []string
	lastActivities []time.Time
}

func NewConsumer(cf *ConsumerConfig) (*Consumer, error) {
	e, err := env.LoadConsumerEnv()
	if err != nil {
		log.Println("Error loading the Consumer environment variables.")
		return nil, err
	}

	exchangeName := "chat.exchange"
	// stateChanged := make(chan *rmq.StateChanged, 8)

	user := url.UserPassword(e.RabbitUser, e.RabbitPassword)
	addr := net.JoinHostPort(e.RabbitHost, e.RabbitPort)
	rmqUrl := url.URL{Scheme: "amqp", User: user, Host: addr, Path: "/"}

	// Create a AMQP Connection
	environment := rmq.NewEnvironment(rmqUrl.String(), nil)
	consumerConn, err := environment.NewConnection(cf.Ctx)
	if err != nil {
		rmq.Error("Error opening consumer connection", err)
		return nil, err
	}

	// Subscribe to state changes on both connections.
	// consumerConn.NotifyStatusChange(stateChanged)

	// Create a Topic Exchange
	management := consumerConn.Management()
	exchange := &rmq.TopicExchangeSpecification{
		Name: exchangeName,
	}
	_, err = management.DeclareExchange(cf.Ctx, exchange)
	if err != nil {
		rmq.Error("Error declaring the exchange", err)
		return nil, err
	}

	queueName := "persistence_queue"
	queue := &rmq.ClassicQueueSpecification{
		Name:         queueName,
		IsAutoDelete: false,
	}

	// Create a queue
	queueInfo, err := management.DeclareQueue(cf.Ctx, queue)
	if err != nil {
		rmq.Error("Error declaring queue [%s]: ", queue, err)
		return nil, err
	}

	// Bind Routing Keys with the binding key.
	bindingKey := "room.#"
	_, err = management.Bind(cf.Ctx, &rmq.ExchangeToQueueBindingSpecification{
		SourceExchange:   exchangeName,
		DestinationQueue: queueInfo.Name(),
		BindingKey:       bindingKey,
	})
	if err != nil {
		rmq.Error("Error binding queue", queueName, err)
		return nil, err
	}

	// Register a consumer on the queue.
	rmqConsumers := make([]*rmq.Consumer, 0, e.ConsumerWorkers)
	for range e.ConsumerWorkers {
		rmqConsumer, err := consumerConn.NewConsumer(cf.Ctx, queueName, &rmq.ConsumerOptions{InitialCredits: 500})
		if err != nil {
			rmq.Error("Error creating consumer for queue", queueName, err)
			return nil, err
		}
		rmqConsumers = append(rmqConsumers, rmqConsumer)
	}

	// DB connection
	dbUser := url.UserPassword(e.DBUser, e.DBPassword)
	dbAddr := net.JoinHostPort(e.DBHost, e.DBPort)
	dbUrl := url.URL{Scheme: "postgres", User: dbUser, Host: dbAddr, Path: "/" + e.DBName, RawQuery: "search_path=chatflow"}

	poolConfig, _ := pgxpool.ParseConfig(dbUrl.String())
	poolConfig.ConnConfig.RuntimeParams["search_path"] = "chatflow"
	poolConfig.MaxConns = int32(e.DBWorkers + e.StatsWorkers + 2) // headroom for metrics queries
	dbConn, err := pgxpool.NewWithConfig(cf.Ctx, poolConfig)
	if err != nil {
		log.Println("Error initiating the DB connection.")
		return nil, err
	}

	cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "consumer",
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

	consumer := &Consumer{
		Ctx:             cf.Ctx,
		DBChan:          make(chan *models.QueueMessage, cf.DBChanBufferSize),
		StatsChan:       make(chan *models.QueueMessage, cf.DBChanBufferSize),
		DLQChan:         make(chan []*models.QueueMessage, cf.DLQBufferSize),
		RmqConsumers:    rmqConsumers,
		DBConn:          dbConn,
		consumerWorkers: e.ConsumerWorkers,
		statsWorkers:    e.StatsWorkers,
		dbWorkers:       e.DBWorkers,
		bulkSize:        e.BatchSize,
		flushInterval:   e.FlushInterval,
		cb:              cb,
	}

	return consumer, nil
}

func (c *Consumer) ConsumeMessage(msg []byte) {
	queueMsg := &models.QueueMessage{}

	if err := json.Unmarshal(msg, queueMsg); err != nil {
		log.Printf("Error un-marshalling message from RabbitMQ: %+v\n", err)
		return
	}

	select {
	case c.DBChan <- queueMsg:
	case <-c.Ctx.Done():
		return
	}

	select {
	case c.StatsChan <- queueMsg:
	case <-c.Ctx.Done():
		return
	default:
		log.Println("Losing messages from statsChan. Buffer full.")
	}

}

func (c *Consumer) ConsumeFromRmq(rmqConsumer *rmq.Consumer) {
	var lagCount int
	var lagTotal time.Duration

	for {
		delivery, err := rmqConsumer.Receive(c.Ctx)
		if err != nil {
			if c.Ctx.Err() != nil {
				return
			}
			rmq.Error("Error receiving message from the persistence queue", err)
			continue
		}

		var lagMsg struct {
			Timestamp string `json:"timestamp"`
		}
		data := delivery.Message().GetData()
		if err := json.Unmarshal(data, &lagMsg); err == nil {
			if t, err := time.Parse(time.RFC3339Nano, lagMsg.Timestamp); err == nil {
				lagTotal += time.Since(t)
				lagCount++
				if lagCount >= 5000 {
					log.Printf("Avg message lag (last 5000): %s\n", lagTotal/time.Duration(lagCount))
					lagCount = 0
					lagTotal = 0
				}
			}
		}

		d := delivery
		go func() {
			if err := d.Accept(c.Ctx); err != nil {
				rmq.Error("Error accepting the message", err)
			}
		}()
		c.ConsumeMessage(data)
	}
}

func (c *Consumer) getUserRoomDetails(batch []*models.QueueMessage) *userRoomDetails {
	userRoomsMap := make(map[string]map[string]time.Time) // userId -> roomId -> timeStamp
	details := &userRoomDetails{
		userIDs:        make([]string, 0),
		roomIDs:        make([]string, 0),
		lastActivities: make([]time.Time, 0),
	}

	for _, msg := range batch {
		t, err := time.Parse(time.RFC3339Nano, msg.Timestamp)
		if err == nil {
			if lastActivityTimestamp, ok := userRoomsMap[msg.Message.UserID][msg.RoomID]; !ok || t.After(lastActivityTimestamp) {
				if _, ok := userRoomsMap[msg.Message.UserID]; !ok {
					userRoomsMap[msg.Message.UserID] = make(map[string]time.Time)
				}
				userRoomsMap[msg.Message.UserID][msg.RoomID] = t
			}
		}
	}

	for userId, rooms := range userRoomsMap {
		for roomId, ts := range rooms {
			details.userIDs = append(details.userIDs, userId)
			details.roomIDs = append(details.roomIDs, roomId)
			details.lastActivities = append(details.lastActivities, ts)
		}
	}

	return details
}

// writeBatchToDB writes a batch to the DB with retries and circuit breaker.
// If sendToDLQ is false (DLQ drain path), a final failure is logged and discarded
// instead of re-queuing — preventing an infinite bounce cycle.
func (c *Consumer) writeBatchToDB(batch []*models.QueueMessage, sendToDLQ bool) error {
	userRooms := c.getUserRoomDetails(batch)
	maxRetries := 5
	backoff := 2 * time.Second

	connectAndWriteToDB := func() error {
		// Create a transaction and Rollback if the bulk write fails.
		tx, err := c.DBConn.Begin(c.Ctx)
		if err != nil {
			log.Printf("Error creating a transaction")
			return err
		}

		// Copy the values from msg
		msgCopyCount, err := tx.CopyFrom(c.Ctx,
			pgx.Identifier{"messages"},
			[]string{"message_id", "user_id", "room_id", "server_id", "username", "content", "message_type", "timestamp"},
			pgx.CopyFromSlice(len(batch), func(i int) ([]any, error) {
				m := batch[i]
				ts, err := time.Parse(time.RFC3339Nano, m.Timestamp)
				if err != nil {
					return nil, fmt.Errorf("invalid timestamp %q: %w", m.Timestamp, err)
				}
				return []any{m.MessageID, m.Message.UserID, m.RoomID, m.ServerID, m.Message.Username, m.Message.Message, string(m.Message.MessageType), ts}, nil
			}))

		if err != nil {
			tx.Rollback(c.Ctx)
			log.Printf("copy failed: %s", err)
			return err
		}
		_, err = tx.Exec(c.Ctx, `
			INSERT INTO user_rooms (user_id, room_id, last_activity_timestamp)
			SELECT * FROM UNNEST($1::text[], $2::text[], $3::timestamptz[])
			ON CONFLICT (user_id, room_id) DO UPDATE
			SET last_activity_timestamp = GREATEST(
				EXCLUDED.last_activity_timestamp,
				user_rooms.last_activity_timestamp
			)`,
			userRooms.userIDs, userRooms.roomIDs, userRooms.lastActivities,
		)
		if err != nil {
			tx.Rollback(c.Ctx)
			log.Printf("user_rooms upsert failed: %s", err)
			return err
		}

		err = tx.Commit(c.Ctx)
		if err != nil {
			log.Printf("Error committing the transaction: %s", err)
			return err
		}
		log.Printf("Inserted %d messages in DB.", msgCopyCount)

		return nil
	}

	if c.cb.State() == gobreaker.StateOpen {
		if sendToDLQ {
			select {
			case c.DLQChan <- batch:
			case <-c.Ctx.Done():
			}
		} else {
			log.Printf("[dlq] circuit open, discarding batch of %d", len(batch))
		}
		return gobreaker.ErrOpenState
	}

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		_, lastErr = c.cb.Execute(func() (interface{}, error) {
			err := connectAndWriteToDB()
			return nil, err
		})

		if lastErr == gobreaker.ErrOpenState || lastErr == gobreaker.ErrTooManyRequests {
			break
		}

		if lastErr == nil {
			return nil
		}

		time.Sleep(backoff)
		backoff *= 2
		log.Printf("attempt %d failed writing batch to DB: %v, retrying in %v...\n", attempt+1, lastErr, backoff)
	}

	if sendToDLQ {
		select {
		case c.DLQChan <- batch:
		case <-c.Ctx.Done():
		}
	} else {
		log.Printf("[dlq] discarding batch of %d after retry failure: %v", len(batch), lastErr)
	}

	return lastErr
}

func (c *Consumer) WriteToDB() {
	ticker := time.NewTicker(time.Duration(c.flushInterval) * time.Millisecond)
	batch := make([]*models.QueueMessage, 0, c.bulkSize)

	for {
		select {
		case dlqBatch := <-c.DLQChan:
			log.Printf("[dlq] retrying batch of %d\n", len(dlqBatch))
			if err := c.writeBatchToDB(dlqBatch, false); err != nil {
				log.Printf("[dlq] discarding batch of %d: %v", len(dlqBatch), err)
			}
		default:
		}

		select {
		case msg := <-c.DBChan:
			batch = append(batch, msg)
		case dlqBatch := <-c.DLQChan:
			log.Printf("[dlq] retrying batch of %d\n", len(dlqBatch))
			if err := c.writeBatchToDB(dlqBatch, false); err != nil {
				log.Printf("[dlq] discarding batch of %d: %v", len(dlqBatch), err)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				if err := c.writeBatchToDB(batch, true); err != nil {
					log.Printf("Error writing batch to DB: %s", err)
					batch = batch[:0]
					continue
				}
				batch = batch[:0]
			}
		case <-c.Ctx.Done():
			return
		}

		if len(batch) >= c.bulkSize {
			if err := c.writeBatchToDB(batch, true); err != nil {
				log.Printf("Error writing batch to DB: %s", err)
				batch = batch[:0]
				continue
			}
			batch = batch[:0]
		}
	}
}

func getKeysAndValues[K comparable, V any](m map[K]V) ([]K, []V) {
	keys := make([]K, 0, len(m))
	values := make([]V, 0, len(m))

	for k, v := range m {
		keys = append(keys, k)
		values = append(values, v)
	}
	return keys, values
}

func (c *Consumer) writeStatsToDB(batch []*models.QueueMessage) error {
	minuteBucket := make(map[time.Time]int)
	userMessagesMap := make(map[string]int)
	roomMessagesMap := make(map[string]int)

	for _, msg := range batch {
		ts, err := time.Parse(time.RFC3339Nano, msg.Timestamp)
		if err != nil {
			continue
		}
		minuteBucket[ts.Truncate(time.Minute)]++
		userMessagesMap[msg.Message.UserID]++
		roomMessagesMap[msg.RoomID]++
	}

	tx, err := c.DBConn.Begin(c.Ctx)
	if err != nil {
		log.Printf("[stats] begin tx: %s", err)
		return err
	}

	minuteBuckets, count := getKeysAndValues(minuteBucket)
	if _, err := tx.Exec(c.Ctx, `
		INSERT INTO message_stats (bucket, message_count)
		SELECT * FROM UNNEST($1::timestamptz[], $2::bigint[])
		ON CONFLICT (bucket) DO UPDATE
		SET message_count = message_stats.message_count + EXCLUDED.message_count`,
		minuteBuckets, count); err != nil {
		tx.Rollback(c.Ctx)
		log.Printf("[stats] message_stats: %s", err)
		return err
	}

	userIds, count := getKeysAndValues(userMessagesMap)
	if _, err := tx.Exec(c.Ctx, `
		INSERT INTO user_message_stats (user_id, message_count)
		SELECT * FROM UNNEST($1::text[], $2::bigint[])
		ON CONFLICT (user_id) DO UPDATE
		SET message_count = user_message_stats.message_count + EXCLUDED.message_count`,
		userIds, count); err != nil {
		tx.Rollback(c.Ctx)
		log.Printf("[stats] user_message_stats: %s", err)
		return err
	}

	roomIds, count := getKeysAndValues(roomMessagesMap)
	if _, err := tx.Exec(c.Ctx, `
		INSERT INTO room_message_stats (room_id, message_count)
		SELECT * FROM UNNEST($1::text[], $2::bigint[])
		ON CONFLICT (room_id) DO UPDATE
		SET message_count = room_message_stats.message_count + EXCLUDED.message_count`,
		roomIds, count); err != nil {
		tx.Rollback(c.Ctx)
		log.Printf("[stats] room_message_stats: %s", err)
		return err
	}

	if err := tx.Commit(c.Ctx); err != nil {
		log.Printf("[stats] commit: %s", err)
		return err
	}

	return nil
}

func (c *Consumer) ProcessStats() {
	ticker := time.NewTicker(time.Duration(c.flushInterval) * time.Millisecond)
	batch := make([]*models.QueueMessage, 0, c.bulkSize)

	for {
		select {
		case msg := <-c.StatsChan:
			batch = append(batch, msg)
		case <-ticker.C:
			if len(batch) > 0 {
				if err := c.writeStatsToDB(batch); err != nil {
					log.Printf("[stats] error writing stats to DB: %s", err)
				}
			}
			batch = batch[:0]
		case <-c.Ctx.Done():
			return
		}
	}
}

func (c *Consumer) Start() {
	for _, consumer := range c.RmqConsumers {
		go c.ConsumeFromRmq(consumer)
	}

	for range c.dbWorkers {
		go c.WriteToDB()
	}

	for range c.statsWorkers {
		go c.ProcessStats()
	}
}
