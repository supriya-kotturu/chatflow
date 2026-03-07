// Package internal implements the ChatFlow WebSocket server.
// It manages chat rooms, client connections, and message routing
// using a one-connection-per-user-per-room architecture.
package internal

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"supriyakotturu.github.com/chatflow/pkg/env"
	"supriyakotturu.github.com/chatflow/pkg/models"
	rmq "supriyakotturu.github.com/chatflow/pkg/rabbitmq"
)

// Stats tracks request success and failure counts using atomic counters.
type Stats struct {
	SuccessfulRequests atomic.Int64
	FailedRequests     atomic.Int64
}

// Client represents a single user's WebSocket connection within a room.
// Each client owns exactly one connection — no mutex is needed for writes.
type Client struct {
	Conn      *websocket.Conn
	Send      chan *models.Response
	Room      *Room
	UserID    string
	closeOnce sync.Once
}

// roomEvent is sent through Room.Broadcast for ordered processing by the
// broadcast goroutine. Either resp is set (fan-out to all users) or
// removeUserId is set (in-order user removal after all preceding messages).
type roomEvent struct {
	resp         *models.Response
	removeUserId string
}

// Room represents a chat room containing connected users.
type Room struct {
	ID               string
	Users            map[string]*Client
	Broadcast        chan roomEvent
	Ctx              context.Context
	Cancel           context.CancelFunc
	Mu               sync.RWMutex
	DroppedBroadcast atomic.Int64 // messages dropped because Broadcast channel was full
	DroppedSend      atomic.Int64 // messages dropped because a client's Send channel was full
}

// Server is the central hub that manages rooms, routes, and request stats.
type Server struct {
	ID         string
	Ctx        context.Context
	BufferSize int
	Mux        *http.ServeMux
	Rooms      map[string]*Room
	Rabbit     rmq.RabbitMQServer
	Mu         sync.RWMutex
	UserRooms  map[string][]string // userID → active roomIDs
	UserRoomMu sync.RWMutex
	maxRooms   int
	Stats
}

// ServerConfig is the config struct to create a ChatServerMux
type ServerConfig struct {
	Id         string
	Ctx        context.Context
	Rabbit     rmq.RabbitMQServer
	BufferSize int
	MaxRooms   int
}

// NewServerMux creates a Server with the given per-client send buffer size.
func NewServerMux(cf *ServerConfig) *Server {
	server := &Server{
		ID:         cf.Id,
		Ctx:        cf.Ctx,
		BufferSize: cf.BufferSize,
		Mux:        http.NewServeMux(),
		Rooms:      make(map[string]*Room),
		Mu:         sync.RWMutex{},
		UserRooms:  make(map[string][]string),
	}

	if cf.Rabbit != nil {
		server.Rabbit = cf.Rabbit
	}

	if cf.MaxRooms != 0 {
		server.maxRooms = cf.MaxRooms
	}

	return server
}

// RecordSuccess increments the successful request counter.
func (s *Server) RecordSuccess() {
	s.Stats.SuccessfulRequests.Add(1)
}

// RecordFailure increments the failed request counter.
func (s *Server) RecordFailure() {
	s.Stats.FailedRequests.Add(1)
}

// StartMonitoring logs throughput and health stats every interval.
// Call this before Start(); it runs in a background goroutine.
func (s *Server) StartMonitoring(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		var prevSuccess, prevFail int64
		prevTime := time.Now()

		for {
			select {
			case <-ticker.C:
				now := time.Now()
				elapsed := now.Sub(prevTime).Seconds()

				success := s.Stats.SuccessfulRequests.Load()
				fail := s.Stats.FailedRequests.Load()

				successRate := float64(success-prevSuccess) / elapsed
				failRate := float64(fail-prevFail) / elapsed

				s.Mu.RLock()
				activeRooms := len(s.Rooms)
				activeConns := 0
				for _, r := range s.Rooms {
					r.Mu.RLock()
					activeConns += len(r.Users)
					r.Mu.RUnlock()
				}
				s.Mu.RUnlock()

				if s.Rabbit != nil {
					dropped := s.Rabbit.DroppedMessages()

					log.Printf("[monitor] success=%.0f/s fail=%.0f/s | rooms=%d conns=%d | total: ok=%d err=%d dropped=%d",
						successRate, failRate, activeRooms, activeConns, success, fail, dropped)
				} else {
					log.Printf("[monitor] success=%.0f/s fail=%.0f/s | rooms=%d conns=%d | total: ok=%d err=%d",
						successRate, failRate, activeRooms, activeConns, success, fail)
				}

				prevSuccess = success
				prevFail = fail
				prevTime = now

			case <-ctx.Done():
				return
			}
		}
	}()
}

// Start registers HTTP routes and begins listening on the configured port.
func (s *Server) Start() {
	e, err := env.LoadServerEnv()
	if err != nil {
		fmt.Printf("Error loading the .env file: %+v\n", err)
		os.Exit(1)
	}
	var addr = flag.String("addr", ":"+e.Port, "http service address")

	// Serve static files
	fileServer := http.FileServer(http.Dir("server/html/"))
	s.Mux.Handle("/static/", http.StripPrefix("/static/", fileServer))

	s.Mux.HandleFunc("GET /{$}", s.HomeHandler)
	s.Mux.HandleFunc("/health", s.HealthHandler)
	s.Mux.HandleFunc("GET /metrics", s.MetricsHandler)
	s.Mux.HandleFunc("/chat/{roomId}", s.ChatRoomHandler)

	s.Mux.HandleFunc("GET /chat-room/{roomId}", s.ChatRoomPageHandler)

	httpServer := &http.Server{Addr: *addr, Handler: s.Mux}

	go func() {
		<-s.Ctx.Done()

		// Force-close all WebSocket connections immediately.
		// httpServer.Shutdown skips hijacked connections (WebSockets), so
		// ReadJSON calls never unblock and handler goroutines never exit.
		// Closing each conn here unblocks ReadJSON across all rooms at once.
		s.Mu.RLock()
		for _, room := range s.Rooms {
			room.Mu.RLock()
			for _, user := range room.Users {
				user.Conn.Close()
			}
			room.Mu.RUnlock()
		}
		s.Mu.RUnlock()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		httpServer.Shutdown(shutdownCtx)
	}()

	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Printf("Error starting the server: %+v\n", err)
	}
}

// AddNewRoom creates a room and starts its infrastructure: a RabbitMQ consumer
// for cross-server messages and a broadcast goroutine that fans out to local clients.
// It is a no-op if the room already exists.
func (s *Server) AddNewRoom(roomId string) {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	if _, exists := s.Rooms[roomId]; exists {
		return
	}

	ctx, cancel := context.WithCancel(s.Ctx)
	room := &Room{
		ID:        roomId,
		Users:     make(map[string]*Client),
		Broadcast: make(chan roomEvent, s.BufferSize),
		Ctx:       ctx,
		Cancel:    cancel,
	}

	// consumeMessages handles messages published by other servers.
	// It skips messages originating from this server (self-filter) and
	// forwards the rest into the room's broadcast channel for local fan-out.
	consumeMessages := func(msg []byte) {
		var queueMsg models.QueueMessage

		if err := json.Unmarshal(msg, &queueMsg); err != nil {
			log.Printf("Error un-marshalling message from RabbitMQ: %+v\n", err)
			return
		}

		if queueMsg.ServerID == s.ID {
			return
		}

		message := models.NewResponse(queueMsg.Message)
		message.ServerTimestamp = queueMsg.Timestamp
		select {
		case room.Broadcast <- roomEvent{resp: message}:
		default:
			log.Printf("Dropping message for room [%s] due to full broadcast channel\n", roomId)
		}
	}

	if s.Rabbit != nil {
		if err := s.Rabbit.Consume(s.Ctx, roomId, consumeMessages); err != nil {
			log.Printf("Error consuming messages for room [%s]: %+v\n", roomId, err)
		}
	}

	// broadcast goroutine processes events from Broadcast in FIFO order.
	// - roomEvent{resp}: fans out the message to all local clients.
	// - roomEvent{removeUserId}: closes that client's Send channel and removes
	//   them from Users. Because both events share the same channel, the removal
	//   is guaranteed to happen only after all preceding messages have been
	//   delivered — fixing the teardown race where M999/M1000 would be skipped.
	// On room shutdown (ctx cancel), it closes all remaining client Send channels.
	go func(r *Room) {
		for {
			select {
			case event, ok := <-r.Broadcast:
				if !ok {
					log.Println("Unable to read the message from Broadcast channel.")
					return
				}
				if event.removeUserId != "" {
					// In-order removal: all messages preceding this event have
					// already been fanned out, so it is safe to close Send now.
					r.Mu.Lock()
					userId := event.removeUserId
					if client, exists := r.Users[userId]; exists {
						client.closeOnce.Do(func() { close(client.Send) })
						delete(r.Users, userId)
						log.Printf("User %s left room %s", userId, r.ID)
						r.Mu.Unlock()
						s.removeFromUserRooms(userId, r.ID)
					} else {
						r.Mu.Unlock()
					}
				} else {
					r.Mu.RLock()
					for _, u := range r.Users {
						select {
						case u.Send <- event.resp:
						default:
							r.DroppedSend.Add(1)
							log.Printf("Dropping message for user [%s] in room [%s] due to full send channel\n", u.UserID, r.ID)
						}
					}
					r.Mu.RUnlock()
				}

			case <-r.Ctx.Done():
				log.Println("Room context cancelled, closing client connection")

				// Close WebSocket connections first so ReadJSON in the handler
				// goroutines unblocks and they can exit cleanly.
				r.Mu.Lock()
				for _, user := range r.Users {
					user.Conn.Close()
					user.closeOnce.Do(func() { close(user.Send) })
				}
				r.Mu.Unlock()
				return
			}
		}
	}(room)

	s.Rooms[roomId] = room
}

// NewClient creates a Client with a buffered send channel.
func NewClient(userId string, room *Room, conn *websocket.Conn, bufferSize int) *Client {
	return &Client{
		UserID: userId,
		Conn:   conn,
		Send:   make(chan *models.Response, bufferSize),
		Room:   room,
	}
}

// AddUserToRoom registers a user in a room, creating the room if needed.
// Returns an error if the user is already present.
func (s *Server) AddUserToRoom(userId string, roomId string, conn *websocket.Conn) (*Client, error) {
	s.Mu.RLock()
	room, exists := s.Rooms[roomId]
	s.Mu.RUnlock()

	if !exists {
		s.AddNewRoom(roomId)
		s.Mu.RLock()
		room = s.Rooms[roomId]
		s.Mu.RUnlock()
	}

	room.Mu.Lock()
	defer room.Mu.Unlock()

	if _, userExists := room.Users[userId]; userExists {
		return nil, fmt.Errorf("user %s already exists in room %s", userId, roomId)
	}

	newClient := NewClient(userId, room, conn, s.BufferSize)
	room.Users[userId] = newClient

	s.UserRoomMu.Lock()
	s.UserRooms[userId] = append(s.UserRooms[userId], roomId)
	s.UserRoomMu.Unlock()

	return newClient, nil

}

// removeFromUserRooms removes roomId from the UserRooms index for userId.
// Called while holding no other locks.
func (s *Server) removeFromUserRooms(userId, roomId string) {
	s.UserRoomMu.Lock()
	defer s.UserRoomMu.Unlock()
	rooms := s.UserRooms[userId]
	for i, r := range rooms {
		if r == roomId {
			s.UserRooms[userId] = append(rooms[:i], rooms[i+1:]...)
			break
		}
	}
	if len(s.UserRooms[userId]) == 0 {
		delete(s.UserRooms, userId)
	}
}

// RemoveUserFromRoom removes a user from a room and closes its send channel.
// Use scheduleRemoveUser for the clean-leave path so that in-flight broadcast
// messages are delivered before the user is removed.
func (s *Server) RemoveUserFromRoom(userId string, roomId string) error {
	s.Mu.RLock()
	room, exists := s.Rooms[roomId]
	s.Mu.RUnlock()

	if !exists {
		return fmt.Errorf("room %s doesn't exist", roomId)
	}

	room.Mu.Lock()
	defer room.Mu.Unlock()

	client, userExists := room.Users[userId]

	if !userExists {
		return fmt.Errorf("user %s doesn't exist in room %s", userId, roomId)
	}

	client.closeOnce.Do(func() { close(client.Send) })
	delete(room.Users, userId)
	log.Printf("User %s left room %s", userId, roomId)
	s.removeFromUserRooms(userId, roomId)
	return nil
}
