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

// Room represents a chat room containing connected users.
type Room struct {
	ID        string
	Users     map[string]*Client
	Broadcast chan *models.Response
	Ctx       context.Context
	Cancel    context.CancelFunc
	Mu        sync.RWMutex
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
	return &Server{
		ID:         cf.Id,
		Ctx:        cf.Ctx,
		Rabbit:     cf.Rabbit,
		BufferSize: cf.BufferSize,
		Mux:        http.NewServeMux(),
		Rooms:      make(map[string]*Room),
		Mu:         sync.RWMutex{},
		maxRooms:   cf.MaxRooms,
	}
}

// RecordSuccess increments the successful request counter.
func (s *Server) RecordSuccess() {
	s.Stats.SuccessfulRequests.Add(1)
}

// RecordFailure increments the failed request counter.
func (s *Server) RecordFailure() {
	s.Stats.FailedRequests.Add(1)
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
	s.Mux.HandleFunc("/chat/{roomId}", s.ChatRoomHandler)

	s.Mux.HandleFunc("GET /chat-room/{roomId}", s.ChatRoomPageHandler)

	if err := http.ListenAndServe(*addr, s.Mux); err != nil {
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

	ctx, cancel := context.WithCancel(context.Background())
	room := &Room{
		ID:        roomId,
		Users:     make(map[string]*Client),
		Broadcast: make(chan *models.Response, s.BufferSize*s.maxRooms),
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
		case room.Broadcast <- message:
		default:
			log.Printf("Dropping message for room [%s] due to full broadcast channel\n", roomId)
		}
	}

	if err := s.Rabbit.Consume(s.Ctx, roomId, consumeMessages); err != nil {
		log.Printf("Error consuming messages for room [%s]: %+v\n", roomId, err)
	}

	// broadcast goroutine fans out each message to a snapshot of connected clients.
	// On room shutdown (ctx cancel), it closes all client Send channels.
	go func(r *Room) {
		for {
			sendChans := make([]chan *models.Response, 0)
			select {
			case resp, ok := <-r.Broadcast:
				if !ok {
					log.Println("Unable to read the message from Broadcast channel.")
					return
				}
				r.Mu.RLock()
				for _, u := range r.Users {
					sendChans = append(sendChans, u.Send)
				}
				r.Mu.RUnlock()

				// Broadcast the response to all the users in the room.
				for _, conn := range sendChans {
					select {
					case conn <- resp:
					default:
					}
				}

			case <-r.Ctx.Done():
				log.Println("Room context cancelled, closing client connection")

				// Close the user channels if the Room's ctx is cancelled.
				r.Mu.Lock()
				for _, user := range r.Users {
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
	return newClient, nil

}

// RemoveUserFromRoom removes a user from a room and closes its send channel.
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

	// Close the Send channel when user explicitly leaves
	client.closeOnce.Do(func() { close(client.Send) })
	delete(room.Users, userId)
	log.Printf("User %s left room %s", userId, roomId)
	return nil

}
