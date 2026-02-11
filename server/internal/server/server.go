package internal

import (
	"context"
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
)

type Stats struct {
	SuccessfulRequests atomic.Int64
	FailedRequests     atomic.Int64
}

type Client struct {
	Conn   *websocket.Conn
	Send   chan *models.Response
	Room   *Room
	UserId string
}

type Room struct {
	ID        string
	Users     map[string]*Client
	Broadcast chan []byte
	Ctx       context.Context
	Cancel    context.CancelFunc
	Mu        sync.RWMutex
}

type Server struct {
	BufferSize int
	Mux        *http.ServeMux
	Rooms      map[string]*Room
	Mu         sync.RWMutex
	Stats
}

func NewServerMux(bufferSize int) *Server {
	return &Server{
		BufferSize: bufferSize,
		Mux:        http.NewServeMux(),
		Rooms:      make(map[string]*Room),
		Mu:         sync.RWMutex{},
	}
}

func (s *Server) RecordSuccess() {
	s.Stats.SuccessfulRequests.Add(1)
}

func (s *Server) RecordFailure() {
	s.Stats.FailedRequests.Add(1)
}

func (s *Server) Start() {
	envConfig, err := env.LoadEnv()
	if err != nil {
		fmt.Printf("Error loading the .env file: %+v\n", err)
		os.Exit(1)
	}
	var addr = flag.String("addr", ":"+envConfig.Port, "http service address")

	// Serve static files
	fileServer := http.FileServer(http.Dir("server/html/"))
	s.Mux.Handle("/static/", http.StripPrefix("/static/", fileServer))

	s.Mux.HandleFunc("GET /{$}", s.HomeHandler)
	s.Mux.HandleFunc("/health", s.HealthHandler)
	s.Mux.HandleFunc("/chat/{roomId}", s.ChatRoomHandler)

	s.Mux.HandleFunc("GET /chat/{roomId}", s.ChatRoomPageHandler)

	if err := http.ListenAndServe(*addr, s.Mux); err != nil {
		fmt.Printf("Error starting the server: %+v\n", err)
	}
}

func (s *Server) AddNewRoom(roomId string) {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	if _, exists := s.Rooms[roomId]; exists {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.Rooms[roomId] = &Room{
		ID:        roomId,
		Users:     make(map[string]*Client),
		Broadcast: make(chan []byte),
		Ctx:       ctx,
		Cancel:    cancel,
	}
}

func NewClient(userId string, room *Room, conn *websocket.Conn, bufferSize int) *Client {
	return &Client{
		UserId: userId,
		Conn:   conn,
		Send:   make(chan *models.Response, bufferSize),
		Room:   room,
	}
}

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
	close(client.Send)
	delete(room.Users, userId)
	log.Printf("User %s left room %s", userId, roomId)
	return nil

}
