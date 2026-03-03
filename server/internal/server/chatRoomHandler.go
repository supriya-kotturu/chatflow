package internal

import (
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"supriyakotturu.github.com/chatflow/pkg/models"
)

// ChatRoomHandler upgrades an HTTP request to a WebSocket connection and
// manages a user's lifecycle in a chat room: JOIN, TEXT messages, and LEAVE.
func (s *Server) ChatRoomHandler(w http.ResponseWriter, r *http.Request) {
	log.Printf("ChatRoomHandler called for path: %s", r.URL.Path)
	clientIp := r.RemoteAddr
	conn, err := WsUpgrader.Upgrade(w, r, nil)
	roomId := r.PathValue("roomId")

	if err != nil {
		s.RecordFailure()
		log.Printf("Failed to set websocket upgrade: %+v\n", err)
		return
	}
	defer conn.Close()

	// Read JOIN message
	var joinMsg models.Message
	if err := conn.ReadJSON(&joinMsg); err != nil {
		s.RecordFailure()
		log.Printf("Error reading JOIN message: %+v", err)
		return
	}

	if joinMsg.MessageType != models.MessageTypeJoin {
		s.RecordFailure()
		log.Printf("First message must be JOIN, got %s", joinMsg.MessageType)
		return
	}

	// Register User in Room
	userId := joinMsg.UserID
	client, err := s.AddUserToRoom(userId, roomId, conn)
	if err != nil {
		s.RecordFailure()
		log.Printf("Error adding user [%s] to room [%s]: %+v", userId, roomId, err)
		return
	}

	// Start writer goroutine
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		s.handleClientWrites(client)
	}()

	// Echo JOIN response
	resp := models.NewResponse(joinMsg)

	s.broadcastAndPublish(client, resp, clientIp)
	s.RecordSuccess()

	// Read Loop
	for {
		var msg models.Message
		if err := conn.ReadJSON(&msg); err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("Client %s disconnected from room %s", userId, roomId)
			} else {
				s.RecordFailure()
				log.Printf("Error reading from client %s: %v", userId, err)
			}

			// Synthetic LEAVE message to notify other users when the connection is dropped.
			msg := models.NewMessage(userId, resp.Username, "", models.MessageTypeLeave)
			resp := models.NewResponse(*msg)
			resp.ServerTimestamp = time.Now().UTC().Format(time.RFC3339Nano)

			s.broadcastAndPublish(client, resp, clientIp)
			// Schedule in-order removal: the remove event is enqueued after the
			// synthetic LEAVE in Broadcast, so other users see the LEAVE first.
			s.scheduleRemoveUser(client)
			break
		}

		resp := models.NewResponse(msg)

		switch msg.MessageType {
		case models.MessageTypeJoin:
			// Ignore subsequent JOIN messages
		case models.MessageTypeText:
			s.broadcastAndPublish(client, resp, clientIp)
			s.RecordSuccess()
		case models.MessageTypeLeave:
			s.broadcastAndPublish(client, resp, clientIp)
			// Schedule in-order removal: the remove event sits after the LEAVE
			// response in Broadcast. The broadcast goroutine will fan out M999,
			// M1000, and the LEAVE before closing this client's Send channel.
			s.scheduleRemoveUser(client)
			s.RecordSuccess()
			<-writeDone
			return
		default:
			log.Printf("Unexpected message type %s from user %s", msg.MessageType, userId)
		}
	}
	<-writeDone
}

// handleClientWrites drains the client's send channel and writes responses
// to the WebSocket connection. It exits when the channel is closed or the
// room context is cancelled.
func (s *Server) handleClientWrites(client *Client) {
	for {
		select {
		case resp, ok := <-client.Send:
			if !ok {
				// Channel closed, exit goroutine
				return
			}
			if err := client.Conn.WriteJSON(resp); err != nil {
				log.Println("error writing to client:", err)
				return
			}
		case <-client.Room.Ctx.Done():
			log.Println("Room context cancelled, closing client connection")
			return
		}
	}
}

// broadcastAndPublish fans out resp to all local room users via the broadcast
// channel and publishes a QueueMessage to RabbitMQ for cross-server delivery.
// The broadcast send is non-blocking; messages are dropped if the channel is full.
func (s *Server) broadcastAndPublish(client *Client, resp *models.Response, clientIp string) {
	select {
	case client.Room.Broadcast <- roomEvent{resp: resp}:
	default:
		log.Printf("Dropping message for room [%s] due to full broadcast channel\n", client.Room.ID)
	}

	id := uuid.NewString()
	qMessage := &models.QueueMessage{
		MessageID:   id,
		RoomID:      client.Room.ID,
		Username:    resp.Username,
		Message:     resp.Message,
		Timestamp:   time.Now().UTC().Format(time.RFC3339Nano),
		MessageType: resp.MessageType,
		ServerID:    s.Rabbit.ServerID(),
		ClientIp:    clientIp,
	}

	if err := s.Rabbit.Publish(client.Room.ID, qMessage); err != nil {
		log.Printf("Error publishing message [%s] to RabbitMQ: %v", id, err)
	}
}

// scheduleRemoveUser enqueues a user-removal event into the room's Broadcast
// channel. Because Broadcast is FIFO, the removal is guaranteed to be processed
// only after all messages already queued ahead of it — eliminating the race
// where the last few messages are skipped for the leaving user.
// Falls back to direct removal if the channel is unexpectedly full.
func (s *Server) scheduleRemoveUser(client *Client) {
	select {
	case client.Room.Broadcast <- roomEvent{removeUserId: client.UserID}:
	default:
		log.Printf("Broadcast full during leave for user %s, removing directly", client.UserID)
		s.RemoveUserFromRoom(client.UserID, client.Room.ID)
	}
}
