package internal

import (
	"log"
	"net/http"

	"github.com/gorilla/websocket"
	"supriyakotturu.github.com/chatflow/pkg/models"
)

// ChatRoomHandler upgrades an HTTP request to a WebSocket connection and
// manages a user's lifecycle in a chat room: JOIN, TEXT messages, and LEAVE.
func (s *Server) ChatRoomHandler(w http.ResponseWriter, r *http.Request) {
	log.Printf("ChatRoomHandler called for path: %s", r.URL.Path)
	conn, err := WsUpgrader.Upgrade(w, r, nil)
	roomId := r.PathValue("roomId")

	if err != nil {
		s.RecordFailure()
		log.Printf("Failed to set websocket upgrade: %+v\n", err)
		return
	}
	defer conn.Close()

	// Send JOIN message
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
	userId := joinMsg.UserId
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
	client.Send <- models.NewResponse(joinMsg)
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
			s.RemoveUserFromRoom(userId, roomId)
			break
		}

		resp := models.NewResponse(msg)

		switch msg.MessageType {
		case models.MessageTypeText:
			select {
			case client.Send <- resp:
			default:
				log.Printf("Send buffer full for user %s in room %s", userId, roomId)
			}
			s.RecordSuccess()
		case models.MessageTypeLeave:
			client.Send <- resp
			s.RemoveUserFromRoom(userId, roomId)
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
