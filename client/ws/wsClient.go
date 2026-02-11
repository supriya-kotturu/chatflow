package client

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"supriyakotturu.github.com/chatflow/pkg/models"
)

// WsClient wraps a single WebSocket connection to a chat room.
// It owns a reader goroutine that pushes responses into Send.
type WsClient struct {
	ChatRoomId string
	Conn       *websocket.Conn
	Send       chan *models.Response
	WriteMu    sync.Mutex
	OwnerUser  string
}

// NewWsClient dials a WebSocket connection to the given room with exponential
// backoff retry and starts a background reader goroutine.
func NewWsClient(messageBuffer int, port string, roomId string) (*WsClient, error) {
	chatRoomUrl := url.URL{Scheme: "ws", Host: "localhost:" + port, Path: "/chat/" + roomId}
	dailer := websocket.DefaultDialer

	maxRetries := 5
	backoff := 1 * time.Second
	var conn *websocket.Conn
	var err error

	for attempt := range maxRetries {
		conn, _, err = dailer.Dial(chatRoomUrl.String(), nil)
		if err == nil {
			break
		}
		fmt.Printf("attempt %d failed: %v, retrying in %v...\n", attempt+1, err, backoff)
		time.Sleep(backoff)
		backoff *= 2
	}

	if err != nil {
		return nil, fmt.Errorf("dial %s after %d attempts: %w", chatRoomUrl.String(), maxRetries, err)
	}

	c := &WsClient{
		ChatRoomId: roomId,
		Conn:       conn,
		Send:       make(chan *models.Response, messageBuffer),
	}

	go c.Read()

	return c, nil
}

// Close closes the underlying WebSocket connection.
func (c *WsClient) Close() error {
	if c == nil || c.Conn == nil {
		return nil
	}
	return c.Conn.Close()
}

// Write sends a message as JSON over the WebSocket connection.
func (c *WsClient) Write(m *models.Message) error {
	c.WriteMu.Lock()
	defer c.WriteMu.Unlock()
	return c.Conn.WriteJSON(m)
}

// Read continuously reads responses from the server and forwards them to Send.
// It silently exits on connection close or network errors.
func (c *WsClient) Read() {
	for {
		var msg models.Response
		if err := c.Conn.ReadJSON(&msg); err != nil {
			// to avoid logging interruption errors (e.g using ctrl+c to terminate the client)
			// ERR: 2026/01/30 01:31:54 read error: read tcp [::1]:56499->[::1]:3000: use of closed network connection
			if errors.Is(err, net.ErrClosed) || websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				return
			}
			return
		}
		select {
		case c.Send <- &msg:
		default:
		}
	}
}
