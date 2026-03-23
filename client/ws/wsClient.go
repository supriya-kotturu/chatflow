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
	Retries    int
	closeOnce  sync.Once
}

// NewWsClient dials a WebSocket connection to the given room with exponential
// backoff retry and starts a background reader goroutine.
func NewWsClient(messageBuffer int, host string, port string, roomId string) (*WsClient, error) {
	addr := net.JoinHostPort(host, port)
	chatRoomPath := "/chat/"
	chatRoomUrl := url.URL{Scheme: "ws", Host: addr, Path: chatRoomPath + roomId}
	dialer := websocket.DefaultDialer

	maxRetries := 5
	backoff := 1 * time.Second
	var conn *websocket.Conn
	var err error
	retries := 0

	for attempt := 0; attempt < maxRetries; attempt++ {
		conn, _, err = dialer.Dial(chatRoomUrl.String(), nil)
		if err == nil {
			retries = attempt
			break
		}
		retries = attempt + 1
		fmt.Printf("attempt %d failed dialing room[%s]: %v, retrying in %v...\n", retries, roomId, err, backoff)
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
		Retries:    retries,
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

// CloseSend safely closes the Send channel exactly once.
func (c *WsClient) CloseSend() {
	c.closeOnce.Do(func() { close(c.Send) })
}

// Read continuously reads responses from the server and forwards them to Send.
// It closes Send when the connection is lost so readers know no more data is coming.
func (c *WsClient) Read() {
	defer c.CloseSend()
	for {
		var msg models.Response
		if err := c.Conn.ReadJSON(&msg); err != nil {
			if errors.Is(err, net.ErrClosed) || websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				return
			}
			return
		}
		c.Send <- &msg
	}
}
