package client

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"sync"

	"github.com/gorilla/websocket"
	"supriyakotturu.github.com/chatflow/pkg/generate"
	"supriyakotturu.github.com/chatflow/pkg/models"
)

type WsClient struct {
	ChatRoomId string
	Conn       *websocket.Conn
	WriteCh    chan models.Message
	ReadCh     chan models.Message
	WriteMu    sync.Mutex
}

func NewWsClient(port string) (*WsClient, error) {
	chatRoomId := generate.ChatRoomId()
	chatRoomUrl := url.URL{Scheme: "ws", Host: "localhost:" + port, Path: "/chat/" + chatRoomId}
	dailer := websocket.DefaultDialer
	conn, resp, err := dailer.Dial(chatRoomUrl.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w (resp: %+v)", chatRoomUrl.String(), err, resp)
	}

	c := &WsClient{
		ChatRoomId: chatRoomId,
		Conn:       conn,
		WriteCh:    make(chan models.Message, 16),
		ReadCh:     make(chan models.Message, 16),
	}

	go c.Read()

	return c, nil
}

func (c *WsClient) Reset() {
	c.WriteCh = make(chan models.Message, 16)
}

func (c *WsClient) Close() error {
	if c == nil || c.Conn == nil {
		return nil
	}
	return c.Conn.Close()
}

func (c *WsClient) Write(m *models.Message) error {
	c.WriteMu.Lock()
	defer c.WriteMu.Unlock()
	return c.Conn.WriteJSON(m)
}

func (c *WsClient) Read() {
	for {
		var msg models.Message
		if err := c.Conn.ReadJSON(&msg); err != nil {
			// avoid logging interruption errors (e.g using ctrl+c to terminate the client)
			// 2026/01/30 01:31:54 read error: read tcp [::1]:56499->[::1]:3000: use of closed network connection
			if errors.Is(err, net.ErrClosed) || websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				return
			}
			log.Println("read error: ", err)
			return
		}
		select {
		case c.ReadCh <- msg:
		default:
		}
	}
}
