package client

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"sync"

	"github.com/gorilla/websocket"
	"supriyakotturu.github.com/chatflow/pkg/models"
)

type WsClient struct {
	ChatRoomId string
	Conn       *websocket.Conn
	Send       chan *models.Response
	WriteMu    sync.Mutex
	OwnerUser  string
}

func NewWsClient(messageBuffer int, port string, roomId string) (*WsClient, error) {
	chatRoomUrl := url.URL{Scheme: "ws", Host: "localhost:" + port, Path: "/chat/" + roomId}
	dailer := websocket.DefaultDialer
	conn, resp, err := dailer.Dial(chatRoomUrl.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w (resp: %+v)", chatRoomUrl.String(), err, resp)
	}

	c := &WsClient{
		ChatRoomId: roomId,
		Conn:       conn,
		Send:       make(chan *models.Response, messageBuffer),
	}

	go c.Read()

	return c, nil
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
		var msg models.Response
		if err := c.Conn.ReadJSON(&msg); err != nil {
			// avoid logging interruption errors (e.g using ctrl+c to terminate the client)
			// 2026/01/30 01:31:54 read error: read tcp [::1]:56499->[::1]:3000: use of closed network connection
			if errors.Is(err, net.ErrClosed) || websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				return
			}
			// log.Println("read error: ", err)
			return
		}
		select {
		case c.Send <- &msg:
		default:
		}
	}
}
