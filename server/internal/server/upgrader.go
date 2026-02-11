package internal

import (
	"net/http"

	"github.com/gorilla/websocket"
)

// BufferSize is the read/write buffer size for WebSocket connections.
var BUFFER_SIZE = 4096

// WsUpgrader is the default WebSocket upgrader used by all handlers.
var WsUpgrader = websocket.Upgrader{
	ReadBufferSize:    BUFFER_SIZE,
	WriteBufferSize:   BUFFER_SIZE,
	EnableCompression: true,
	CheckOrigin:       func(r *http.Request) bool { return true },
}
