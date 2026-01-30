package handlers

import (
	"net/http"

	"github.com/gorilla/websocket"
)

var BUFFER_SIZE = 4096

var WsUpgrader = websocket.Upgrader{
	ReadBufferSize:    BUFFER_SIZE,
	WriteBufferSize:   BUFFER_SIZE,
	EnableCompression: true,
	CheckOrigin:       func(r *http.Request) bool { return true },
}
