// Command server starts the ChatFlow WebSocket server.
package main

import server "supriyakotturu.github.com/chatflow/server/internal/server"

func main() {
	bufferSize := 2048
	s := server.NewServerMux(bufferSize)
	s.Start()
}
