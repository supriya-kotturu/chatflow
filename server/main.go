package main

import server "supriyakotturu.github.com/chatflow/server/internal/server"

func main() {
	s := server.NewServerMux()
	s.Start()
}
