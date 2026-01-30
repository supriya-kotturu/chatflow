package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"

	"supriyakotturu.github.com/chatflow/pkg/env"
	"supriyakotturu.github.com/chatflow/server/handlers"
)

func main() {
	envConfig, err := env.LoadEnv()
	if err != nil {
		fmt.Printf("Error loading the .env file: %+v\n", err)
		os.Exit(1)
	}
	var addr = flag.String("addr", ":"+envConfig.Port, "http service address")

	mux := http.NewServeMux()

	// Serve static files
	fileServer := http.FileServer(http.Dir("server/html/"))
	mux.Handle("/static/", http.StripPrefix("/static/", fileServer))

	mux.HandleFunc("GET /{$}", handlers.HomeHandler)
	mux.HandleFunc("/health", handlers.HealthHandler)
	mux.HandleFunc("/chat/{roomId}", handlers.ChatRoomHandler)

	// mux.HandleFunc("GET /chat/{roomId}", handlers.ChatRoomPageHandler)

	if err := http.ListenAndServe(*addr, mux); err != nil {
		fmt.Printf("Error starting the server: %+v\n", err)
	}
}
