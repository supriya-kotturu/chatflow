// Package env loads environment configuration from .env files.
package env

import (
	"os"
	"strconv"
	"strings"

	"github.com/lpernett/godotenv"
)

// ServerEnv holds application configuration loaded from environment variables for Chat Server.
type ServerEnv struct {
	Name       string
	Port       string
	ServerHost string
	RabbitEnv
}

// RabbitEnv holds application configuration loaded from environment variables for RabbitMQ server.
type RabbitEnv struct {
	RabbitHost     string
	RabbitPort     string
	RabbitUser     string
	RabbitPassword string
	RoomCount      int
}

// atoiWithDefault converts a string to an integer, returning a default value if conversion fails.
func atoiWithDefault(s string, defaultValue int) int {
	if val, err := strconv.Atoi(s); err == nil {
		return val
	}
	return defaultValue
}

// LoadEnvFromFile reads the .env file (parent dir first, then cwd) and returns
// the parsed configuration.
func LoadEnvFromFile() error {
	err := godotenv.Load("../.env")
	if err != nil {
		// Try current directory as fallback
		return godotenv.Load()
	}

	return err
}

// LoadServerEnv reads the .env file (parent dir first, then cwd) and returns
// the parsed configuration for Chat server.
func LoadServerEnv() (*ServerEnv, error) {
	rabbit, err := LoadRabbitEnv()

	if err != nil {
		return nil, err
	}

	host := os.Getenv("SERVER_HOST")
	port := os.Getenv("PORT")

	host = strings.TrimSpace(host)
	host = strings.TrimPrefix(host, "http://")
	host = strings.TrimPrefix(host, "https://")
	host = strings.TrimSuffix(host, "/")

	port = strings.TrimSpace(port)
	port = strings.TrimPrefix(port, ":")

	return &ServerEnv{
		Name:       os.Getenv("NAME"),
		ServerHost: host,
		Port:       port,
		RabbitEnv:  *rabbit,
	}, nil
}

// LoadRabbitEnv reads the .env file (parent dir first, then cwd) and returns
// the parsed configuration for RabbitMQ server.
func LoadRabbitEnv() (*RabbitEnv, error) {
	err := LoadEnvFromFile()

	if err != nil {
		return nil, err
	}

	rabbitHost := os.Getenv("RABBIT_HOST")
	rabbitPort := os.Getenv("RABBIT_PORT")
	rabbitUser := os.Getenv("RABBIT_USER")
	rabbitPassword := os.Getenv("RABBIT_PASSWORD")
	roomCount := os.Getenv("ROOM_COUNT")

	return &RabbitEnv{
		RabbitHost:     rabbitHost,
		RabbitPort:     rabbitPort,
		RabbitUser:     rabbitUser,
		RabbitPassword: rabbitPassword,
		RoomCount:      atoiWithDefault(roomCount, 20),
	}, nil
}
