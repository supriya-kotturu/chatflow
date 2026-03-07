// Package env loads environment configuration from .env files.
package env

import (
	"fmt"
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
	PublishWorkers int
}

// atoiWithDefault converts a string to an integer, returning a default value if conversion fails.
func atoiWithDefault(s string, defaultValue int) int {
	if val, err := strconv.Atoi(s); err == nil {
		return val
	}
	return defaultValue
}

// LoadEnvFromFile loads environment configuration.
// Resolution order (first found wins):
//  1. .env.local  — local overrides, never deployed to EC2
//  2. .env        — production values, deployed to EC2
//
// Both are tried in the parent directory first, then cwd.
func LoadEnvFromFile() error {
	for _, name := range []string{".env.local", ".env"} {
		if err := godotenv.Load("../" + name); err == nil {
			return nil
		}
		if err := godotenv.Load(name); err == nil {
			return nil
		}
	}
	return fmt.Errorf("no .env or .env.local file found")
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
	publishWorkers := os.Getenv("PUBLISH_WORKERS")

	return &RabbitEnv{
		RabbitHost:     rabbitHost,
		RabbitPort:     rabbitPort,
		RabbitUser:     rabbitUser,
		RabbitPassword: rabbitPassword,
		RoomCount:      atoiWithDefault(roomCount, 20),
		PublishWorkers: atoiWithDefault(publishWorkers, 30),
	}, nil
}
