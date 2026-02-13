// Package env loads environment configuration from .env files.
package env

import (
	"os"
	"strings"

	"github.com/lpernett/godotenv"
)

// Env holds application configuration loaded from environment variables.
type Env struct {
	Name       string
	Port       string
	ServerHost string
}

// LoadEnv reads the .env file (parent dir first, then cwd) and returns
// the parsed configuration.
func LoadEnv() (*Env, error) {
	err := godotenv.Load("../.env")
	if err != nil {
		// Try current directory as fallback
		err = godotenv.Load()
		if err != nil {
			return nil, err
		}
	}

	host := os.Getenv("SERVER_HOST")
	port := os.Getenv("PORT")

	host = strings.TrimSpace(host)
	host = strings.TrimPrefix(host, "http://")
	host = strings.TrimPrefix(host, "https://")
	host = strings.TrimSuffix(host, "/")

	port = strings.TrimSpace(port)
	port = strings.TrimPrefix(port, ":")

	return &Env{
		Name:       os.Getenv("NAME"),
		ServerHost: host,
		Port:       port,
	}, nil
}
