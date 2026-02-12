// Package env loads environment configuration from .env files.
package env

import (
	"os"

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

	return &Env{
		Name:       os.Getenv("NAME"),
		Port:       os.Getenv("PORT"),
		ServerHost: os.Getenv("SERVER_HOST"),
	}, nil
}
