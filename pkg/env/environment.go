package env

import (
	"os"

	"github.com/lpernett/godotenv"
)

type Env struct {
	Name string
	Port string
}

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
		Name: os.Getenv("NAME"),
		Port: os.Getenv("PORT"),
	}, nil
}
