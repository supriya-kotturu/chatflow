package main

import (
	"supriyakotturu.github.com/chatflow/client"
	clientWs "supriyakotturu.github.com/chatflow/client/ws"
)

func main() {
	config := &clientWs.ClientConfig{
		PoolSize:      320, // total concurrent sessions: 32 * 10 [Allows one user to parallelly join 10 rooms]
		UserCount:     32,
		MessageCount:  1000,
		RoomCount:     10,
		MessageBuffer: 1250,
	}

	client.RunFanOutLoadTest(config) // 29.6655396s
}
