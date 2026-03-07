package main

import (
	"supriyakotturu.github.com/chatflow/client"
	ws "supriyakotturu.github.com/chatflow/client/ws"
)

func main() {
	config := &ws.ClientConfig{
		PoolSize:      320, // total concurrent sessions: 32 * 10 [Allows one user to parallelly join 10 rooms]
		UserCount:     32,
		MessageCount:  1000,
		RoomCount:     10,
		MessageBuffer: 20000, // 32 users × (10/20 rooms) × (1000+2) ≈ 16,032 + margin
	}

	client.RunFanOutLoadTest(config) // 29.6655396s
}
