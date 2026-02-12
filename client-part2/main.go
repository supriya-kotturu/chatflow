package main

import (
	"supriyakotturu.github.com/chatflow/client"
	clientWs "supriyakotturu.github.com/chatflow/client/ws"
)

func main() {
	config := &clientWs.ClientConfig{
		PoolSize:       10000, // total concurrent sessions: users * rooms
		UserCount:      500,
		MessageCount:   1000,
		RoomCount:      20,
		MessageBuffer:  12000,
		CollectMetrics: true,
		OutputFolder:   "results",
	}

	client.RunPipelineLoadTest(config)
}
