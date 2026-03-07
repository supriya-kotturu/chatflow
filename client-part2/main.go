package main

import (
	"supriyakotturu.github.com/chatflow/client"
	ws "supriyakotturu.github.com/chatflow/client/ws"
)

func main() {
	// 500K baseline: 1000 connections, 50 users/room, 500 msgs/user, 20 rooms
	// Total = 50 × 500 × 20 = 500,000 messages (+2K join/leave ≈ 502K)
	// MessageBuffer = users/room × msgs/user + margin = 50 × 500 = 25,000 → 60,000 with margin
	configs := []*ws.ClientConfig{
		{
			PoolSize:       1000,
			UserCount:      50,
			MessageCount:   500,
			RoomCount:      20,
			MessageBuffer:  60000,
			CollectMetrics: true,
			OutputFile:     "results/metrics/500K_EC2_direct.csv",
		},
	}

	for _, cfg := range configs {
		client.RunPipelineLoadTest(cfg)
	}
}
