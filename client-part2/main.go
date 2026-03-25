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
		// {
		// 	PoolSize:       16,
		// 	UserCount:      50,
		// 	MessageCount:   500,
		// 	RoomCount:      20,
		// 	MessageBuffer:  60000,
		// 	CollectMetrics: true,
		// 	OutputFile:     "results-a2/metrics/500K_pool16.csv",
		// },
		// {
		// 	PoolSize:       32,
		// 	UserCount:      50,
		// 	MessageCount:   500,
		// 	RoomCount:      20,
		// 	MessageBuffer:  60000,
		// 	CollectMetrics: true,
		// 	OutputFile:     "results-a2/metrics/500K_pool32.csv",
		// },
		{
			PoolSize:      128,
			UserCount:     50,
			MessageCount:  9360,
			RoomCount:     20,
			MessageBuffer: 1120000,
			// CollectMetrics: true,
			// OutputFile:     "results-a2/metrics/5W_4S_500K_pool1000.csv",
		},
		// {
		// 	PoolSize:       128,
		// 	UserCount:      50,
		// 	MessageCount:   500,
		// 	RoomCount:      20,
		// 	MessageBuffer:  60000,
		// 	CollectMetrics: true,
		// 	OutputFile:     "results-a2/metrics/500K_pool128.csv",
		// },
		// {
		// 	PoolSize:       256,
		// 	UserCount:      50,
		// 	MessageCount:   500,
		// 	RoomCount:      20,
		// 	MessageBuffer:  60000,
		// 	CollectMetrics: true,
		// 	OutputFile:     "results-a2/metrics/500K_pool256.csv",
		// },
		// {
		// 	PoolSize:       512,
		// 	UserCount:      50,
		// 	MessageCount:   500,
		// 	RoomCount:      20,
		// 	MessageBuffer:  60000,
		// 	CollectMetrics: true,
		// 	OutputFile:     "results-a2/metrics/500K_pool512.csv",
		// },
		// {
		// 	PoolSize:       1024,
		// 	UserCount:      50,
		// 	MessageCount:   500,
		// 	RoomCount:      20,
		// 	MessageBuffer:  60000,
		// 	CollectMetrics: true,
		// 	OutputFile:     "results-a2/metrics/500K_pool1024.csv",
		// },
	}

	for _, cfg := range configs {
		client.RunPipelineLoadTest(cfg)

	}
}
