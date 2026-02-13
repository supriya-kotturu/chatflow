package main

import (
	"supriyakotturu.github.com/chatflow/client"
	clientWs "supriyakotturu.github.com/chatflow/client/ws"
)

func main() {
	configs := []*clientWs.ClientConfig{
		// 500K: 50 × (500+2) × 20 = 502,000 messages
		{
			PoolSize:       1000,
			UserCount:      50,
			MessageCount:   500,
			RoomCount:      20,
			MessageBuffer:  1200,
			CollectMetrics: true,
			OutputFolder:   "results/500K",
		},
		// 1M: 100 × (1000+2) × 10 = 1,002,000 messages
		{
			PoolSize:       1000,
			UserCount:      100,
			MessageCount:   1000,
			RoomCount:      10,
			MessageBuffer:  1200,
			CollectMetrics: true,
			OutputFolder:   "results/1M",
		},
		// 1.5M: 100 × (750+2) × 20 = 1,504,000 messages
		{
			PoolSize:       1000,
			UserCount:      100,
			MessageCount:   750,
			RoomCount:      20,
			MessageBuffer:  3000,
			CollectMetrics: true,
			OutputFolder:   "results/1_5M",
		},
		// 2M: 100 × (1000+2) × 20 = 2,004,000 messages
		{
			PoolSize:       2000,
			UserCount:      100,
			MessageCount:   1000,
			RoomCount:      20,
			MessageBuffer:  2500,
			CollectMetrics: true,
			OutputFolder:   "results/2M",
		},
		// 2.5M: 125 × (1000+2) × 20 = 2,505,000 messages
		{
			PoolSize:       2500,
			UserCount:      125,
			MessageCount:   1000,
			RoomCount:      20,
			MessageBuffer:  3000,
			CollectMetrics: true,
			OutputFolder:   "results/2_5M",
		},
	}

	for _, cfg := range configs {
		client.RunPipelineLoadTest(cfg)
	}
}
