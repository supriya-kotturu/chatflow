package main

import (
	"supriyakotturu.github.com/chatflow/client"
	ws "supriyakotturu.github.com/chatflow/client/ws"
)

func main() {
	configs := []*ws.ClientConfig{
		{
			PoolSize:       1000,
			UserCount:      100,
			MessageCount:   1000,
			RoomCount:      10,
			MessageBuffer:  120000, // 100 × (10/20) × (1000+2) ≈ 50,100 + margin
			CollectMetrics: true,
			OutputFolder:   "results/1M_4S",
		},
	}

	for _, cfg := range configs {
		client.RunPipelineLoadTest(cfg)
	}
}
