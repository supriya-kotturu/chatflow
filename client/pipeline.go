package client

import (
	"context"
	"fmt"
	"time"

	client "supriyakotturu.github.com/chatflow/client/ws"
)

// RunPipelineLoadTest runs a load test using the pipeline pattern: connections
// are produced into a channel, consumed by writer goroutines, and metrics are
// collected to CSV with aggregate stats printed on completion.
func RunPipelineLoadTest(config *client.ClientConfig) {
	c := client.NewClient(config)
	defer c.Pool.CloseAll()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	start := time.Now()

	fmt.Println("Starting pipeline load test...")

	go c.GenerateMessages()
	go c.CollectMetrics(ctx)
	c.WriteMessages(ctx)
	c.Wg.Wait()

	wallTime := time.Since(start)

	c.CloseChannels()
	c.GetPerformanceMetricsSummary(wallTime)
	c.GetOverAllStats()

	if c.CSVWriter != nil {
		c.CSVWriter.Flush()
		c.CSVWriter.Fd.Close()
	}
}
