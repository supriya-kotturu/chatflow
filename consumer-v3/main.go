package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"supriyakotturu.github.com/chatflow/consumer-v3/consumer"
	"supriyakotturu.github.com/chatflow/consumer-v3/metrics"
)

const (
	dlqBufferSize    = 1000
	dbChanBufferSize = 10_000
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	config := &consumer.ConsumerConfig{
		Ctx:              ctx,
		DLQBufferSize:    dlqBufferSize,
		DBChanBufferSize: dbChanBufferSize,
	}

	c, err := consumer.NewConsumer(config)
	if err != nil {
		fmt.Printf("Error creating a new consumer: %+v\n", err)
		os.Exit(1)
	}

	metricsServer := metrics.NewServer(ctx, c.DBConn)
	go metricsServer.Start(":8080")

	c.Start()
	<-ctx.Done()
	fmt.Println("Shutting down...")
}
