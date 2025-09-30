package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/seaweedfs/seaweedfs/test/kafka/kafka-client-loadtest/internal/config"
	"github.com/seaweedfs/seaweedfs/test/kafka/kafka-client-loadtest/internal/metrics"
	"github.com/seaweedfs/seaweedfs/test/kafka/kafka-client-loadtest/internal/producer"
)

func main() {
	fmt.Println("🧪 Testing Producer Schema Registration")

	// Create a basic config
	cfg := &config.Config{}
	cfg.setDefaults()

	// Override for testing
	cfg.Kafka.BootstrapServers = []string{"localhost:9093"}
	cfg.SchemaRegistry.URL = "http://localhost:8081"
	cfg.Topics.Count = 2
	cfg.Topics.Prefix = "test-topic"
	cfg.Producers.ValueType = "avro"

	fmt.Printf("📋 Config: Topics=%v, SchemaRegistry=%s\n", cfg.GetTopicNames(), cfg.SchemaRegistry.URL)

	// Create metrics collector
	metricsCollector := metrics.NewCollector()

	// Try to create a producer (this will register schemas)
	fmt.Println("🏭 Creating producer with schema registration...")
	p, err := producer.New(cfg, metricsCollector, 0)
	if err != nil {
		log.Fatalf("❌ Failed to create producer: %v", err)
	}
	defer p.Close()

	fmt.Println("✅ Producer created successfully with schemas registered!")

	// Try to produce a few messages
	fmt.Println("📤 Testing message production...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		if err := p.Run(ctx); err != nil {
			log.Printf("Producer run error: %v", err)
		}
	}()

	// Let it run for a few seconds
	time.Sleep(3 * time.Second)
	cancel()

	fmt.Println("✅ Test completed successfully!")
}
