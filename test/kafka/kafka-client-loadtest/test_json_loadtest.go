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
	fmt.Println("🧪 Testing JSON Load Test Producer")

	// Create a test configuration with JSON messages (no schemas)
	cfg := &config.Config{
		TestMode: "producer",
		Duration: 30 * time.Second,
		Kafka: config.KafkaConfig{
			BootstrapServers: []string{"kafka-gateway:9093"},
		},
		Producers: config.ProducersConfig{
			Count:       1,
			MessageRate: 10,
			MessageSize: 512,
			ValueType:   "json", // Use JSON instead of Avro
		},
		Topics: config.TopicsConfig{
			Count:             1,
			Prefix:            "json-test-topic",
			Partitions:        4,
			ReplicationFactor: 1,
		},
		Schemas: config.SchemasConfig{
			Enabled: false, // Disable schemas for this test
		},
	}

	// Initialize metrics collector
	collector := metrics.NewCollector()

	// Create producer
	fmt.Println("1️⃣  Creating JSON producer...")
	prod, err := producer.New(cfg, collector, 0)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer prod.Close()

	fmt.Println("✅ Producer created successfully!")

	// Run producer for a short duration
	fmt.Println("2️⃣  Running producer for 30 seconds...")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	prod.Run(ctx)

	fmt.Println("✅ JSON load test completed!")
}
