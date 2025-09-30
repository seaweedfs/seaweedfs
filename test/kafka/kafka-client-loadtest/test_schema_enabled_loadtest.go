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
	fmt.Println("🧪 Testing Schema-Enabled Load Test Producer")

	// Create a test configuration with schemas enabled
	cfg := &config.Config{
		TestMode: "producer",
		Duration: 30 * time.Second,
		Kafka: config.KafkaConfig{
			BootstrapServers: []string{"kafka-gateway:9093"},
		},
		SchemaRegistry: config.SchemaRegistryConfig{
			URL: "http://schema-registry:8081",
		},
		Producers: config.ProducersConfig{
			Count:       1,
			MessageRate: 10,
			MessageSize: 512,
			ValueType:   "avro",
		},
		Topics: config.TopicsConfig{
			Count:             1,
			Prefix:            "schema-test-topic",
			Partitions:        4,
			ReplicationFactor: 1,
		},
		Schemas: config.SchemasConfig{
			Enabled: true,
		},
	}

	// Initialize metrics collector
	collector := metrics.NewCollector()

	// Create producer
	fmt.Println("1️⃣  Creating schema-aware producer...")
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

	fmt.Println("✅ Schema-enabled load test completed!")
}
