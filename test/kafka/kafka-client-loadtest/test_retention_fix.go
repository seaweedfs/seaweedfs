package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	// Create a Kafka writer to produce to _schemas topic
	writer := &kafka.Writer{
		Addr:     kafka.TCP("kafka-gateway:9093"),
		Topic:    "_schemas",
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	// Create a test schema message
	key := []byte(`{"keytype":"SCHEMA","subject":"retention-test-value","version":1,"magic":1}`)
	value := []byte(`{"subject":"retention-test-value","version":1,"id":1,"schemaType":"AVRO","schema":"\"string\"","deleted":false}`)

	fmt.Printf("🔥 RETENTION TEST: Producing message to _schemas topic\n")
	fmt.Printf("🔥 RETENTION TEST: Key length=%d, Value length=%d\n", len(key), len(value))

	// Produce the message
	err := writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   key,
			Value: value,
		},
	)

	if err != nil {
		log.Fatalf("Failed to write message: %v", err)
	}

	fmt.Printf("✅ RETENTION TEST: Message produced successfully\n")
	fmt.Printf("🔥 RETENTION TEST: Waiting 5 seconds to simulate publisher disconnect...\n")

	// Wait to simulate the publisher disconnecting
	time.Sleep(5 * time.Second)

	fmt.Printf("✅ RETENTION TEST: Test completed - check if _schemas partition was retained\n")
}
