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

	// Create a test schema message similar to what Schema Registry would send
	key := []byte(`{"keytype":"SCHEMA","subject":"test-buffer-value","version":1,"magic":1}`)
	value := []byte(`{"subject":"test-buffer-value","version":1,"id":1,"schemaType":"AVRO","schema":"\"string\"","deleted":false}`)

	fmt.Printf("🔥 TEST: Producing message to _schemas topic\n")
	fmt.Printf("🔥 TEST: Key length=%d, Value length=%d\n", len(key), len(value))
	fmt.Printf("🔥 TEST: Key: %s\n", string(key))
	fmt.Printf("🔥 TEST: Value: %s\n", string(value))

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

	fmt.Printf("✅ TEST: Message produced successfully\n")

	// Wait a moment for the message to be processed
	time.Sleep(2 * time.Second)

	fmt.Printf("🔥 TEST: Test completed - check broker logs for buffer debug info\n")
}
