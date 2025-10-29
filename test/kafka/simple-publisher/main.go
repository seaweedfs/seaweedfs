package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	// Configuration
	brokerAddress := "localhost:9093" // Kafka gateway port (not SeaweedMQ broker port 17777)
	topicName := "_raw_messages"      // Topic with "_" prefix - should skip schema validation

	fmt.Printf("Publishing messages to topic '%s' on broker '%s'\n", topicName, brokerAddress)

	// Create a new writer
	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    topicName,
		Balancer: &kafka.LeastBytes{},
		// Configure for immediate delivery (useful for testing)
		BatchTimeout: 10 * time.Millisecond,
		BatchSize:    1,
	}
	defer writer.Close()

	// Sample data to publish
	messages := []map[string]interface{}{
		{
			"id":        1,
			"message":   "Hello from kafka-go client",
			"timestamp": time.Now().Unix(),
			"user_id":   "user123",
		},
		{
			"id":        2,
			"message":   "Raw message without schema validation",
			"timestamp": time.Now().Unix(),
			"user_id":   "user456",
			"metadata": map[string]string{
				"source": "test-client",
				"type":   "raw",
			},
		},
		{
			"id":        3,
			"message":   "Testing SMQ with underscore prefix topic",
			"timestamp": time.Now().Unix(),
			"user_id":   "user789",
			"data":      []byte("Some binary data here"),
		},
	}

	ctx := context.Background()

	fmt.Println("Publishing messages...")
	for i, msgData := range messages {
		// Convert message to JSON (simulating raw messages stored in "value" field)
		valueBytes, err := json.Marshal(msgData)
		if err != nil {
			log.Fatalf("Failed to marshal message %d: %v", i+1, err)
		}

		// Create Kafka message
		msg := kafka.Message{
			Key:   []byte(fmt.Sprintf("key_%d", msgData["id"])),
			Value: valueBytes,
			Headers: []kafka.Header{
				{Key: "source", Value: []byte("kafka-go-client")},
				{Key: "content-type", Value: []byte("application/json")},
			},
		}

		// Write message
		err = writer.WriteMessages(ctx, msg)
		if err != nil {
			log.Printf("Failed to write message %d: %v", i+1, err)
			continue
		}

		fmt.Printf("-Published message %d: %s\n", i+1, string(valueBytes))

		// Small delay between messages
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Println("\nAll messages published successfully!")

	// Test with different raw message types
	fmt.Println("\nPublishing different raw message formats...")

	rawMessages := []kafka.Message{
		{
			Key:   []byte("binary_key"),
			Value: []byte("Simple string message"),
		},
		{
			Key:   []byte("json_key"),
			Value: []byte(`{"raw_field": "raw_value", "number": 42}`),
		},
		{
			Key:   []byte("empty_key"),
			Value: []byte{}, // Empty value
		},
		{
			Key:   nil, // No key
			Value: []byte("Message with no key"),
		},
	}

	for i, msg := range rawMessages {
		err := writer.WriteMessages(ctx, msg)
		if err != nil {
			log.Printf("Failed to write raw message %d: %v", i+1, err)
			continue
		}
		fmt.Printf("-Published raw message %d: key=%s, value=%s\n",
			i+1, string(msg.Key), string(msg.Value))
	}

	fmt.Println("\nAll test messages published to topic with '_' prefix!")
	fmt.Println("These messages should be stored as raw bytes without schema validation.")
}
