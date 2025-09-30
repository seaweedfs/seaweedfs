package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	fmt.Println("🔍 DEBUG: Testing offset issue with _schemas topic")

	// Create a consumer to read from _schemas topic starting from offset 0
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"kafka-gateway:9093"},
		Topic:     "_schemas",
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer reader.Close()

	// Set offset to 0 (beginning)
	err := reader.SetOffset(0)
	if err != nil {
		log.Fatalf("Failed to set offset to 0: %v", err)
	}

	fmt.Println("🔍 DEBUG: Set offset to 0, attempting to read messages...")

	// Try to read up to 5 messages
	for i := 0; i < 5; i++ {
		ctx := context.Background()
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			fmt.Printf("🔍 DEBUG: Error reading message %d: %v\n", i, err)
			break
		}

		fmt.Printf("🔍 DEBUG: Message %d - Offset: %d, Key: %s, Value length: %d\n",
			i, message.Offset, string(message.Key), len(message.Value))

		if len(message.Value) > 0 {
			fmt.Printf("🔍 DEBUG: Message %d - Value: %s\n", i, string(message.Value))
		} else {
			fmt.Printf("🔍 DEBUG: Message %d - Value is empty!\n", i)
		}
	}

	fmt.Println("🔍 DEBUG: Test completed")
}


