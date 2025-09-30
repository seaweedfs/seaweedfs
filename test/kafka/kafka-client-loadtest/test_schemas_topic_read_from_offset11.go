package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("🧪 Testing _schemas Topic Read from Offset 11 (Final Timestamp Fix Verification)")

	// Create Kafka consumer
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Get partition consumer for _schemas topic, starting from offset 11
	partitionConsumer, err := consumer.ConsumePartition("_schemas", 0, 11) // Start from offset 11
	if err != nil {
		log.Fatalf("Failed to create partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	fmt.Println("📖 Reading messages from _schemas topic starting from offset 11...")

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	messageCount := 0
	maxMessages := 2

	for {
		select {
		case message := <-partitionConsumer.Messages():
			messageCount++
			fmt.Printf("\n--- Message %d ---\n", messageCount)
			fmt.Printf("Partition: %d, Offset: %d\n", message.Partition, message.Offset)

			// Check if timestamp is reasonable (should be around current time)
			timestamp := message.Timestamp
			fmt.Printf("Timestamp: %s\n", timestamp.Format(time.RFC3339))
			fmt.Printf("Timestamp (Unix): %d\n", timestamp.Unix())

			// Verify timestamp is reasonable (within last hour and next hour)
			now := time.Now()
			oneHourAgo := now.Add(-1 * time.Hour)
			oneHourFromNow := now.Add(1 * time.Hour)

			if timestamp.After(oneHourAgo) && timestamp.Before(oneHourFromNow) {
				fmt.Printf("✅ Timestamp looks correct (within reasonable range)\n")
			} else {
				fmt.Printf("❌ Timestamp looks incorrect (outside reasonable range)\n")
				fmt.Printf("   Expected between: %s and %s\n", oneHourAgo.Format(time.RFC3339), oneHourFromNow.Format(time.RFC3339))
			}

			// Display key
			if message.Key != nil {
				fmt.Printf("Key Length: %d bytes\n", len(message.Key))
				fmt.Printf("Key String: %q\n", string(message.Key))
			} else {
				fmt.Println("Key: <nil>")
			}

			// Display value (abbreviated)
			if message.Value != nil {
				fmt.Printf("Value Length: %d bytes\n", len(message.Value))
				fmt.Printf("Value: <nil> (expected for _schemas topic)\n")
			} else {
				fmt.Println("Value: <nil> (expected for _schemas topic)")
			}

			if messageCount >= maxMessages {
				fmt.Printf("\n✅ Read %d messages, stopping\n", messageCount)
				return
			}

		case err := <-partitionConsumer.Errors():
			log.Printf("❌ Consumer error: %v", err)

		case <-ctx.Done():
			fmt.Printf("\n⏰ Timeout reached, read %d messages\n", messageCount)
			return
		}
	}
}

