package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("🧪 Final Timestamp Fix Verification - Reading Newest Message")

	// Create Kafka consumer
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Get partition consumer for _schemas topic, starting from offset 15 (newest message)
	partitionConsumer, err := consumer.ConsumePartition("_schemas", 0, 15)
	if err != nil {
		log.Fatalf("Failed to create partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	fmt.Println("📖 Reading newest message from _schemas topic (offset 15)...")

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	messageCount := 0
	maxMessages := 1

	for {
		select {
		case message := <-partitionConsumer.Messages():
			messageCount++
			fmt.Printf("\n🎯 FINAL VERIFICATION - Message at Offset %d\n", message.Offset)

			// Check if timestamp is reasonable (should be around current time)
			timestamp := message.Timestamp
			fmt.Printf("Timestamp: %s\n", timestamp.Format(time.RFC3339))
			fmt.Printf("Timestamp (Unix): %d\n", timestamp.Unix())
			fmt.Printf("Timestamp (Unix Millis): %d\n", timestamp.UnixMilli())

			// Verify timestamp is reasonable (within last hour and next hour)
			now := time.Now()
			oneHourAgo := now.Add(-1 * time.Hour)
			oneHourFromNow := now.Add(1 * time.Hour)

			if timestamp.After(oneHourAgo) && timestamp.Before(oneHourFromNow) {
				fmt.Printf("✅ SUCCESS! Timestamp is correct (within reasonable range)\n")
				fmt.Printf("🎉 TIMESTAMP FIX SUCCESSFUL!\n")
				fmt.Printf("🔧 Nanoseconds → Milliseconds conversion is working properly\n")

				// Calculate how close to current time
				timeDiff := now.Sub(timestamp)
				if timeDiff < 0 {
					timeDiff = -timeDiff
				}
				fmt.Printf("📊 Time difference from now: %v\n", timeDiff)

				if timeDiff < 10*time.Minute {
					fmt.Printf("⭐ EXCELLENT! Timestamp is very recent (within 10 minutes)\n")
				}
			} else {
				fmt.Printf("❌ FAILED! Timestamp is still incorrect (outside reasonable range)\n")
				fmt.Printf("   Expected between: %s and %s\n", oneHourAgo.Format(time.RFC3339), oneHourFromNow.Format(time.RFC3339))
				fmt.Printf("🔧 Timestamp fix needs further investigation\n")
			}

			// Display key and value info
			fmt.Printf("\nMessage Details:\n")
			if message.Key != nil {
				fmt.Printf("  Key: %q\n", string(message.Key))
			} else {
				fmt.Printf("  Key: <nil>\n")
			}

			if message.Value != nil {
				fmt.Printf("  Value Length: %d bytes\n", len(message.Value))
			} else {
				fmt.Printf("  Value: <nil> (expected for _schemas topic)\n")
			}

			if messageCount >= maxMessages {
				fmt.Printf("\n✅ Verification complete!\n")
				return
			}

		case err := <-partitionConsumer.Errors():
			log.Printf("❌ Consumer error: %v", err)

		case <-ctx.Done():
			fmt.Printf("\n⏰ Timeout reached, read %d messages\n", messageCount)
			if messageCount == 0 {
				fmt.Printf("❌ No messages consumed - check offset range\n")
			}
			return
		}
	}
}
