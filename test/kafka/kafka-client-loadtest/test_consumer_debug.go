package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("🧪 Debug Consumer for _schemas Topic")

	// Create Kafka consumer
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Get topic metadata first
	topics, err := consumer.Topics()
	if err != nil {
		log.Fatalf("Failed to get topics: %v", err)
	}
	fmt.Printf("Available topics: %v\n", topics)

	// Get partition info
	partitions, err := consumer.Partitions("_schemas")
	if err != nil {
		log.Fatalf("Failed to get partitions: %v", err)
	}
	fmt.Printf("Partitions for _schemas: %v\n", partitions)

	// Create a client to get offset info
	client, err := sarama.NewClient([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Get offset info for partition 0
	oldest, err := client.GetOffset("_schemas", 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Failed to get oldest offset: %v", err)
	}
	newest, err := client.GetOffset("_schemas", 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Failed to get newest offset: %v", err)
	}
	fmt.Printf("Offset range for _schemas[0]: oldest=%d, newest=%d\n", oldest, newest)

	// Try to consume from each offset
	for offset := oldest; offset < newest; offset++ {
		fmt.Printf("\n--- Trying to consume from offset %d ---\n", offset)
		
		partitionConsumer, err := consumer.ConsumePartition("_schemas", 0, offset)
		if err != nil {
			fmt.Printf("❌ Failed to create partition consumer for offset %d: %v\n", offset, err)
			continue
		}

		// Create context with short timeout
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		
		select {
		case message := <-partitionConsumer.Messages():
			fmt.Printf("✅ Message found at offset %d:\n", offset)
			fmt.Printf("   Partition: %d, Offset: %d\n", message.Partition, message.Offset)
			fmt.Printf("   Timestamp: %s\n", message.Timestamp.Format(time.RFC3339))
			
			if message.Key != nil {
				fmt.Printf("   Key Length: %d bytes\n", len(message.Key))
				fmt.Printf("   Key Hex: %s\n", hex.EncodeToString(message.Key))
				fmt.Printf("   Key String: %q\n", string(message.Key))
			} else {
				fmt.Println("   Key: <nil>")
			}
			
			if message.Value != nil {
				fmt.Printf("   Value Length: %d bytes\n", len(message.Value))
				fmt.Printf("   Value Hex: %s\n", hex.EncodeToString(message.Value))
				fmt.Printf("   Value String: %q\n", string(message.Value))
				
				// Check if it looks like protobuf
				if len(message.Value) > 0 {
					firstByte := message.Value[0]
					if firstByte&0x07 == 2 { // Wire type 2 (length-delimited)
						fmt.Println("   🔍 Format: Likely protobuf RecordValue")
					} else if message.Value[0] == '{' {
						fmt.Println("   🔍 Format: JSON")
					} else {
						fmt.Printf("   🔍 Format: Unknown (first byte: 0x%02x)\n", firstByte)
					}
				}
			} else {
				fmt.Println("   Value: <nil>")
			}
			
		case err := <-partitionConsumer.Errors():
			fmt.Printf("❌ Consumer error at offset %d: %v\n", offset, err)
			
		case <-ctx.Done():
			fmt.Printf("⏰ Timeout at offset %d\n", offset)
		}
		
		cancel()
		partitionConsumer.Close()
	}
	
	fmt.Printf("\n🎉 Debug completed. Found %d messages in range [%d, %d)\n", newest-oldest, oldest, newest)
}
