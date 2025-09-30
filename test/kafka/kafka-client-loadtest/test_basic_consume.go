package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("=== Basic Consume Test (Go Client) ===")

	// Configure Sarama
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Net.DialTimeout = 30 * time.Second
	config.Net.ReadTimeout = 30 * time.Second
	config.Net.WriteTimeout = 30 * time.Second

	brokers := []string{"localhost:9093"}
	topic := "_schemas"

	fmt.Printf("Connecting to brokers: %v\n", brokers)
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	fmt.Println("Consumer created successfully")

	// Get partitions for the topic
	partitions, err := consumer.Partitions(topic)
	if err != nil {
		log.Fatalf("Failed to get partitions for topic %s: %v", topic, err)
	}

	fmt.Printf("Topic %s has %d partitions: %v\n", topic, len(partitions), partitions)

	// Consume from partition 0
	partition := int32(0)
	fmt.Printf("Starting to consume from partition %d from the beginning...\n", partition)

	partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Failed to create partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	// Set up timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	messageCount := 0
	fmt.Println("Waiting for messages...")

	for {
		select {
		case message := <-partitionConsumer.Messages():
			messageCount++
			fmt.Printf("✅ Message %d received!\n", messageCount)
			fmt.Printf("Partition: %d\n", message.Partition)
			fmt.Printf("Offset: %d\n", message.Offset)
			fmt.Printf("Timestamp: %v\n", message.Timestamp)
			fmt.Printf("Key: %s\n", string(message.Key))
			fmt.Printf("Value: %s\n", string(message.Value))
			fmt.Println("---")

		case err := <-partitionConsumer.Errors():
			log.Printf("❌ Consumer error: %v", err)

		case <-ctx.Done():
			fmt.Printf("⏰ Timeout reached. Total messages consumed: %d\n", messageCount)
			if messageCount == 0 {
				fmt.Println("❌ No messages found - this indicates a fetch/consumer issue")
			} else {
				fmt.Println("✅ Consumer is working correctly")
			}
			return
		}
	}
}
