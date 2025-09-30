package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("🧪 Testing Timestamp Fix on Regular Topic")

	// Create Kafka producer
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Version = sarama.V2_8_0_0
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3
	config.Producer.Timeout = 10 * time.Second

	producer, err := sarama.NewSyncProducer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Test on a regular topic (not _schemas)
	topicName := "timestamp-test-topic"
	testKey := []byte(`{"test":"timestamp-key"}`)
	testValue := []byte(`{"test":"timestamp-value","timestamp":"` + time.Now().Format(time.RFC3339) + `"}`)

	fmt.Printf("📝 Producing message to topic: %s\n", topicName)
	fmt.Printf("Key: %s\n", string(testKey))
	fmt.Printf("Value: %s\n", string(testValue))

	// Create Kafka message
	msg := &sarama.ProducerMessage{
		Topic: topicName,
		Key:   sarama.ByteEncoder(testKey),
		Value: sarama.ByteEncoder(testValue),
	}

	// Send message
	fmt.Println("🚀 Producing message...")
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Fatalf("❌ Failed to send message: %v", err)
	}

	fmt.Printf("✅ Message produced successfully!\n")
	fmt.Printf("   Topic: %s\n", topicName)
	fmt.Printf("   Partition: %d\n", partition)
	fmt.Printf("   Offset: %d\n", offset)

	// Now consume the message immediately
	fmt.Println("\n📖 Consuming the message back...")

	// Create consumer
	consumerConfig := sarama.NewConfig()
	consumerConfig.Version = sarama.V2_8_0_0
	consumerConfig.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer([]string{"kafka-gateway:9093"}, consumerConfig)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Consume from the specific offset
	partitionConsumer, err := consumer.ConsumePartition(topicName, partition, offset)
	if err != nil {
		log.Fatalf("Failed to create partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fmt.Printf("🔍 Reading message from offset %d...\n", offset)

	select {
	case message := <-partitionConsumer.Messages():
		fmt.Printf("\n🎯 CONSUMED MESSAGE\n")
		fmt.Printf("Topic: %s, Partition: %d, Offset: %d\n", message.Topic, message.Partition, message.Offset)

		// Check timestamp
		timestamp := message.Timestamp
		fmt.Printf("Timestamp: %s\n", timestamp.Format(time.RFC3339))
		fmt.Printf("Timestamp (Unix): %d\n", timestamp.Unix())

		// Verify timestamp is reasonable (within last hour and next hour)
		now := time.Now()
		oneHourAgo := now.Add(-1 * time.Hour)
		oneHourFromNow := now.Add(1 * time.Hour)

		if timestamp.After(oneHourAgo) && timestamp.Before(oneHourFromNow) {
			fmt.Printf("✅ SUCCESS! Timestamp is correct (within reasonable range)\n")
			fmt.Printf("🎉 TIMESTAMP FIX WORKING!\n")

			timeDiff := now.Sub(timestamp)
			if timeDiff < 0 {
				timeDiff = -timeDiff
			}
			fmt.Printf("📊 Time difference from now: %v\n", timeDiff)
		} else {
			fmt.Printf("❌ FAILED! Timestamp is incorrect\n")
			fmt.Printf("   Expected between: %s and %s\n", oneHourAgo.Format(time.RFC3339), oneHourFromNow.Format(time.RFC3339))
		}

		// Display message content
		fmt.Printf("\nMessage Content:\n")
		fmt.Printf("  Key: %q\n", string(message.Key))
		fmt.Printf("  Value: %q\n", string(message.Value))

	case err := <-partitionConsumer.Errors():
		log.Printf("❌ Consumer error: %v", err)

	case <-ctx.Done():
		fmt.Printf("⏰ Timeout - no message received\n")
	}

	fmt.Println("\n✅ Test complete!")
}

