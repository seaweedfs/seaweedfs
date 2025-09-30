package main

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("🧪 Testing Kafka Gateway with Sarama client")

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// Create client to check offsets
	client, err := sarama.NewClient([]string{"loadtest-kafka-gateway-no-schema:9093"}, config)
	if err != nil {
		log.Fatalf("❌ Failed to create client: %v", err)
	}
	defer client.Close()

	topicName := "test-roundtrip-topic"

	fmt.Printf("📊 Checking topic: %s\n", topicName)

	// Check offset information
	oldest, err := client.GetOffset(topicName, 0, sarama.OffsetOldest)
	if err != nil {
		log.Printf("⚠️  Failed to get oldest offset: %v", err)
	} else {
		fmt.Printf("📊 Oldest offset: %d\n", oldest)
	}
	
	newest, err := client.GetOffset(topicName, 0, sarama.OffsetNewest)
	if err != nil {
		log.Printf("⚠️  Failed to get newest offset: %v", err)
	} else {
		fmt.Printf("📊 Newest offset: %d\n", newest)
	}
	
	if newest > oldest {
		fmt.Printf("📊 Message count: %d\n", newest-oldest)
		fmt.Println("✅ Topic has messages - testing fetch")
		
		// Try to consume
		consumer, err := sarama.NewConsumer([]string{"loadtest-kafka-gateway-no-schema:9093"}, config)
		if err != nil {
			log.Fatalf("❌ Failed to create consumer: %v", err)
		}
		defer consumer.Close()

		partitionConsumer, err := consumer.ConsumePartition(topicName, 0, sarama.OffsetOldest)
		if err != nil {
			log.Fatalf("❌ Failed to consume partition: %v", err)
		}
		defer partitionConsumer.Close()

		fmt.Println("🔍 Waiting for messages...")
		select {
		case msg := <-partitionConsumer.Messages():
			fmt.Printf("✅ Received message: offset=%d, key=%s, value=%s\n", 
				msg.Offset, string(msg.Key), string(msg.Value))
		case err := <-partitionConsumer.Errors():
			fmt.Printf("❌ Consumer error: %v\n", err)
		case <-time.After(5 * time.Second):
			fmt.Println("⏰ Timeout waiting for messages")
		}
	} else {
		fmt.Println("❌ Topic appears empty")
	}
}

