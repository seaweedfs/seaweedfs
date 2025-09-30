package main

import (
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("🧪 Testing Kafka Gateway offset information")

	config := sarama.NewConfig()
	client, err := sarama.NewClient([]string{"loadtest-kafka-gateway-no-schema:9093"}, config)
	if err != nil {
		log.Fatalf("❌ Failed to create client: %v", err)
	}
	defer client.Close()

	topicName := "test-roundtrip-topic"

	// Check offset information
	oldest, err := client.GetOffset(topicName, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("❌ Failed to get oldest offset: %v", err)
	}

	newest, err := client.GetOffset(topicName, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("❌ Failed to get newest offset: %v", err)
	}

	fmt.Printf("📊 Topic: %s, Partition: 0\n", topicName)
	fmt.Printf("📊 Oldest offset: %d\n", oldest)
	fmt.Printf("📊 Newest offset: %d\n", newest)
	fmt.Printf("📊 Message count: %d\n", newest-oldest)

	if newest > oldest {
		fmt.Println("✅ Topic has messages - fetch should work")
	} else {
		fmt.Println("❌ Topic appears empty - this is the problem")
	}
}

