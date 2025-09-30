package main

import (
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("🧪 Testing _schemas Topic Offset Information")

	// Create Kafka client
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0

	client, err := sarama.NewClient([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Get topics
	topics, err := client.Topics()
	if err != nil {
		log.Fatalf("Failed to get topics: %v", err)
	}
	fmt.Printf("Available topics: %v\n", topics)

	// Get partitions for _schemas topic
	partitions, err := client.Partitions("_schemas")
	if err != nil {
		log.Fatalf("Failed to get partitions for _schemas: %v", err)
	}
	fmt.Printf("Partitions for _schemas: %v\n", partitions)

	// Get offset information for partition 0
	oldest, err := client.GetOffset("_schemas", 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Failed to get oldest offset: %v", err)
	}

	newest, err := client.GetOffset("_schemas", 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Failed to get newest offset: %v", err)
	}

	fmt.Printf("\n📊 Offset Information for _schemas[0]:\n")
	fmt.Printf("   Oldest offset: %d\n", oldest)
	fmt.Printf("   Newest offset: %d\n", newest)
	fmt.Printf("   Message count: %d\n", newest-oldest)

	if newest > oldest {
		fmt.Printf("✅ Topic has %d messages in range [%d, %d)\n", newest-oldest, oldest, newest)
		fmt.Printf("💡 Consumer should start from offset %d to read all messages\n", oldest)
	} else {
		fmt.Printf("❌ Topic appears to be empty (oldest=%d, newest=%d)\n", oldest, newest)
	}
}

