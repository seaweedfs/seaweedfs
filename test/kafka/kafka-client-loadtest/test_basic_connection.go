package main

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("Testing basic connection...")

	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Net.DialTimeout = 5 * time.Second
	config.Net.ReadTimeout = 5 * time.Second
	config.Net.WriteTimeout = 5 * time.Second
	config.ClientID = "test-basic-connection"

	fmt.Println("Creating client...")
	client, err := sarama.NewClient([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	fmt.Println("Client created successfully")

	// Test basic operations
	fmt.Println("Getting topics...")
	topics, err := client.Topics()
	if err != nil {
		log.Fatalf("Failed to get topics: %v", err)
	}
	fmt.Printf("Found %d topics: %v\n", len(topics), topics)

	fmt.Println("Getting brokers...")
	brokers := client.Brokers()
	fmt.Printf("Found %d brokers\n", len(brokers))
	for i, broker := range brokers {
		fmt.Printf("Broker %d: %s\n", i, broker.Addr())
	}

	fmt.Println("Basic connection test completed successfully")
}


