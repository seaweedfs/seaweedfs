package main

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("🧪 Testing Kafka Gateway fetch debugging")

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

	// Check if topic exists and get offset info
	partitions, err := client.Partitions(topicName)
	if err != nil {
		log.Fatalf("❌ Failed to get partitions: %v", err)
	}
	fmt.Printf("📊 Topic %s has %d partitions\n", topicName, len(partitions))

	for _, partition := range partitions {
		oldest, err := client.GetOffset(topicName, partition, sarama.OffsetOldest)
		if err != nil {
			log.Printf("⚠️  Failed to get oldest offset for partition %d: %v", partition, err)
			continue
		}
		
		newest, err := client.GetOffset(topicName, partition, sarama.OffsetNewest)
		if err != nil {
			log.Printf("⚠️  Failed to get newest offset for partition %d: %v", partition, err)
			continue
		}
		
		fmt.Printf("📊 Partition %d: oldest=%d, newest=%d, messages=%d\n", 
			partition, oldest, newest, newest-oldest)
	}

	// Try to consume from the topic
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
}