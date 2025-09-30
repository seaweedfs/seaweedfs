package main

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("🧪 Testing Simple Message Production to _schemas Topic")

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

	// Simple test message
	testKey := []byte(`{"test":"key"}`)
	testValue := []byte(`{"test":"value","timestamp":"` + time.Now().Format(time.RFC3339) + `"}`)

	fmt.Printf("📝 Sending simple test message to _schemas topic\n")
	fmt.Printf("Key: %s\n", string(testKey))
	fmt.Printf("Value: %s\n", string(testValue))

	// Create Kafka message
	msg := &sarama.ProducerMessage{
		Topic: "_schemas",
		Key:   sarama.ByteEncoder(testKey),
		Value: sarama.ByteEncoder(testValue),
	}

	// Send message with timeout
	fmt.Println("🚀 Sending message...")
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Fatalf("❌ Failed to send message: %v", err)
	}

	fmt.Printf("✅ Message sent successfully!\n")
	fmt.Printf("   Partition: %d\n", partition)
	fmt.Printf("   Offset: %d\n", offset)
	fmt.Println("💡 Check Kafka Gateway logs for produce request processing")
}

