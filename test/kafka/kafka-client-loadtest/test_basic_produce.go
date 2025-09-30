package main

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("=== Basic Produce Test (Go Client) ===")

	// Configure Sarama
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3
	config.Producer.Timeout = 30 * time.Second
	config.Net.DialTimeout = 30 * time.Second
	config.Net.ReadTimeout = 30 * time.Second
	config.Net.WriteTimeout = 30 * time.Second

	// Disable idempotence to avoid InitProducerId
	config.Producer.Idempotent = false

	brokers := []string{"localhost:9093"}
	topic := "_schemas"

	fmt.Printf("Connecting to brokers: %v\n", brokers)
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	fmt.Println("Producer created successfully")

	// Create test message
	key := `{"keytype":"TEST","magic":1}`
	value := fmt.Sprintf(`{"test":"go_client_test","timestamp":%d}`, time.Now().UnixMilli())

	fmt.Printf("Producing message to topic %s\n", topic)
	fmt.Printf("Key: %s\n", key)
	fmt.Printf("Value: %s\n", value)

	message := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(value),
	}

	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}

	fmt.Printf("✅ SUCCESS: Message produced successfully!\n")
	fmt.Printf("Partition: %d\n", partition)
	fmt.Printf("Offset: %d\n", offset)
}
