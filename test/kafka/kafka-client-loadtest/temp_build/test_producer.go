package main

import (
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("🚀 Producing test message to Kafka Gateway")

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{"loadtest-kafka-gateway-no-schema:9093"}, config)
	if err != nil {
		log.Fatalf("❌ Failed to create producer: %v", err)
	}
	defer producer.Close()

	topicName := "test-roundtrip-topic"
	message := &sarama.ProducerMessage{
		Topic: topicName,
		Key:   sarama.StringEncoder("test-key"),
		Value: sarama.StringEncoder("Hello from Kafka Gateway test!"),
	}

	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		log.Fatalf("❌ Failed to send message: %v", err)
	}

	fmt.Printf("✅ Message sent successfully: partition=%d, offset=%d\n", partition, offset)
}
