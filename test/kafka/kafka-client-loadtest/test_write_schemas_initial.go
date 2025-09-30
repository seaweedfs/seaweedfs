package main

import (
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("🧪 Writing initial record to _schemas topic")

	// Connect to Kafka Gateway
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{"127.0.0.1:9093"}, config)
	if err != nil {
		log.Fatalf("❌ Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Create a simple Noop record for _schemas topic
	// Schema Registry Noop records have keytype="NOOP" and empty/no value
	key := `{"keytype":"NOOP","magic":0}`
	value := ""

	msg := &sarama.ProducerMessage{
		Topic: "_schemas",
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(value),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Fatalf("❌ Failed to send message: %v", err)
	}

	fmt.Printf("✅ Successfully wrote initial record to _schemas topic: partition=%d, offset=%d\n", partition, offset)
	fmt.Printf("   Key: %s\n", key)
	fmt.Printf("   Value: %s\n", value)
}
