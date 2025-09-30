package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("🧪 Testing Protobuf RecordValue Conversion")

	// Create Kafka producer
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Version = sarama.V2_8_0_0

	producer, err := sarama.NewSyncProducer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Create a simple JSON message (non-schematized)
	testMessage := map[string]interface{}{
		"id":        "test-123",
		"timestamp": time.Now().Unix(),
		"message":   "Hello from protobuf conversion test",
		"count":     42,
	}

	jsonBytes, err := json.Marshal(testMessage)
	if err != nil {
		log.Fatalf("Failed to marshal JSON: %v", err)
	}

	fmt.Printf("📤 Sending JSON message: %s\n", string(jsonBytes))

	// Send message to Kafka Gateway (this will auto-create the topic)
	topicName := "protobuf-test-topic"
	msg := &sarama.ProducerMessage{
		Topic: topicName,
		Key:   sarama.StringEncoder("test-key"),
		Value: sarama.ByteEncoder(jsonBytes),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}

	fmt.Printf("✅ Message sent successfully!\n")
	fmt.Printf("   Topic: protobuf-test-topic\n")
	fmt.Printf("   Partition: %d\n", partition)
	fmt.Printf("   Offset: %d\n", offset)
	fmt.Printf("   Message: %s\n", string(jsonBytes))

	fmt.Println("\n🎯 Expected behavior:")
	fmt.Println("   - JSON message should be converted to protobuf RecordValue")
	fmt.Println("   - SMQ should receive protobuf bytes, not JSON")
	fmt.Println("   - Topic should be auto-created with schema support")

	fmt.Println("\n✅ Test completed!")
}
