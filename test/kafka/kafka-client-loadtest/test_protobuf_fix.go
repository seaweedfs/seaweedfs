package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

// SchemaKey represents the key structure used by Schema Registry for _schemas topic
type SchemaKey struct {
	MagicByte int    `json:"magicByte"`
	KeyType   string `json:"keytype"`
	Subject   string `json:"subject"`
	Version   int    `json:"version"`
}

// SchemaValue represents the value structure used by Schema Registry for _schemas topic
type SchemaValue struct {
	Version    int    `json:"version"`
	ID         int    `json:"id"`
	MD5        string `json:"md5"`
	Schema     string `json:"schema"`
	SchemaType string `json:"schemaType"`
	Deleted    bool   `json:"deleted"`
}

func main() {
	fmt.Println("🧪 Testing Protobuf RecordValue Fix for _schemas Topic")

	// Create Kafka producer
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Version = sarama.V2_8_0_0
	config.Producer.RequiredAcks = sarama.WaitForAll

	producer, err := sarama.NewSyncProducer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Test message to verify protobuf conversion
	testKey := SchemaKey{
		MagicByte: 0,
		KeyType:   "SCHEMA",
		Subject:   "protobuf-test-value",
		Version:   1,
	}

	testValue := SchemaValue{
		Version:    1,
		ID:         999,
		MD5:        "test-md5-hash-for-protobuf-verification",
		Schema:     `{"type":"record","name":"ProtobufTest","fields":[{"name":"message","type":"string"}]}`,
		SchemaType: "AVRO",
		Deleted:    false,
	}

	// Serialize key and value to JSON
	keyBytes, err := json.Marshal(testKey)
	if err != nil {
		log.Fatalf("Failed to marshal key: %v", err)
	}

	valueBytes, err := json.Marshal(testValue)
	if err != nil {
		log.Fatalf("Failed to marshal value: %v", err)
	}

	fmt.Printf("📝 Writing test message to _schemas topic\n")
	fmt.Printf("Key JSON: %s\n", string(keyBytes))
	fmt.Printf("Value JSON: %s\n", string(valueBytes))

	// Create Kafka message
	msg := &sarama.ProducerMessage{
		Topic: "_schemas",
		Key:   sarama.ByteEncoder(keyBytes),
		Value: sarama.ByteEncoder(valueBytes),
	}

	// Send message
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}

	fmt.Printf("✅ Message sent successfully to partition %d, offset %d\n", partition, offset)
	fmt.Println("💡 This message should now be converted to protobuf RecordValue format in SMQ")
	fmt.Printf("🔍 Check the raw file content to verify protobuf conversion\n")
	
	// Wait a moment for the message to be written
	time.Sleep(2 * time.Second)
	
	fmt.Println("🎉 Test completed! The message should now be stored as protobuf RecordValue with key/value byte fields.")
}

