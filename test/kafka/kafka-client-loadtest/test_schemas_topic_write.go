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
	fmt.Println("🧪 Testing _schemas Topic Write via Kafka Gateway")

	// Create Kafka producer
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Version = sarama.V2_8_0_0
	config.Producer.RequiredAcks = sarama.WaitForAll

	producer, err := sarama.NewSyncProducer([]string{"localhost:9093"}, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Test messages to write to _schemas topic
	testMessages := []struct {
		description string
		key         SchemaKey
		value       SchemaValue
	}{
		{
			description: "Test schema registration for user-value subject",
			key: SchemaKey{
				MagicByte: 0,
				KeyType:   "SCHEMA",
				Subject:   "user-value",
				Version:   1,
			},
			value: SchemaValue{
				Version:    1,
				ID:         1,
				MD5:        "d41d8cd98f00b204e9800998ecf8427e",
				Schema:     `{"type":"record","name":"User","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}`,
				SchemaType: "AVRO",
				Deleted:    false,
			},
		},
		{
			description: "Test schema registration for product-value subject",
			key: SchemaKey{
				MagicByte: 0,
				KeyType:   "SCHEMA",
				Subject:   "product-value",
				Version:   1,
			},
			value: SchemaValue{
				Version:    1,
				ID:         2,
				MD5:        "a1b2c3d4e5f6789012345678901234567",
				Schema:     `{"type":"record","name":"Product","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"},{"name":"price","type":"double"}]}`,
				SchemaType: "AVRO",
				Deleted:    false,
			},
		},
		{
			description: "Test NOOP message (used by Schema Registry for leader election)",
			key: SchemaKey{
				MagicByte: 0,
				KeyType:   "NOOP",
				Subject:   "",
				Version:   0,
			},
			value: SchemaValue{
				Version:    0,
				ID:         0,
				MD5:        "",
				Schema:     "",
				SchemaType: "",
				Deleted:    false,
			},
		},
	}

	fmt.Printf("📝 Writing %d test messages to _schemas topic\n", len(testMessages))

	for i, testMsg := range testMessages {
		fmt.Printf("\n--- Message %d: %s ---\n", i+1, testMsg.description)

		// Serialize key and value to JSON
		keyBytes, err := json.Marshal(testMsg.key)
		if err != nil {
			log.Printf("❌ Failed to marshal key for message %d: %v", i+1, err)
			continue
		}

		valueBytes, err := json.Marshal(testMsg.value)
		if err != nil {
			log.Printf("❌ Failed to marshal value for message %d: %v", i+1, err)
			continue
		}

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
			log.Printf("❌ Failed to send message %d: %v", i+1, err)
			continue
		}

		fmt.Printf("✅ Message %d sent successfully to partition %d, offset %d\n", i+1, partition, offset)

		// Small delay between messages
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Println("\n🎉 All test messages sent to _schemas topic!")
	fmt.Println("💡 These messages should be converted to protobuf RecordValue format in SMQ")
	fmt.Println("📊 Check the _schemas topic configuration and data to verify the default schema is working")
}
