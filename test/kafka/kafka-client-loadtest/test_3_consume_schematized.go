package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/linkedin/goavro/v2"
)

// Test 3: Consume Schematized Messages
// This test produces a schematized message and then consumes it to verify the full pipeline

func main() {
	log.Println("=== Test 3: Consume Schematized Messages ===")

	topicName := "test-consume-topic"

	// Step 1: Register schema and get codec
	log.Println("Step 1: Setting up schema and codec...")
	schemaID, codec, err := setupSchemaAndCodec(topicName)
	if err != nil {
		log.Fatalf("Failed to setup schema: %v", err)
	}
	log.Printf("✓ Schema ID: %d, Codec ready", schemaID)

	// Step 2: Create topic
	log.Println("Step 2: Creating topic...")
	err = createTopic(topicName)
	if err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}
	log.Printf("✓ Topic '%s' created", topicName)

	// Step 3: Produce a test message
	log.Println("Step 3: Producing test message...")
	err = produceTestMessage(topicName, schemaID, codec)
	if err != nil {
		log.Fatalf("Failed to produce message: %v", err)
	}
	log.Printf("✓ Test message produced")

	// Step 4: Consume and decode the message
	log.Println("Step 4: Consuming and decoding message...")
	err = consumeAndDecodeMessage(topicName, codec)
	if err != nil {
		log.Fatalf("Failed to consume message: %v", err)
	}
	log.Printf("✓ Message consumed and decoded successfully")

	log.Println("✅ Test 3 COMPLETED: Full schematized message pipeline works!")
}

func setupSchemaAndCodec(topicName string) (int, *goavro.Codec, error) {
	schema := `{
		"type": "record",
		"name": "TestMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "message", "type": "string"},
			{"name": "timestamp", "type": "long"}
		]
	}`

	// Register schema
	payload := fmt.Sprintf(`{"schema": %q}`, schema)
	resp, err := http.Post(
		fmt.Sprintf("http://localhost:8081/subjects/%s-value/versions", topicName),
		"application/vnd.schemaregistry.v1+json",
		strings.NewReader(payload),
	)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return 0, nil, fmt.Errorf("schema registration failed: %d - %s", resp.StatusCode, string(body))
	}

	var result struct {
		ID int `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, nil, err
	}

	// Create codec
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return 0, nil, err
	}

	return result.ID, codec, nil
}

func createTopic(topicName string) error {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0

	admin, err := sarama.NewClusterAdmin([]string{"localhost:9093"}, config)
	if err != nil {
		return fmt.Errorf("failed to create admin client: %w", err)
	}
	defer admin.Close()

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     4,
		ReplicationFactor: 1,
	}

	err = admin.CreateTopic(topicName, topicDetail, false)
	if err != nil && err != sarama.ErrTopicAlreadyExists {
		return fmt.Errorf("failed to create topic: %w", err)
	}

	return nil
}

func produceTestMessage(topicName string, schemaID int, codec *goavro.Codec) error {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	producer, err := sarama.NewSyncProducer([]string{"localhost:9093"}, config)
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
	}
	defer producer.Close()

	// Create test data
	messageData := map[string]interface{}{
		"id":        456,
		"message":   "Test consume message",
		"timestamp": time.Now().UnixNano() / int64(time.Millisecond),
	}

	// Encode to Avro
	avroBytes, err := codec.BinaryFromNative(nil, messageData)
	if err != nil {
		return fmt.Errorf("failed to encode Avro: %w", err)
	}

	// Wrap in Confluent Wire Format
	wireFormatBytes := createConfluentWireFormat(schemaID, avroBytes)

	// Send message
	msg := &sarama.ProducerMessage{
		Topic: topicName,
		Key:   sarama.StringEncoder("test-consume-key"),
		Value: sarama.ByteEncoder(wireFormatBytes),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	log.Printf("  Message sent to partition %d at offset %d", partition, offset)
	log.Printf("  Original data: %+v", messageData)
	return nil
}

func consumeAndDecodeMessage(topicName string, codec *goavro.Codec) error {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumer([]string{"localhost:9093"}, config)
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topicName, 0, sarama.OffsetOldest)
	if err != nil {
		return fmt.Errorf("failed to create partition consumer: %w", err)
	}
	defer partitionConsumer.Close()

	// Set timeout for consuming
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for {
		select {
		case message := <-partitionConsumer.Messages():
			log.Printf("  Received message: partition=%d offset=%d", message.Partition, message.Offset)
			log.Printf("  Raw message size: %d bytes", len(message.Value))

			// Decode the message
			decodedData, err := decodeConfluentWireFormat(message.Value, codec)
			if err != nil {
				return fmt.Errorf("failed to decode message: %w", err)
			}

			log.Printf("  Decoded data: %+v", decodedData)
			return nil

		case err := <-partitionConsumer.Errors():
			return fmt.Errorf("consumer error: %w", err)

		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for message")
		}
	}
}

func createConfluentWireFormat(schemaID int, avroData []byte) []byte {
	wireFormat := make([]byte, 5+len(avroData))
	wireFormat[0] = 0 // Magic byte
	binary.BigEndian.PutUint32(wireFormat[1:5], uint32(schemaID))
	copy(wireFormat[5:], avroData)
	return wireFormat
}

func decodeConfluentWireFormat(wireFormatData []byte, codec *goavro.Codec) (interface{}, error) {
	if len(wireFormatData) < 5 {
		return nil, fmt.Errorf("message too short for Confluent Wire Format: %d bytes", len(wireFormatData))
	}

	// Check magic byte
	if wireFormatData[0] != 0 {
		return nil, fmt.Errorf("invalid magic byte: %d", wireFormatData[0])
	}

	// Extract schema ID
	schemaID := binary.BigEndian.Uint32(wireFormatData[1:5])
	log.Printf("  Schema ID from message: %d", schemaID)

	// Extract and decode Avro data
	avroData := wireFormatData[5:]
	native, _, err := codec.NativeFromBinary(avroData)
	if err != nil {
		return nil, fmt.Errorf("failed to decode Avro data: %w", err)
	}

	return native, nil
}

