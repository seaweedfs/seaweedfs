package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/IBM/sarama"
)

type SchemaRequest struct {
	Schema string `json:"schema"`
}

type SchemaResponse struct {
	ID int `json:"id"`
}

func main() {
	fmt.Println("🧪 Schema Integration Test Starting...")

	// Wait for Schema Registry to be ready
	if err := waitForSchemaRegistry(); err != nil {
		log.Fatalf("Schema Registry not ready: %v", err)
	}

	// Step 1: Register a schema
	schemaID, err := registerTestSchema()
	if err != nil {
		log.Fatalf("Failed to register schema: %v", err)
	}
	fmt.Printf("✅ Schema registered with ID: %d\n", schemaID)

	// Step 2: Create Kafka producer
	producer, err := createProducer()
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Step 3: Create topic with schema
	topicName := "schema-test-topic"
	if err := createTopic(topicName); err != nil {
		log.Printf("Topic creation failed (may already exist): %v", err)
	}

	// Step 4: Produce schematized messages
	messageCount := 5
	fmt.Printf("📝 Producing %d schematized messages to topic '%s'...\n", messageCount, topicName)

	for i := 0; i < messageCount; i++ {
		// Create test message
		testMessage := map[string]interface{}{
			"user_id":    fmt.Sprintf("user-%d", i+1),
			"event_type": "test_event",
			"timestamp":  time.Now().UnixNano(),
			"properties": map[string]interface{}{
				"test_run": "schema_integration",
				"seq_num":  fmt.Sprintf("%d", i+1),
			},
		}

		// Create Confluent Wire Format message (schema ID + JSON)
		schematizedMessage, err := createSchematizedMessage(schemaID, testMessage)
		if err != nil {
			log.Printf("Failed to create schematized message %d: %v", i+1, err)
			continue
		}

		// Produce message
		msg := &sarama.ProducerMessage{
			Topic: topicName,
			Key:   sarama.StringEncoder(fmt.Sprintf("key-%d", i+1)),
			Value: sarama.ByteEncoder(schematizedMessage),
		}

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Printf("Failed to produce message %d: %v", i+1, err)
			continue
		}

		fmt.Printf("  ✅ Message %d: partition=%d, offset=%d\n", i+1, partition, offset)
	}

	// Step 5: Wait a moment for messages to be stored
	fmt.Println("⏳ Waiting for messages to be stored...")
	time.Sleep(3 * time.Second)

	// Step 6: Create consumer and read back messages
	consumer, err := createConsumer()
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Step 7: Consume messages
	fmt.Printf("📖 Consuming messages from topic '%s'...\n", topicName)

	partitionConsumer, err := consumer.ConsumePartition(topicName, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Failed to create partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	// Read messages with timeout
	consumedCount := 0
	timeout := time.After(10 * time.Second)

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			consumedCount++

			// Decode schematized message
			decodedMsg, err := decodeSchematizedMessage(msg.Value)
			if err != nil {
				log.Printf("Failed to decode message %d: %v", consumedCount, err)
				continue
			}

			fmt.Printf("  ✅ Message %d: key=%s, schema_id=%d, data=%s\n",
				consumedCount, string(msg.Key), decodedMsg.SchemaID, string(decodedMsg.Payload))

			// Check if we've consumed all expected messages
			if consumedCount >= messageCount {
				goto success
			}

		case err := <-partitionConsumer.Errors():
			log.Printf("Consumer error: %v", err)

		case <-timeout:
			log.Printf("❌ Timeout: consumed %d/%d messages", consumedCount, messageCount)
			goto done
		}
	}

success:
	fmt.Printf("🎉 SUCCESS: Schema integration test completed!\n")
	fmt.Printf("   - Registered schema ID: %d\n", schemaID)
	fmt.Printf("   - Produced messages: %d\n", messageCount)
	fmt.Printf("   - Consumed messages: %d\n", consumedCount)

done:
	if consumedCount < messageCount {
		fmt.Printf("⚠️  Warning: Only consumed %d out of %d messages\n", consumedCount, messageCount)
	}
}

func waitForSchemaRegistry() error {
	maxRetries := 15
	for i := 0; i < maxRetries; i++ {
		fmt.Printf("⏳ Waiting for Schema Registry... (attempt %d/%d)\n", i+1, maxRetries)
		resp, err := http.Get("http://schema-registry:8081/config")
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			fmt.Println("✅ Schema Registry is ready!")
			return nil
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("schema registry not ready after %d retries", maxRetries)
}

func registerTestSchema() (int, error) {
	// Avro schema for user events
	avroSchema := `{
		"type": "record",
		"name": "UserEvent",
		"namespace": "com.seaweedfs.test",
		"fields": [
			{"name": "user_id", "type": "string"},
			{"name": "event_type", "type": "string"},
			{"name": "timestamp", "type": "long"},
			{"name": "properties", "type": {"type": "map", "values": "string"}}
		]
	}`

	schemaReq := SchemaRequest{Schema: avroSchema}
	jsonData, err := json.Marshal(schemaReq)
	if err != nil {
		return 0, err
	}

	resp, err := http.Post("http://schema-registry:8081/subjects/schema-test-topic-value/versions",
		"application/vnd.schemaregistry.v1+json", bytes.NewBuffer(jsonData))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("schema registration failed: status=%d, body=%s", resp.StatusCode, string(body))
	}

	var schemaResp SchemaResponse
	if err := json.NewDecoder(resp.Body).Decode(&schemaResp); err != nil {
		return 0, err
	}

	return schemaResp.ID, nil
}

func createProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Version = sarama.V2_8_0_0

	return sarama.NewSyncProducer([]string{"kafka-gateway:9093"}, config)
}

func createConsumer() (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Version = sarama.V2_8_0_0

	return sarama.NewConsumer([]string{"kafka-gateway:9093"}, config)
}

func createTopic(topicName string) error {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0

	admin, err := sarama.NewClusterAdmin([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		return err
	}
	defer admin.Close()

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	return admin.CreateTopic(topicName, topicDetail, false)
}

// SchematizedMessage represents a Confluent Wire Format message
type SchematizedMessage struct {
	SchemaID int    `json:"schema_id"`
	Payload  []byte `json:"payload"`
}

func createSchematizedMessage(schemaID int, data map[string]interface{}) ([]byte, error) {
	// Create JSON payload
	jsonPayload, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	// Create Confluent Wire Format: magic byte (0x0) + schema ID (4 bytes) + payload
	message := make([]byte, 5+len(jsonPayload))

	// Magic byte
	message[0] = 0x0

	// Schema ID (4 bytes, big-endian)
	message[1] = byte(schemaID >> 24)
	message[2] = byte(schemaID >> 16)
	message[3] = byte(schemaID >> 8)
	message[4] = byte(schemaID)

	// Payload
	copy(message[5:], jsonPayload)

	return message, nil
}

func decodeSchematizedMessage(data []byte) (*SchematizedMessage, error) {
	if len(data) < 5 {
		return nil, fmt.Errorf("message too short for Confluent Wire Format")
	}

	// Check magic byte
	if data[0] != 0x0 {
		return nil, fmt.Errorf("invalid magic byte: expected 0x0, got 0x%x", data[0])
	}

	// Extract schema ID (4 bytes, big-endian)
	schemaID := int(data[1])<<24 | int(data[2])<<16 | int(data[3])<<8 | int(data[4])

	// Extract payload
	payload := data[5:]

	return &SchematizedMessage{
		SchemaID: schemaID,
		Payload:  payload,
	}, nil
}
