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
	fmt.Println("🧪 Simple Schema Test Starting...")

	// Step 1: Check Schema Registry
	fmt.Println("1️⃣  Checking Schema Registry...")
	if err := checkSchemaRegistry(); err != nil {
		log.Printf("❌ Schema Registry check failed: %v", err)
		return
	}
	fmt.Println("✅ Schema Registry is working!")

	// Step 2: Register a schema
	fmt.Println("2️⃣  Registering test schema...")
	schemaID, err := registerTestSchema()
	if err != nil {
		log.Printf("❌ Schema registration failed: %v", err)
		return
	}
	fmt.Printf("✅ Schema registered with ID: %d\n", schemaID)

	// Step 3: Create topic
	fmt.Println("3️⃣  Creating Kafka topic...")
	topicName := "simple-schema-test"
	if err := createTopic(topicName); err != nil {
		log.Printf("⚠️  Topic creation warning: %v", err)
	} else {
		fmt.Println("✅ Topic created successfully!")
	}

	// Step 4: Produce a single schematized message
	fmt.Println("4️⃣  Producing schematized message...")
	if err := produceSchematizedMessage(topicName, schemaID); err != nil {
		log.Printf("❌ Message production failed: %v", err)
		return
	}
	fmt.Println("✅ Message produced successfully!")

	// Step 5: Wait and then try to consume
	fmt.Println("5️⃣  Waiting 5 seconds before consuming...")
	time.Sleep(5 * time.Second)

	fmt.Println("6️⃣  Attempting to consume message...")
	if err := consumeMessage(topicName); err != nil {
		log.Printf("❌ Message consumption failed: %v", err)
		return
	}
	fmt.Println("✅ Message consumed successfully!")

	fmt.Println("🎉 Simple schema test completed successfully!")
}

func checkSchemaRegistry() error {
	resp, err := http.Get("http://schema-registry:8081/config")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("Schema Registry returned status %d", resp.StatusCode)
	}

	return nil
}

func registerTestSchema() (int, error) {
	// Simple Avro schema
	avroSchema := `{
		"type": "record",
		"name": "SimpleTest",
		"fields": [
			{"name": "message", "type": "string"},
			{"name": "timestamp", "type": "long"}
		]
	}`

	schemaReq := SchemaRequest{Schema: avroSchema}
	jsonData, err := json.Marshal(schemaReq)
	if err != nil {
		return 0, err
	}

	resp, err := http.Post("http://schema-registry:8081/subjects/simple-schema-test-value/versions",
		"application/vnd.schemaregistry.v1+json", bytes.NewBuffer(jsonData))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("status=%d, body=%s", resp.StatusCode, string(body))
	}

	var schemaResp SchemaResponse
	if err := json.NewDecoder(resp.Body).Decode(&schemaResp); err != nil {
		return 0, err
	}

	return schemaResp.ID, nil
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

func produceSchematizedMessage(topicName string, schemaID int) error {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Version = sarama.V2_8_0_0

	producer, err := sarama.NewSyncProducer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		return err
	}
	defer producer.Close()

	// Create simple test message
	testMessage := map[string]interface{}{
		"message":   "Hello from schema test!",
		"timestamp": time.Now().UnixNano(),
	}

	// Create schematized message using Confluent Wire Format
	schematizedMessage, err := createSchematizedMessage(schemaID, testMessage)
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic: topicName,
		Key:   sarama.StringEncoder("test-key"),
		Value: sarama.ByteEncoder(schematizedMessage),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	fmt.Printf("   📝 Message sent: partition=%d, offset=%d\n", partition, offset)
	return nil
}

func consumeMessage(topicName string) error {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Version = sarama.V2_8_0_0

	consumer, err := sarama.NewConsumer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		return err
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topicName, 0, sarama.OffsetOldest)
	if err != nil {
		return err
	}
	defer partitionConsumer.Close()

	// Try to read one message with timeout
	timeout := time.After(10 * time.Second)

	select {
	case msg := <-partitionConsumer.Messages():
		// Decode schematized message
		decodedMsg, err := decodeSchematizedMessage(msg.Value)
		if err != nil {
			return fmt.Errorf("decode failed: %v", err)
		}

		fmt.Printf("   📖 Message received: key=%s, schema_id=%d, payload=%s\n",
			string(msg.Key), decodedMsg.SchemaID, string(decodedMsg.Payload))
		return nil

	case err := <-partitionConsumer.Errors():
		return fmt.Errorf("consumer error: %v", err)

	case <-timeout:
		return fmt.Errorf("timeout waiting for message")
	}
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

type SchematizedMessage struct {
	SchemaID int
	Payload  []byte
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


<<<<<<< Updated upstream

=======
>>>>>>> Stashed changes
