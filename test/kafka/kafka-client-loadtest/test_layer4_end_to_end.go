package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("🧪 Layer 4: End-to-End Integration Test")
	fmt.Println("Testing: Complete workflow from schema registration to message consumption")

	// Test 1: Complete schematized workflow
	testCompleteSchematizedWorkflow()

	// Test 2: Schema evolution
	testSchemaEvolution()

	// Test 3: Error handling
	testErrorHandling()
}

func testCompleteSchematizedWorkflow() {
	fmt.Println("\n📋 Test 4.1: Complete Schematized Workflow")

	// Step 1: Register schema
	testSubject := fmt.Sprintf("e2e-test-value-%d", time.Now().Unix())
	testSchema := `{
		"type": "record",
		"name": "E2ETest",
		"fields": [
			{"name": "user_id", "type": "int"},
			{"name": "action", "type": "string"},
			{"name": "timestamp", "type": "long"},
			{"name": "metadata", "type": ["null", "string"], "default": null}
		]
	}`

	schemaID, err := registerSchema(testSubject, testSchema)
	if err != nil {
		log.Printf("❌ Failed to register schema: %v", err)
		return
	}

	fmt.Printf("✅ Schema registered with ID: %d\n", schemaID)

	// Step 2: Create topic
	topicName := fmt.Sprintf("e2e-test-%d", time.Now().Unix())

	// Step 3: Produce schematized message
	err = produceSchematizedMessage(topicName, schemaID, map[string]interface{}{
		"user_id":   12345,
		"action":    "login",
		"timestamp": time.Now().UnixMilli(),
		"metadata":  "test metadata",
	})
	if err != nil {
		log.Printf("❌ Failed to produce schematized message: %v", err)
		return
	}

	fmt.Printf("✅ Schematized message produced\n")

	// Step 4: Consume and verify message
	err = consumeAndVerifyMessage(topicName, schemaID)
	if err != nil {
		log.Printf("❌ Failed to consume/verify message: %v", err)
		return
	}

	fmt.Printf("✅ Message consumed and verified\n")

	// Step 5: Verify storage format
	err = verifyStorageFormat(topicName)
	if err != nil {
		log.Printf("❌ Failed to verify storage format: %v", err)
		return
	}

	fmt.Printf("✅ Storage format verified\n")
}

func registerSchema(subject, schema string) (int, error) {
	schemaRequest := map[string]interface{}{
		"schema": schema,
	}

	requestBody, err := json.Marshal(schemaRequest)
	if err != nil {
		return 0, err
	}

	url := fmt.Sprintf("http://schema-registry:8081/subjects/%s/versions", subject)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("registration failed: %d - %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	var regResponse map[string]interface{}
	if err := json.Unmarshal(body, &regResponse); err != nil {
		return 0, err
	}

	schemaID, ok := regResponse["id"].(float64)
	if !ok {
		return 0, fmt.Errorf("failed to get schema ID from response")
	}

	return int(schemaID), nil
}

func produceSchematizedMessage(topicName string, schemaID int, data map[string]interface{}) error {
	// Create Confluent Wire Format message
	magicByte := byte(0x0)
	schemaIDBytes := make([]byte, 4)
	schemaIDBytes[0] = byte(schemaID >> 24)
	schemaIDBytes[1] = byte(schemaID >> 16)
	schemaIDBytes[2] = byte(schemaID >> 8)
	schemaIDBytes[3] = byte(schemaID)

	// Simplified Avro payload (in real scenario, this would be properly Avro-encoded)
	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}

	// Combine into Confluent Wire Format
	confluentMessage := make([]byte, 0, 1+4+len(payload))
	confluentMessage = append(confluentMessage, magicByte)
	confluentMessage = append(confluentMessage, schemaIDBytes...)
	confluentMessage = append(confluentMessage, payload...)

	// Produce to Kafka
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Version = sarama.V2_8_0_0

	producer, err := sarama.NewSyncProducer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		return err
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topicName,
		Key:   sarama.StringEncoder(fmt.Sprintf("user-%d", data["user_id"])),
		Value: sarama.ByteEncoder(confluentMessage),
	}

	_, _, err = producer.SendMessage(msg)
	return err
}

func consumeAndVerifyMessage(topicName string, expectedSchemaID int) error {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	select {
	case message := <-partitionConsumer.Messages():
		fmt.Printf("📋 Consumed message: offset=%d, timestamp=%s\n",
			message.Offset, message.Timestamp.Format(time.RFC3339))

		// Verify timestamp is reasonable
		now := time.Now()
		timeDiff := now.Sub(message.Timestamp)
		if timeDiff < 0 {
			timeDiff = -timeDiff
		}

		if timeDiff > 1*time.Hour {
			return fmt.Errorf("timestamp appears incorrect: %s (diff: %v)",
				message.Timestamp.Format(time.RFC3339), timeDiff)
		}

		// Verify Confluent Wire Format
		if len(message.Value) < 5 {
			return fmt.Errorf("message too short to be Confluent Wire Format")
		}

		if message.Value[0] != 0x0 {
			return fmt.Errorf("invalid magic byte: expected 0x0, got 0x%02x", message.Value[0])
		}

		// Extract schema ID
		schemaIDBytes := message.Value[1:5]
		receivedSchemaID := int(schemaIDBytes[0])<<24 |
			int(schemaIDBytes[1])<<16 |
			int(schemaIDBytes[2])<<8 |
			int(schemaIDBytes[3])

		if receivedSchemaID != expectedSchemaID {
			return fmt.Errorf("schema ID mismatch: expected %d, got %d",
				expectedSchemaID, receivedSchemaID)
		}

		fmt.Printf("✅ Confluent Wire Format verified: schema ID %d\n", receivedSchemaID)

		// Verify payload
		payload := message.Value[5:]
		var data map[string]interface{}
		if err := json.Unmarshal(payload, &data); err != nil {
			return fmt.Errorf("failed to parse payload: %v", err)
		}

		if userID, ok := data["user_id"].(float64); ok && userID == 12345 {
			fmt.Printf("✅ Payload verified: user_id=%v\n", userID)
		} else {
			return fmt.Errorf("payload verification failed: user_id=%v", data["user_id"])
		}

		return nil

	case err := <-partitionConsumer.Errors():
		return fmt.Errorf("consumer error: %v", err)

	case <-ctx.Done():
		return fmt.Errorf("timeout waiting for message")
	}
}

func verifyStorageFormat(topicName string) error {
	// Wait for storage to be written
	time.Sleep(2 * time.Second)

	// Check if topic directory exists
	topicURL := fmt.Sprintf("http://localhost:8888/topics/kafka/%s/", topicName)
	resp, err := http.Get(topicURL)
	if err != nil {
		return fmt.Errorf("failed to access topic directory: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		fmt.Printf("✅ Topic stored in SeaweedMQ\n")
		return nil
	} else {
		return fmt.Errorf("topic not found in SeaweedMQ: %d", resp.StatusCode)
	}
}

func testSchemaEvolution() {
	fmt.Println("\n📋 Test 4.2: Schema Evolution")

	subject := fmt.Sprintf("evolution-test-%d", time.Now().Unix())

	// Register v1 schema
	schemaV1 := `{
		"type": "record",
		"name": "EvolutionTest",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"}
		]
	}`

	schemaIDV1, err := registerSchema(subject, schemaV1)
	if err != nil {
		log.Printf("❌ Failed to register schema v1: %v", err)
		return
	}

	fmt.Printf("✅ Schema v1 registered: ID %d\n", schemaIDV1)

	// Register v2 schema (backward compatible - added optional field)
	schemaV2 := `{
		"type": "record",
		"name": "EvolutionTest",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"},
			{"name": "email", "type": ["null", "string"], "default": null}
		]
	}`

	schemaIDV2, err := registerSchema(subject, schemaV2)
	if err != nil {
		log.Printf("❌ Failed to register schema v2: %v", err)
		return
	}

	fmt.Printf("✅ Schema v2 registered: ID %d\n", schemaIDV2)

	if schemaIDV2 > schemaIDV1 {
		fmt.Printf("✅ Schema evolution working: v1=%d, v2=%d\n", schemaIDV1, schemaIDV2)
	} else {
		fmt.Printf("❌ Schema evolution issue: v1=%d, v2=%d\n", schemaIDV1, schemaIDV2)
	}
}

func testErrorHandling() {
	fmt.Println("\n📋 Test 4.3: Error Handling")

	// Test 1: Invalid schema registration
	invalidSchema := `{"invalid": "schema"}`
	_, err := registerSchema("invalid-test", invalidSchema)
	if err != nil {
		fmt.Printf("✅ Invalid schema correctly rejected: %v\n", err)
	} else {
		fmt.Printf("❌ Invalid schema was accepted\n")
	}

	// Test 2: Produce to non-existent topic with schema
	err = produceSchematizedMessage("non-existent-topic", 99999, map[string]interface{}{
		"test": "data",
	})
	if err != nil {
		fmt.Printf("✅ Production to non-existent topic handled: %v\n", err)
	} else {
		fmt.Printf("❌ Production to non-existent topic succeeded unexpectedly\n")
	}

	fmt.Printf("✅ Error handling tests completed\n")
}

