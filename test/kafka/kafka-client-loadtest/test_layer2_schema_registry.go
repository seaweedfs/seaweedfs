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

func main() {
	fmt.Println("🧪 Layer 2: Schema Registry Integration Test")
	fmt.Println("Testing: Schema registration, retrieval, Confluent Wire Format")

	// Test 1: Schema Registry connectivity and basic operations
	testSchemaRegistryBasics()

	// Test 2: Schema registration and retrieval
	testSchemaRegistration()

	// Test 3: Confluent Wire Format message production
	testConfluentWireFormat()
}

func testSchemaRegistryBasics() {
	fmt.Println("\n📋 Test 2.1: Schema Registry Basic Operations")

	// Test connectivity
	resp, err := http.Get("http://schema-registry:8081/subjects")
	if err != nil {
		log.Printf("❌ Failed to connect to Schema Registry: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		fmt.Printf("✅ Schema Registry is accessible\n")
	} else {
		fmt.Printf("❌ Schema Registry returned status: %d\n", resp.StatusCode)
		return
	}

	// List existing subjects
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("❌ Failed to read response: %v", err)
		return
	}

	var subjects []string
	if err := json.Unmarshal(body, &subjects); err != nil {
		log.Printf("❌ Failed to parse subjects: %v", err)
		return
	}

	fmt.Printf("📋 Existing subjects: %v\n", subjects)
}

func testSchemaRegistration() {
	fmt.Println("\n📋 Test 2.2: Schema Registration and Retrieval")

	// Define test schema
	testSubject := fmt.Sprintf("test-value-%d", time.Now().Unix())
	testSchema := `{
		"type": "record",
		"name": "TestRecord",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"},
			{"name": "timestamp", "type": "long"}
		]
	}`

	// Register schema
	schemaRequest := map[string]interface{}{
		"schema": testSchema,
	}

	requestBody, err := json.Marshal(schemaRequest)
	if err != nil {
		log.Printf("❌ Failed to marshal schema request: %v", err)
		return
	}

	url := fmt.Sprintf("http://schema-registry:8081/subjects/%s/versions", testSubject)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		log.Printf("❌ Failed to register schema: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		fmt.Printf("✅ Schema registered successfully\n")
	} else {
		body, _ := io.ReadAll(resp.Body)
		log.Printf("❌ Schema registration failed: %d - %s", resp.StatusCode, string(body))
		return
	}

	// Parse registration response to get schema ID
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("❌ Failed to read registration response: %v", err)
		return
	}

	var regResponse map[string]interface{}
	if err := json.Unmarshal(body, &regResponse); err != nil {
		log.Printf("❌ Failed to parse registration response: %v", err)
		return
	}

	schemaID, ok := regResponse["id"].(float64)
	if !ok {
		log.Printf("❌ Failed to get schema ID from response: %v", regResponse)
		return
	}

	fmt.Printf("📋 Schema registered with ID: %.0f\n", schemaID)

	// Retrieve schema by ID
	getURL := fmt.Sprintf("http://schema-registry:8081/schemas/ids/%.0f", schemaID)
	getResp, err := http.Get(getURL)
	if err != nil {
		log.Printf("❌ Failed to retrieve schema: %v", err)
		return
	}
	defer getResp.Body.Close()

	if getResp.StatusCode == 200 {
		fmt.Printf("✅ Schema retrieved successfully\n")

		getBody, _ := io.ReadAll(getResp.Body)
		var getResponse map[string]interface{}
		if err := json.Unmarshal(getBody, &getResponse); err == nil {
			if retrievedSchema, ok := getResponse["schema"].(string); ok {
				fmt.Printf("📋 Retrieved schema matches: %t\n", retrievedSchema == testSchema)
			}
		}
	} else {
		fmt.Printf("❌ Failed to retrieve schema: %d\n", getResp.StatusCode)
	}
}

func testConfluentWireFormat() {
	fmt.Println("\n📋 Test 2.3: Confluent Wire Format Message Production")

	// First register a simple schema
	testSubject := fmt.Sprintf("confluent-test-value-%d", time.Now().Unix())
	testSchema := `{
		"type": "record",
		"name": "ConfluentTest",
		"fields": [
			{"name": "message", "type": "string"},
			{"name": "timestamp", "type": "long"}
		]
	}`

	// Register schema
	schemaRequest := map[string]interface{}{
		"schema": testSchema,
	}

	requestBody, err := json.Marshal(schemaRequest)
	if err != nil {
		log.Printf("❌ Failed to marshal schema request: %v", err)
		return
	}

	url := fmt.Sprintf("http://schema-registry:8081/subjects/%s/versions", testSubject)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		log.Printf("❌ Failed to register schema: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		log.Printf("❌ Schema registration failed: %d - %s", resp.StatusCode, string(body))
		return
	}

	// Get schema ID
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("❌ Failed to read registration response: %v", err)
		return
	}

	var regResponse map[string]interface{}
	if err := json.Unmarshal(body, &regResponse); err != nil {
		log.Printf("❌ Failed to parse registration response: %v", err)
		return
	}

	schemaID, ok := regResponse["id"].(float64)
	if !ok {
		log.Printf("❌ Failed to get schema ID: %v", regResponse)
		return
	}

	fmt.Printf("📋 Schema ID for Confluent Wire Format: %.0f\n", schemaID)

	// Create Confluent Wire Format message
	// Format: [magic_byte][schema_id][avro_payload]
	// Magic byte: 0x0
	// Schema ID: 4 bytes big-endian
	// Payload: Avro-encoded data (simplified as JSON for this test)

	magicByte := byte(0x0)
	schemaIDBytes := make([]byte, 4)
	schemaIDBytes[0] = byte(int(schemaID) >> 24)
	schemaIDBytes[1] = byte(int(schemaID) >> 16)
	schemaIDBytes[2] = byte(int(schemaID) >> 8)
	schemaIDBytes[3] = byte(int(schemaID))

	// Simplified payload (in real scenario, this would be Avro-encoded)
	payload := []byte(`{"message": "test confluent wire format", "timestamp": ` + fmt.Sprintf("%d", time.Now().UnixMilli()) + `}`)

	// Combine into Confluent Wire Format
	confluentMessage := make([]byte, 0, 1+4+len(payload))
	confluentMessage = append(confluentMessage, magicByte)
	confluentMessage = append(confluentMessage, schemaIDBytes...)
	confluentMessage = append(confluentMessage, payload...)

	fmt.Printf("📋 Created Confluent Wire Format message: %d bytes\n", len(confluentMessage))
	fmt.Printf("   Magic byte: 0x%02x\n", magicByte)
	fmt.Printf("   Schema ID: %.0f\n", schemaID)
	fmt.Printf("   Payload size: %d bytes\n", len(payload))

	// Produce the message to Kafka
	topicName := fmt.Sprintf("confluent-test-%d", time.Now().Unix())

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Version = sarama.V2_8_0_0

	producer, err := sarama.NewSyncProducer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Printf("❌ Failed to create producer: %v", err)
		return
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topicName,
		Key:   sarama.StringEncoder("confluent-key"),
		Value: sarama.ByteEncoder(confluentMessage),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Printf("❌ Failed to produce Confluent Wire Format message: %v", err)
		return
	}

	fmt.Printf("✅ Confluent Wire Format message produced: partition=%d, offset=%d\n", partition, offset)

	// TODO: In a complete test, we would consume this message back and verify
	// that the Kafka Gateway correctly handles the Confluent Wire Format
	// and converts it to protobuf RecordValue for storage in SeaweedMQ
}

