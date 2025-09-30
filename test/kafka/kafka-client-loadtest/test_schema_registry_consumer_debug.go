package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

// Debug Schema Registry's consumer behavior in detail
func main() {
	fmt.Println("🔍 Debugging Schema Registry Consumer Read-After-Write Issue")
	fmt.Println(strings.Repeat("=", 60))

	// Test 1: Monitor Schema Registry consumer during registration
	fmt.Println("\n1️⃣  Monitoring Schema Registry Consumer During Registration...")
	testSchemaRegistryConsumerTiming()

	// Test 2: Test immediate read-back timing
	fmt.Println("\n2️⃣  Testing Immediate Read-Back Timing...")
	testImmediateReadBack()

	// Test 3: Monitor Kafka Gateway fetch responses for Schema Registry
	fmt.Println("\n3️⃣  Monitoring Kafka Gateway Fetch Responses...")
	testKafkaGatewayFetchResponses()

	// Test 4: Test offset consistency
	fmt.Println("\n4️⃣  Testing Offset Consistency...")
	testOffsetConsistency()

	fmt.Println("\n🎯 Schema Registry Consumer Debug Completed!")
}

func testSchemaRegistryConsumerTiming() {
	// Start monitoring _schemas topic before registration
	go monitorSchemasTopicActivity()

	// Give monitor time to start
	time.Sleep(1 * time.Second)

	// Get current state
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	client, err := sarama.NewClient([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		fmt.Printf("❌ Failed to create client: %v\n", err)
		return
	}
	defer client.Close()

	beforeOldest, _ := client.GetOffset("_schemas", 0, sarama.OffsetOldest)
	beforeNewest, _ := client.GetOffset("_schemas", 0, sarama.OffsetNewest)

	fmt.Printf("   📊 Before registration: oldest=%d, newest=%d, available=%d\n",
		beforeOldest, beforeNewest, beforeNewest-beforeOldest)

	// Register a schema
	subject := "consumer-debug-test-value"
	schema := `{"type": "record", "name": "ConsumerDebugTest", "fields": [{"name": "id", "type": "string"}]}`

	fmt.Printf("   🔄 Registering schema: %s\n", subject)
	regStart := time.Now()
	schemaID, err := registerSchema(subject, schema)
	regDuration := time.Since(regStart)

	if err != nil {
		fmt.Printf("❌ Registration failed: %v\n", err)
		return
	}

	fmt.Printf("   ✅ Schema registered: ID=%d, Duration=%v\n", schemaID, regDuration)

	// Check offset state immediately after registration
	afterNewest, _ := client.GetOffset("_schemas", 0, sarama.OffsetNewest)
	fmt.Printf("   📊 After registration: newest=%d, new_messages=%d\n",
		afterNewest, afterNewest-beforeNewest)

	// Try to read back immediately with different timing intervals
	intervals := []time.Duration{0, 10 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 500 * time.Millisecond}

	for _, interval := range intervals {
		if interval > 0 {
			time.Sleep(interval)
		}

		readStart := time.Now()
		retrievedSchema, err := getSchema(subject, "latest")
		readDuration := time.Since(readStart)

		if err != nil {
			fmt.Printf("   ❌ Read-back after %v: FAILED (%v) - %v\n", interval, readDuration, err)
		} else {
			fmt.Printf("   ✅ Read-back after %v: SUCCESS (%v)\n", interval, readDuration)
			fmt.Printf("       Retrieved: %s\n", retrievedSchema[:min(50, len(retrievedSchema))]+"...")
			break
		}
	}

	// Wait for monitoring to complete
	time.Sleep(3 * time.Second)
}

func testImmediateReadBack() {
	fmt.Printf("   Testing multiple immediate read-backs...\n")

	for i := 0; i < 3; i++ {
		subject := fmt.Sprintf("immediate-test-%d-value", i)
		schema := fmt.Sprintf(`{"type": "record", "name": "ImmediateTest%d", "fields": [{"name": "id", "type": "string"}]}`, i)

		fmt.Printf("   Test %d: %s\n", i+1, subject)

		// Register
		regStart := time.Now()
		schemaID, err := registerSchema(subject, schema)
		regDuration := time.Since(regStart)

		if err != nil {
			fmt.Printf("      ❌ Registration failed: %v\n", err)
			continue
		}

		// Immediate read-back
		readStart := time.Now()
		_, err = getSchema(subject, "latest")
		readDuration := time.Since(readStart)

		fmt.Printf("      Registration: ID=%d (%v)\n", schemaID, regDuration)
		if err != nil {
			fmt.Printf("      ❌ Immediate read: FAILED (%v) - %v\n", readDuration, err)
		} else {
			fmt.Printf("      ✅ Immediate read: SUCCESS (%v)\n", readDuration)
		}

		time.Sleep(200 * time.Millisecond)
	}
}

func testKafkaGatewayFetchResponses() {
	fmt.Printf("   Monitoring Kafka Gateway fetch behavior for Schema Registry...\n")

	// Create a consumer that mimics Schema Registry's behavior
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.Consumer.Return.Errors = true
	config.ClientID = "schema-registry-debug-consumer"

	consumer, err := sarama.NewConsumer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		fmt.Printf("❌ Failed to create consumer: %v\n", err)
		return
	}
	defer consumer.Close()

	// Get current newest offset
	client, err := sarama.NewClient([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		fmt.Printf("❌ Failed to create client: %v\n", err)
		return
	}
	defer client.Close()

	startOffset, err := client.GetOffset("_schemas", 0, sarama.OffsetNewest)
	if err != nil {
		fmt.Printf("❌ Failed to get starting offset: %v\n", err)
		return
	}

	fmt.Printf("   📊 Starting consumer from offset %d\n", startOffset)

	// Start consuming
	partitionConsumer, err := consumer.ConsumePartition("_schemas", 0, startOffset)
	if err != nil {
		fmt.Printf("❌ Failed to create partition consumer: %v\n", err)
		return
	}
	defer partitionConsumer.Close()

	// Monitor in background
	go func() {
		messageCount := 0
		for {
			select {
			case message := <-partitionConsumer.Messages():
				messageCount++
				fmt.Printf("   📨 Consumer received[%d]: offset=%d, timestamp=%v, key_len=%d, value_len=%d\n",
					messageCount, message.Offset, message.Timestamp, len(message.Key), len(message.Value))

				// Try to decode the key
				if len(message.Key) > 0 {
					fmt.Printf("       Key: %s\n", string(message.Key))
				}

			case err := <-partitionConsumer.Errors():
				fmt.Printf("   ❌ Consumer error: %v\n", err)
				return

			case <-time.After(10 * time.Second):
				fmt.Printf("   ⏰ Consumer monitoring finished (received %d messages)\n", messageCount)
				return
			}
		}
	}()

	// Give consumer time to start
	time.Sleep(500 * time.Millisecond)

	// Now register a schema while monitoring
	subject := "fetch-response-test-value"
	schema := `{"type": "record", "name": "FetchResponseTest", "fields": [{"name": "id", "type": "string"}]}`

	fmt.Printf("   🔄 Registering schema while monitoring fetch responses...\n")
	schemaID, err := registerSchema(subject, schema)
	if err != nil {
		fmt.Printf("❌ Registration failed: %v\n", err)
	} else {
		fmt.Printf("   ✅ Schema registered: ID=%d\n", schemaID)
	}

	// Wait for monitoring to complete
	time.Sleep(8 * time.Second)
}

func testOffsetConsistency() {
	fmt.Printf("   Testing offset consistency during registration...\n")

	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	client, err := sarama.NewClient([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		fmt.Printf("❌ Failed to create client: %v\n", err)
		return
	}
	defer client.Close()

	// Get initial state
	initialOldest, _ := client.GetOffset("_schemas", 0, sarama.OffsetOldest)
	initialNewest, _ := client.GetOffset("_schemas", 0, sarama.OffsetNewest)

	fmt.Printf("   📊 Initial: oldest=%d, newest=%d, available=%d\n",
		initialOldest, initialNewest, initialNewest-initialOldest)

	// Register schema and track offset changes
	subject := "offset-consistency-test-value"
	schema := `{"type": "record", "name": "OffsetConsistencyTest", "fields": [{"name": "id", "type": "string"}]}`

	fmt.Printf("   🔄 Registering schema and tracking offsets...\n")

	// Check offsets before registration
	beforeNewest, _ := client.GetOffset("_schemas", 0, sarama.OffsetNewest)
	fmt.Printf("      Before registration: newest=%d\n", beforeNewest)

	// Register schema
	regStart := time.Now()
	schemaID, err := registerSchema(subject, schema)
	regDuration := time.Since(regStart)

	if err != nil {
		fmt.Printf("❌ Registration failed: %v\n", err)
		return
	}

	// Check offsets immediately after registration
	afterNewest, _ := client.GetOffset("_schemas", 0, sarama.OffsetNewest)
	fmt.Printf("      After registration: newest=%d, added=%d, duration=%v\n",
		afterNewest, afterNewest-beforeNewest, regDuration)

	// Check if we can consume the new message immediately
	if afterNewest > beforeNewest {
		fmt.Printf("   🔄 Attempting to consume new message at offset %d...\n", beforeNewest)

		consumer, err := sarama.NewConsumer([]string{"kafka-gateway:9093"}, config)
		if err != nil {
			fmt.Printf("❌ Failed to create consumer: %v\n", err)
			return
		}
		defer consumer.Close()

		partitionConsumer, err := consumer.ConsumePartition("_schemas", 0, beforeNewest)
		if err != nil {
			fmt.Printf("❌ Failed to create partition consumer: %v\n", err)
			return
		}
		defer partitionConsumer.Close()

		select {
		case message := <-partitionConsumer.Messages():
			fmt.Printf("   ✅ Successfully consumed new message: offset=%d, key_len=%d, value_len=%d\n",
				message.Offset, len(message.Key), len(message.Value))

		case err := <-partitionConsumer.Errors():
			fmt.Printf("   ❌ Consumer error: %v\n", err)

		case <-time.After(2 * time.Second):
			fmt.Printf("   ❌ Timeout waiting for new message\n")
		}
	}

	fmt.Printf("   📊 Schema ID registered: %d\n", schemaID)
}

func monitorSchemasTopicActivity() {
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.Consumer.Return.Errors = true
	config.ClientID = "schemas-topic-monitor"

	consumer, err := sarama.NewConsumer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Printf("Monitor: Failed to create consumer: %v", err)
		return
	}
	defer consumer.Close()

	client, err := sarama.NewClient([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Printf("Monitor: Failed to create client: %v", err)
		return
	}
	defer client.Close()

	// Start from current newest
	startOffset, err := client.GetOffset("_schemas", 0, sarama.OffsetNewest)
	if err != nil {
		log.Printf("Monitor: Failed to get starting offset: %v", err)
		return
	}

	fmt.Printf("   📊 Monitor: Starting from offset %d\n", startOffset)

	partitionConsumer, err := consumer.ConsumePartition("_schemas", 0, startOffset)
	if err != nil {
		log.Printf("Monitor: Failed to create partition consumer: %v", err)
		return
	}
	defer partitionConsumer.Close()

	messageCount := 0
	for {
		select {
		case message := <-partitionConsumer.Messages():
			messageCount++
			fmt.Printf("   📨 Monitor[%d]: offset=%d, timestamp=%v, key=%s\n",
				messageCount, message.Offset, message.Timestamp, string(message.Key))

		case err := <-partitionConsumer.Errors():
			fmt.Printf("   ❌ Monitor error: %v\n", err)
			return

		case <-time.After(8 * time.Second):
			fmt.Printf("   ⏰ Monitor finished (received %d messages)\n", messageCount)
			return
		}
	}
}

func registerSchema(subject, schema string) (int, error) {
	payload := map[string]interface{}{
		"schema":     schema,
		"schemaType": "AVRO",
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}

	client := &http.Client{Timeout: 30 * time.Second}
	url := fmt.Sprintf("http://schema-registry:8081/subjects/%s/versions", subject)

	resp, err := client.Post(url, "application/vnd.schemaregistry.v1+json", bytes.NewBuffer(jsonData))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	if resp.StatusCode != 200 {
		return 0, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, err
	}

	if id, ok := result["id"].(float64); ok {
		return int(id), nil
	}

	return 0, fmt.Errorf("invalid response format: %s", string(body))
}

func getSchema(subject, version string) (string, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	url := fmt.Sprintf("http://schema-registry:8081/subjects/%s/versions/%s", subject, version)

	resp, err := client.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", err
	}

	if schema, ok := result["schema"].(string); ok {
		return schema, nil
	}

	return "", fmt.Errorf("invalid response format: %s", string(body))
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

