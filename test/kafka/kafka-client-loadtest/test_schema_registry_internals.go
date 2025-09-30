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

// Test Schema Registry internal behavior and _schemas topic interaction
func main() {
	fmt.Println("🔍 Testing Schema Registry Internal Behavior")
	fmt.Println(strings.Repeat("=", 50))

	// Test 1: Monitor Schema Registry logs during registration
	fmt.Println("\n1️⃣  Testing Schema Registry Response Times...")
	testSchemaRegistryResponseTimes()

	// Test 2: Monitor _schemas topic during operations
	fmt.Println("\n2️⃣  Monitoring _schemas Topic During Operations...")
	monitorSchemasTopicDuringOperations()

	// Test 3: Test Schema Registry consumer lag
	fmt.Println("\n3️⃣  Testing Schema Registry Consumer Behavior...")
	testSchemaRegistryConsumerBehavior()

	// Test 4: Test rapid sequential vs spaced registrations
	fmt.Println("\n4️⃣  Testing Registration Timing Patterns...")
	testRegistrationTimingPatterns()

	fmt.Println("\n🎯 Schema Registry Internal Tests Completed!")
}

func testSchemaRegistryResponseTimes() {
	schemas := []struct {
		subject string
		schema  string
	}{
		{"timing-test-1-value", `{"type": "string"}`},
		{"timing-test-2-value", `{"type": "int"}`},
		{"timing-test-3-value", `{"type": "long"}`},
		{"timing-test-4-value", `{"type": "double"}`},
		{"timing-test-5-value", `{"type": "boolean"}`},
	}

	fmt.Printf("   Registering %d schemas with detailed timing...\n", len(schemas))

	for i, s := range schemas {
		fmt.Printf("   Schema %d/%d: %s", i+1, len(schemas), s.subject)

		// Measure different phases
		start := time.Now()

		// Phase 1: HTTP request preparation
		payload := map[string]interface{}{
			"schema":     s.schema,
			"schemaType": "AVRO",
		}
		jsonData, _ := json.Marshal(payload)
		prepTime := time.Since(start)

		// Phase 2: HTTP request sending
		client := &http.Client{Timeout: 30 * time.Second}
		url := fmt.Sprintf("http://schema-registry:8081/subjects/%s/versions", s.subject)

		reqStart := time.Now()
		resp, err := client.Post(url, "application/vnd.schemaregistry.v1+json", bytes.NewBuffer(jsonData))
		reqTime := time.Since(reqStart)

		if err != nil {
			fmt.Printf(" ❌ Request failed: %v\n", err)
			continue
		}

		// Phase 3: Response reading
		readStart := time.Now()
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		readTime := time.Since(readStart)

		totalTime := time.Since(start)

		if resp.StatusCode == 200 {
			var result map[string]interface{}
			json.Unmarshal(body, &result)
			if id, ok := result["id"].(float64); ok {
				fmt.Printf(" ✅ ID:%d Total:%v (Prep:%v, Req:%v, Read:%v)\n",
					int(id), totalTime, prepTime, reqTime, readTime)
			}
		} else {
			fmt.Printf(" ❌ HTTP %d: %s (Total:%v)\n", resp.StatusCode, string(body), totalTime)
		}

		// Wait between registrations to see timing patterns
		time.Sleep(200 * time.Millisecond)
	}
}

func monitorSchemasTopicDuringOperations() {
	// Start monitoring _schemas topic
	go func() {
		config := sarama.NewConfig()
		config.Version = sarama.V2_6_0_0
		config.Consumer.Return.Errors = true

		consumer, err := sarama.NewConsumer([]string{"kafka-gateway:9093"}, config)
		if err != nil {
			log.Printf("Failed to create consumer: %v", err)
			return
		}
		defer consumer.Close()

		// Create a client to get offset information
		config2 := sarama.NewConfig()
		config2.Version = sarama.V2_6_0_0
		client, err := sarama.NewClient([]string{"kafka-gateway:9093"}, config2)
		if err != nil {
			log.Printf("Failed to create client: %v", err)
			return
		}
		defer client.Close()

		// Get current newest offset
		newest, err := client.GetOffset("_schemas", 0, sarama.OffsetNewest)
		if err != nil {
			log.Printf("Failed to get newest offset: %v", err)
			return
		}

		fmt.Printf("   📊 Starting to monitor _schemas from offset %d\n", newest)

		partitionConsumer, err := consumer.ConsumePartition("_schemas", 0, newest)
		if err != nil {
			log.Printf("Failed to create partition consumer: %v", err)
			return
		}
		defer partitionConsumer.Close()

		messageCount := 0
		for {
			select {
			case message := <-partitionConsumer.Messages():
				messageCount++
				fmt.Printf("   📨 _schemas[%d]: offset=%d, key_len=%d, value_len=%d, timestamp=%v\n",
					messageCount, message.Offset, len(message.Key), len(message.Value), message.Timestamp)

				// Try to decode the key if it looks like Schema Registry format
				if len(message.Key) > 0 {
					fmt.Printf("       Key: %s\n", string(message.Key))
				}

			case err := <-partitionConsumer.Errors():
				fmt.Printf("   ❌ Consumer error: %v\n", err)
				return

			case <-time.After(10 * time.Second):
				fmt.Printf("   ⏰ No more messages after 10 seconds (total: %d)\n", messageCount)
				return
			}
		}
	}()

	// Give monitor time to start
	time.Sleep(1 * time.Second)

	// Now perform schema registrations
	fmt.Printf("   Performing schema registrations while monitoring...\n")

	schemas := []string{
		"monitor-test-1-value",
		"monitor-test-2-value",
		"monitor-test-3-value",
	}

	for _, subject := range schemas {
		fmt.Printf("   Registering %s...", subject)

		start := time.Now()
		schemaID, err := registerSchemaSimple(subject, `{"type": "string"}`)
		duration := time.Since(start)

		if err != nil {
			fmt.Printf(" ❌ %v\n", err)
		} else {
			fmt.Printf(" ✅ ID:%d (%v)\n", schemaID, duration)
		}

		time.Sleep(1 * time.Second) // Wait to see _schemas topic activity
	}

	// Wait for monitoring to complete
	time.Sleep(5 * time.Second)
}

func testSchemaRegistryConsumerBehavior() {
	// Test how Schema Registry consumes from _schemas topic
	fmt.Printf("   Testing Schema Registry's internal consumer behavior...\n")

	// First, check current state
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0

	consumer, err := sarama.NewConsumer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		fmt.Printf("❌ Failed to create consumer: %v", err)
		return
	}
	defer consumer.Close()

	// Create a client to get offset information
	config2 := sarama.NewConfig()
	config2.Version = sarama.V2_6_0_0
	client, err := sarama.NewClient([]string{"kafka-gateway:9093"}, config2)
	if err != nil {
		fmt.Printf("❌ Failed to create client: %v\n", err)
		return
	}
	defer client.Close()

	oldest, _ := client.GetOffset("_schemas", 0, sarama.OffsetOldest)
	newest, _ := client.GetOffset("_schemas", 0, sarama.OffsetNewest)

	fmt.Printf("   📊 _schemas topic state: oldest=%d, newest=%d, lag=%d\n",
		oldest, newest, newest-oldest)

	// Register a schema and immediately try to read it back
	testSubject := "consumer-behavior-test-value"
	fmt.Printf("   Registering schema: %s\n", testSubject)

	regStart := time.Now()
	schemaID, err := registerSchemaSimple(testSubject, `{"type": "string"}`)
	regDuration := time.Since(regStart)

	if err != nil {
		fmt.Printf("❌ Registration failed: %v\n", err)
		return
	}

	fmt.Printf("   ✅ Schema registered: ID=%d, Duration=%v\n", schemaID, regDuration)

	// Now try to read it back immediately
	readStart := time.Now()
	retrievedSchema, err := getSchema(testSubject, "latest")
	readDuration := time.Since(readStart)

	if err != nil {
		fmt.Printf("   ❌ Immediate read failed: %v (Duration: %v)\n", err, readDuration)
	} else {
		fmt.Printf("   ✅ Immediate read succeeded: Duration=%v\n", readDuration)
		fmt.Printf("       Retrieved schema: %s\n", retrievedSchema)
	}

	// Check _schemas topic state after registration
	newNewest, _ := client.GetOffset("_schemas", 0, sarama.OffsetNewest)
	fmt.Printf("   📊 _schemas topic after registration: newest=%d, new_messages=%d\n",
		newNewest, newNewest-newest)
}

func testRegistrationTimingPatterns() {
	fmt.Printf("   Testing different registration timing patterns...\n")

	// Pattern 1: Rapid sequential (no delays)
	fmt.Printf("   🚀 Pattern 1: Rapid Sequential (no delays)\n")
	rapidSchemas := []string{"rapid-1-value", "rapid-2-value", "rapid-3-value"}

	rapidStart := time.Now()
	rapidSuccesses := 0
	for i, subject := range rapidSchemas {
		start := time.Now()
		schemaID, err := registerSchemaSimple(subject, fmt.Sprintf(`{"type": "record", "name": "Rapid%d", "fields": [{"name": "id", "type": "string"}]}`, i))
		duration := time.Since(start)

		if err != nil {
			fmt.Printf("      ❌ %s: %v (%v)\n", subject, err, duration)
		} else {
			fmt.Printf("      ✅ %s: ID=%d (%v)\n", subject, schemaID, duration)
			rapidSuccesses++
		}
	}
	rapidTotal := time.Since(rapidStart)
	fmt.Printf("      📊 Rapid: %d/%d success, Total: %v\n", rapidSuccesses, len(rapidSchemas), rapidTotal)

	time.Sleep(2 * time.Second)

	// Pattern 2: Spaced sequential (500ms delays)
	fmt.Printf("   ⏱️  Pattern 2: Spaced Sequential (500ms delays)\n")
	spacedSchemas := []string{"spaced-1-value", "spaced-2-value", "spaced-3-value"}

	spacedStart := time.Now()
	spacedSuccesses := 0
	for i, subject := range spacedSchemas {
		start := time.Now()
		schemaID, err := registerSchemaSimple(subject, fmt.Sprintf(`{"type": "record", "name": "Spaced%d", "fields": [{"name": "id", "type": "string"}]}`, i))
		duration := time.Since(start)

		if err != nil {
			fmt.Printf("      ❌ %s: %v (%v)\n", subject, err, duration)
		} else {
			fmt.Printf("      ✅ %s: ID=%d (%v)\n", subject, schemaID, duration)
			spacedSuccesses++
		}

		if i < len(spacedSchemas)-1 {
			time.Sleep(500 * time.Millisecond)
		}
	}
	spacedTotal := time.Since(spacedStart)
	fmt.Printf("      📊 Spaced: %d/%d success, Total: %v\n", spacedSuccesses, len(spacedSchemas), spacedTotal)

	time.Sleep(2 * time.Second)

	// Pattern 3: Slow sequential (2s delays)
	fmt.Printf("   🐌 Pattern 3: Slow Sequential (2s delays)\n")
	slowSchemas := []string{"slow-1-value", "slow-2-value"}

	slowStart := time.Now()
	slowSuccesses := 0
	for i, subject := range slowSchemas {
		start := time.Now()
		schemaID, err := registerSchemaSimple(subject, fmt.Sprintf(`{"type": "record", "name": "Slow%d", "fields": [{"name": "id", "type": "string"}]}`, i))
		duration := time.Since(start)

		if err != nil {
			fmt.Printf("      ❌ %s: %v (%v)\n", subject, err, duration)
		} else {
			fmt.Printf("      ✅ %s: ID=%d (%v)\n", subject, schemaID, duration)
			slowSuccesses++
		}

		if i < len(slowSchemas)-1 {
			time.Sleep(2 * time.Second)
		}
	}
	slowTotal := time.Since(slowStart)
	fmt.Printf("      📊 Slow: %d/%d success, Total: %v\n", slowSuccesses, len(slowSchemas), slowTotal)
}

func registerSchemaSimple(subject, schema string) (int, error) {
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
