package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// Test configuration
const (
	kafkaGatewayAddr    = "kafka-gateway:9093"
	schemaRegistryAddr  = "http://schema-registry:8081"
	testTimeoutDuration = 30 * time.Second
)

// Schema definitions
var testSchemas = map[string]string{
	"user-value": `{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "id", "type": "string"},
			{"name": "name", "type": "string"},
			{"name": "email", "type": "string"},
			{"name": "created_at", "type": "long"}
		]
	}`,
	"user-key": `{"type": "string"}`,
	"order-value": `{
		"type": "record",
		"name": "Order",
		"fields": [
			{"name": "order_id", "type": "string"},
			{"name": "user_id", "type": "string"},
			{"name": "amount", "type": "double"},
			{"name": "timestamp", "type": "long"}
		]
	}`,
	"order-key": `{"type": "string"}`,
	"product-value": `{
		"type": "record",
		"name": "Product",
		"fields": [
			{"name": "product_id", "type": "string"},
			{"name": "name", "type": "string"},
			{"name": "price", "type": "double"},
			{"name": "category", "type": "string"}
		]
	}`,
	"product-key": `{"type": "string"}`,
}

type SchemaRegistrationResult struct {
	Subject  string
	Success  bool
	SchemaID int
	Duration time.Duration
	Error    error
}

type TestResults struct {
	TotalSchemas   int
	SuccessfulRegs int
	FailedRegs     int
	AverageLatency time.Duration
	MaxLatency     time.Duration
	MinLatency     time.Duration
	Results        []SchemaRegistrationResult
}

func main() {
	fmt.Println("🔧 Testing Bulk Schema Operations - Layer by Layer Debug")
	fmt.Println(strings.Repeat("=", 60))

	// Layer 1: Test Schema Registry availability
	fmt.Println("\n1️⃣  Layer 1: Testing Schema Registry Availability...")
	if !testSchemaRegistryHealth() {
		log.Fatal("❌ Schema Registry is not available")
	}
	fmt.Println("✅ Schema Registry is healthy")

	// Layer 2: Test Kafka Gateway connectivity
	fmt.Println("\n2️⃣  Layer 2: Testing Kafka Gateway Connectivity...")
	if !testKafkaGatewayHealth() {
		log.Fatal("❌ Kafka Gateway is not available")
	}
	fmt.Println("✅ Kafka Gateway is healthy")

	// Layer 3: Test single schema registration
	fmt.Println("\n3️⃣  Layer 3: Testing Single Schema Registration...")
	singleResult := testSingleSchemaRegistration()
	if !singleResult.Success {
		log.Printf("❌ Single schema registration failed: %v", singleResult.Error)
	} else {
		fmt.Printf("✅ Single schema registered successfully (ID: %d, Duration: %v)\n",
			singleResult.SchemaID, singleResult.Duration)
	}

	// Layer 4: Test sequential schema registrations
	fmt.Println("\n4️⃣  Layer 4: Testing Sequential Schema Registrations...")
	sequentialResults := testSequentialSchemaRegistrations()
	printTestResults("Sequential", sequentialResults)

	// Layer 5: Test concurrent schema registrations
	fmt.Println("\n5️⃣  Layer 5: Testing Concurrent Schema Registrations...")
	concurrentResults := testConcurrentSchemaRegistrations()
	printTestResults("Concurrent", concurrentResults)

	// Layer 6: Test Kafka Gateway produce timing during schema operations
	fmt.Println("\n6️⃣  Layer 6: Testing Kafka Gateway Produce Timing...")
	testKafkaGatewayProduceTiming()

	// Layer 7: Test _schemas topic directly
	fmt.Println("\n7️⃣  Layer 7: Testing _schemas Topic Performance...")
	testSchemasTopicPerformance()

	fmt.Println("\n🎯 Bulk Schema Debug Tests Completed!")
}

func testSchemaRegistryHealth() bool {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(schemaRegistryAddr + "/subjects")
	if err != nil {
		log.Printf("Schema Registry health check failed: %v", err)
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == 200
}

func testKafkaGatewayHealth() bool {
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.Net.DialTimeout = 5 * time.Second

	client, err := sarama.NewClient([]string{kafkaGatewayAddr}, config)
	if err != nil {
		log.Printf("Kafka Gateway health check failed: %v", err)
		return false
	}
	defer client.Close()

	_, err = client.Topics()
	return err == nil
}

func testSingleSchemaRegistration() SchemaRegistrationResult {
	subject := "test-single-value"
	schema := testSchemas["user-value"]

	start := time.Now()
	schemaID, err := registerSchemaDebug(subject, schema)
	duration := time.Since(start)

	return SchemaRegistrationResult{
		Subject:  subject,
		Success:  err == nil,
		SchemaID: schemaID,
		Duration: duration,
		Error:    err,
	}
}

func testSequentialSchemaRegistrations() TestResults {
	results := TestResults{
		Results: make([]SchemaRegistrationResult, 0),
	}

	subjects := []string{"user-value", "user-key", "order-value", "order-key", "product-value", "product-key"}

	for i, subject := range subjects {
		schema := testSchemas[subject]

		fmt.Printf("   Registering schema %d/%d: %s...", i+1, len(subjects), subject)

		start := time.Now()
		schemaID, err := registerSchemaDebug(fmt.Sprintf("test-seq-%s", subject), schema)
		duration := time.Since(start)

		result := SchemaRegistrationResult{
			Subject:  subject,
			Success:  err == nil,
			SchemaID: schemaID,
			Duration: duration,
			Error:    err,
		}

		results.Results = append(results.Results, result)
		results.TotalSchemas++

		if err == nil {
			results.SuccessfulRegs++
			fmt.Printf(" ✅ (ID: %d, %v)\n", schemaID, duration)
		} else {
			results.FailedRegs++
			fmt.Printf(" ❌ (%v)\n", err)
		}

		// Small delay between sequential registrations
		time.Sleep(100 * time.Millisecond)
	}

	calculateStats(&results)
	return results
}

func testConcurrentSchemaRegistrations() TestResults {
	results := TestResults{
		Results: make([]SchemaRegistrationResult, 0),
	}

	subjects := []string{"user-value", "user-key", "order-value", "order-key", "product-value", "product-key"}

	var wg sync.WaitGroup
	var mu sync.Mutex

	fmt.Printf("   Starting %d concurrent registrations...\n", len(subjects))

	for i, subject := range subjects {
		wg.Add(1)
		go func(idx int, subj string) {
			defer wg.Done()

			schema := testSchemas[subj]

			start := time.Now()
			schemaID, err := registerSchemaDebug(fmt.Sprintf("test-conc-%s", subj), schema)
			duration := time.Since(start)

			result := SchemaRegistrationResult{
				Subject:  subj,
				Success:  err == nil,
				SchemaID: schemaID,
				Duration: duration,
				Error:    err,
			}

			mu.Lock()
			results.Results = append(results.Results, result)
			results.TotalSchemas++
			if err == nil {
				results.SuccessfulRegs++
				fmt.Printf("   ✅ %s: ID %d (%v)\n", subj, schemaID, duration)
			} else {
				results.FailedRegs++
				fmt.Printf("   ❌ %s: %v\n", subj, err)
			}
			mu.Unlock()
		}(i, subject)
	}

	wg.Wait()
	calculateStats(&results)
	return results
}

func testKafkaGatewayProduceTiming() {
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Timeout = 10 * time.Second

	producer, err := sarama.NewSyncProducer([]string{kafkaGatewayAddr}, config)
	if err != nil {
		fmt.Printf("❌ Failed to create producer: %v\n", err)
		return
	}
	defer producer.Close()

	// Test produce timing to _schemas topic
	topic := "_schemas"
	fmt.Printf("   Testing produce timing to %s topic...\n", topic)

	timings := make([]time.Duration, 0, 10)

	for i := 0; i < 10; i++ {
		message := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(fmt.Sprintf("test-key-%d", i)),
			Value: sarama.StringEncoder(fmt.Sprintf(`{"test": "message-%d"}`, i)),
		}

		start := time.Now()
		partition, offset, err := producer.SendMessage(message)
		duration := time.Since(start)

		timings = append(timings, duration)

		if err != nil {
			fmt.Printf("   ❌ Message %d failed: %v\n", i+1, err)
		} else {
			fmt.Printf("   ✅ Message %d: partition %d, offset %d (%v)\n",
				i+1, partition, offset, duration)
		}

		time.Sleep(100 * time.Millisecond)
	}

	// Calculate timing statistics
	var total time.Duration
	var min, max time.Duration = timings[0], timings[0]

	for _, t := range timings {
		total += t
		if t < min {
			min = t
		}
		if t > max {
			max = t
		}
	}

	avg := total / time.Duration(len(timings))
	fmt.Printf("   📊 Produce Timing Stats: Avg=%v, Min=%v, Max=%v\n", avg, min, max)
}

func testSchemasTopicPerformance() {
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer([]string{kafkaGatewayAddr}, config)
	if err != nil {
		fmt.Printf("❌ Failed to create consumer: %v\n", err)
		return
	}
	defer consumer.Close()

	topic := "_schemas"
	partition := int32(0)

	// Get topic metadata
	partitions, err := consumer.Partitions(topic)
	if err != nil {
		fmt.Printf("❌ Failed to get partitions for %s: %v\n", topic, err)
		return
	}

	fmt.Printf("   Topic %s has %d partitions\n", topic, len(partitions))

	// Create a client to get offset information
	config2 := sarama.NewConfig()
	config2.Version = sarama.V2_6_0_0
	client, err := sarama.NewClient([]string{kafkaGatewayAddr}, config2)
	if err != nil {
		fmt.Printf("❌ Failed to create client: %v\n", err)
		return
	}
	defer client.Close()

	// Get offset information
	oldest, err := client.GetOffset(topic, partition, sarama.OffsetOldest)
	if err != nil {
		fmt.Printf("❌ Failed to get oldest offset: %v\n", err)
		return
	}

	newest, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		fmt.Printf("❌ Failed to get newest offset: %v\n", err)
		return
	}

	fmt.Printf("   📊 %s Topic Stats: Oldest=%d, Newest=%d, Messages=%d\n",
		topic, oldest, newest, newest-oldest)

	// Test consuming recent messages
	if newest > oldest {
		startOffset := oldest
		if newest-oldest > 10 {
			startOffset = newest - 10 // Last 10 messages
		}

		fmt.Printf("   Testing consume from offset %d...\n", startOffset)

		partitionConsumer, err := consumer.ConsumePartition(topic, partition, startOffset)
		if err != nil {
			fmt.Printf("❌ Failed to create partition consumer: %v\n", err)
			return
		}
		defer partitionConsumer.Close()

		timeout := time.After(5 * time.Second)
		messageCount := 0

	consumeLoop:
		for {
			select {
			case message := <-partitionConsumer.Messages():
				messageCount++
				fmt.Printf("   📨 Message %d: offset=%d, key=%s, value_len=%d\n",
					messageCount, message.Offset, string(message.Key), len(message.Value))

				if messageCount >= 5 { // Limit to 5 messages
					break consumeLoop
				}

			case err := <-partitionConsumer.Errors():
				fmt.Printf("   ❌ Consumer error: %v\n", err)
				break consumeLoop

			case <-timeout:
				fmt.Printf("   ⏰ Consumer timeout after 5 seconds\n")
				break consumeLoop
			}
		}

		fmt.Printf("   📊 Consumed %d messages successfully\n", messageCount)
	}
}

func registerSchemaDebug(subject, schema string) (int, error) {
	payload := map[string]interface{}{
		"schema":     schema,
		"schemaType": "AVRO",
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}

	client := &http.Client{Timeout: testTimeoutDuration}
	url := fmt.Sprintf("%s/subjects/%s/versions", schemaRegistryAddr, subject)

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

func calculateStats(results *TestResults) {
	if len(results.Results) == 0 {
		return
	}

	var totalDuration time.Duration
	results.MinLatency = results.Results[0].Duration
	results.MaxLatency = results.Results[0].Duration

	for _, result := range results.Results {
		if result.Success {
			totalDuration += result.Duration
			if result.Duration < results.MinLatency {
				results.MinLatency = result.Duration
			}
			if result.Duration > results.MaxLatency {
				results.MaxLatency = result.Duration
			}
		}
	}

	if results.SuccessfulRegs > 0 {
		results.AverageLatency = totalDuration / time.Duration(results.SuccessfulRegs)
	}
}

func printTestResults(testType string, results TestResults) {
	fmt.Printf("   📊 %s Results:\n", testType)
	fmt.Printf("      Total Schemas: %d\n", results.TotalSchemas)
	fmt.Printf("      Successful: %d\n", results.SuccessfulRegs)
	fmt.Printf("      Failed: %d\n", results.FailedRegs)
	fmt.Printf("      Success Rate: %.1f%%\n", float64(results.SuccessfulRegs)/float64(results.TotalSchemas)*100)

	if results.SuccessfulRegs > 0 {
		fmt.Printf("      Avg Latency: %v\n", results.AverageLatency)
		fmt.Printf("      Min Latency: %v\n", results.MinLatency)
		fmt.Printf("      Max Latency: %v\n", results.MaxLatency)
	}

	// Show failed registrations
	for _, result := range results.Results {
		if !result.Success {
			fmt.Printf("      ❌ %s: %v\n", result.Subject, result.Error)
		}
	}
}
