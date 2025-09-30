package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("⚡ Testing Schema Registry Fail-Fast Behavior")
	fmt.Println("Testing that Kafka Gateway fails fast when Schema Registry is configured but unavailable")

	// Generate unique topic names to avoid conflicts
	timestamp := time.Now().Unix()
	regularTopic := fmt.Sprintf("test-fail-fast-regular-%d", timestamp)
	systemTopic := fmt.Sprintf("__test-fail-fast-system-%d", timestamp)

	// Test 1: Try to create a regular topic - should fail fast due to Schema Registry unavailable
	fmt.Printf("\n1️⃣  Test 1: Create regular topic '%s' (should fail fast)\n", regularTopic)
	if err := testCreateTopic(regularTopic); err != nil {
		fmt.Printf("✅ Regular topic creation failed as expected: %v\n", err)
		
		// Check if it's a fail-fast error (Schema Registry unavailable)
		if isSchemaRegistryUnavailableError(err) {
			fmt.Println("✅ Error indicates Schema Registry is unavailable (fail-fast working)")
		} else {
			fmt.Println("⚠️  Error doesn't indicate Schema Registry unavailability")
		}
	} else {
		fmt.Println("❌ Regular topic creation should have failed")
	}

	// Test 2: Try to create a system topic - should succeed
	fmt.Printf("\n2️⃣  Test 2: Create system topic '%s' (should succeed)\n", systemTopic)
	if err := testCreateTopic(systemTopic); err != nil {
		fmt.Printf("❌ System topic creation failed: %v\n", err)
	} else {
		fmt.Println("✅ System topic creation succeeded")
	}

	// Test 3: Try to produce to a non-existent regular topic (auto-create)
	autoCreateTopic := fmt.Sprintf("test-fail-fast-auto-%d", timestamp+rand.Int63n(1000))
	fmt.Printf("\n3️⃣  Test 3: Auto-create regular topic '%s' via produce (should fail)\n", autoCreateTopic)
	if err := testProduceTopic(autoCreateTopic); err != nil {
		fmt.Printf("✅ Auto-creation via produce failed as expected: %v\n", err)
	} else {
		fmt.Println("❌ Auto-creation via produce should have failed")
	}

	fmt.Println("\n📋 Fail-Fast Test Summary:")
	fmt.Println("- Schema Registry unavailable detection")
	fmt.Println("- Regular topic creation behavior")
	fmt.Println("- System topic creation behavior")
	fmt.Println("- Auto-creation behavior")
}

func testCreateTopic(topicName string) error {
	fmt.Printf("   📋 Creating admin client for topic: %s\n", topicName)
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0

	admin, err := sarama.NewClusterAdmin([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		return fmt.Errorf("failed to create admin client: %v", err)
	}
	defer admin.Close()

	fmt.Printf("   📋 Sending CreateTopic request...\n")
	
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	err = admin.CreateTopic(topicName, topicDetail, false)
	fmt.Printf("   📋 CreateTopic result: %v\n", err)
	return err
}

func testProduceTopic(topicName string) error {
	fmt.Printf("   📋 Creating producer for topic: %s\n", topicName)
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Version = sarama.V2_8_0_0

	producer, err := sarama.NewSyncProducer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		return fmt.Errorf("failed to create producer: %v", err)
	}
	defer producer.Close()

	fmt.Printf("   📋 Sending produce message...\n")

	msg := &sarama.ProducerMessage{
		Topic: topicName,
		Key:   sarama.StringEncoder("test-key"),
		Value: sarama.StringEncoder("test-value"),
	}

	partition, offset, err := producer.SendMessage(msg)
	fmt.Printf("   📋 SendMessage result: partition=%d, offset=%d, err=%v\n", partition, offset, err)
	return err
}

func isSchemaRegistryUnavailableError(err error) bool {
	if err == nil {
		return false
	}
	
	errStr := err.Error()
	
	// Look for indicators that Schema Registry is unavailable
	indicators := []string{
		"Schema Registry is unavailable",
		"connection refused",
		"no such host",
		"timeout",
		"network is unreachable",
		"failed to fetch",
	}
	
	for _, indicator := range indicators {
		if contains(errStr, indicator) {
			return true
		}
	}
	
	return false
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || 
		(len(s) > len(substr) && 
		 (s[:len(substr)] == substr || 
		  s[len(s)-len(substr):] == substr || 
		  indexOf(s, substr) >= 0)))
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

