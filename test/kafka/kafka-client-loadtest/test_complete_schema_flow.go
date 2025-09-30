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
	fmt.Println("🧪 Testing Complete Schema-Aware Flow")

	// Step 1: Register schema with Schema Registry
	fmt.Println("1️⃣  Registering schema with Schema Registry...")

	schemaJSON := `{
		"type": "record",
		"name": "LoadTestMessage",
		"namespace": "com.seaweedfs.loadtest",
		"fields": [
			{"name": "id", "type": "string"},
			{"name": "timestamp", "type": "long"},
			{"name": "producer_id", "type": "int"},
			{"name": "counter", "type": "long"},
			{"name": "user_id", "type": "string"},
			{"name": "event_type", "type": "string"},
			{"name": "properties", "type": {"type": "map", "values": "string"}}
		]
	}`

	schemaReq := map[string]interface{}{
		"schema": schemaJSON,
	}

	requestBody, _ := json.Marshal(schemaReq)

	// Register schema for topic value
	topicName := "schema-flow-test-topic"
	subject := topicName + "-value"
	url := fmt.Sprintf("http://schema-registry:8081/subjects/%s/versions", subject)

	resp, err := http.Post(url, "application/vnd.schemaregistry.v1+json", bytes.NewBuffer(requestBody))
	if err != nil {
		log.Fatalf("Failed to register schema: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		log.Fatalf("Schema registration failed: status=%d, body=%s", resp.StatusCode, string(body))
	}

	var schemaResp struct {
		ID int `json:"id"`
	}
	json.NewDecoder(resp.Body).Decode(&schemaResp)
	fmt.Printf("✅ Schema registered with ID: %d\n", schemaResp.ID)

	// Step 2: Create Kafka admin client and create topic
	fmt.Println("2️⃣  Creating topic...")

	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0

	admin, err := sarama.NewClusterAdmin([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Fatalf("Failed to create admin client: %v", err)
	}
	defer admin.Close()

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	err = admin.CreateTopic(topicName, topicDetail, false)
	if err != nil {
		fmt.Printf("Topic creation failed (may already exist): %v\n", err)
	} else {
		fmt.Printf("✅ Topic '%s' created\n", topicName)
	}

	// Step 3: Wait a moment for topic creation to propagate
	time.Sleep(5 * time.Second)

	// Step 4: Check if topic has schema configuration
	fmt.Println("3️⃣  Checking topic schema configuration...")

	configURL := fmt.Sprintf("http://kafka-gateway:8888/topics/kafka/%s/topic.conf", topicName)
	resp, err = http.Get(configURL)
	if err != nil {
		fmt.Printf("⚠️  Failed to get topic config: %v\n", err)
	} else {
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("📋 Topic config: %s\n", string(body))
	}

	fmt.Println("✅ Complete schema-aware flow test completed!")
}


