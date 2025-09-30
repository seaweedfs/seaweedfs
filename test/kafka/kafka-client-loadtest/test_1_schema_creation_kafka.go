package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

// Test 1: Schema Creation in topic.conf using Kafka Protocol
// This test verifies that when we register a schema and create a topic via Kafka,
// the topic.conf contains the correct schema information.

func main() {
	log.Println("=== Test 1: Schema Creation in topic.conf ===")

	// Step 1: Register a schema in Schema Registry
	log.Println("Step 1: Registering schema in Schema Registry...")
	schemaID, err := registerTestSchema()
	if err != nil {
		log.Fatalf("Failed to register schema: %v", err)
	}
	log.Printf("✓ Schema registered with ID: %d", schemaID)

	// Step 2: Create a topic via Kafka protocol
	log.Println("Step 2: Creating topic via Kafka protocol...")
	topicName := "test-schema-topic"
	err = createTopicViaKafkaProtocol(topicName)
	if err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}
	log.Printf("✓ Topic '%s' created", topicName)

	// Step 3: Wait a moment for topic configuration to be written
	time.Sleep(3 * time.Second)

	// Step 4: Check topic.conf for schema information
	log.Println("Step 3: Checking topic.conf for schema information...")
	topicConf, err := getTopicConf(topicName)
	if err != nil {
		log.Fatalf("Failed to get topic.conf: %v", err)
	}

	// Step 5: Verify schema information is present
	log.Println("Step 4: Verifying schema information...")
	if topicConf.MessageRecordType == nil {
		log.Fatalf("❌ FAIL: messageRecordType is null in topic.conf")
	}

	log.Printf("✓ messageRecordType found in topic.conf")
	log.Printf("  Schema type: %v", topicConf.MessageRecordType)

	// Pretty print the topic configuration
	confJSON, _ := json.MarshalIndent(topicConf, "", "  ")
	log.Printf("Complete topic.conf:\n%s", string(confJSON))

	log.Println("✅ Test 1 PASSED: Schema information correctly stored in topic.conf")
}

type TopicConf struct {
	BrokerPartitionAssignments []interface{} `json:"brokerPartitionAssignments"`
	Retention                  interface{}   `json:"retention"`
	MessageRecordType          interface{}   `json:"messageRecordType"`
	KeyColumns                 []string      `json:"keyColumns"`
}

func registerTestSchema() (int, error) {
	schema := `{
		"type": "record",
		"name": "TestMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "message", "type": "string"},
			{"name": "timestamp", "type": "long"}
		]
	}`

	payload := fmt.Sprintf(`{"schema": %q}`, schema)

	resp, err := http.Post(
		"http://localhost:8081/subjects/test-schema-topic-value/versions",
		"application/vnd.schemaregistry.v1+json",
		strings.NewReader(payload),
	)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("schema registration failed: %d - %s", resp.StatusCode, string(body))
	}

	var result struct {
		ID int `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, err
	}

	return result.ID, nil
}

func createTopicViaKafkaProtocol(topicName string) error {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0

	// Create admin client
	admin, err := sarama.NewClusterAdmin([]string{"localhost:9093"}, config)
	if err != nil {
		return fmt.Errorf("failed to create admin client: %w", err)
	}
	defer admin.Close()

	// Create topic
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     4, // Use default partitions
		ReplicationFactor: 1,
	}

	err = admin.CreateTopic(topicName, topicDetail, false)
	if err != nil && err != sarama.ErrTopicAlreadyExists {
		return fmt.Errorf("failed to create topic: %w", err)
	}

	return nil
}

func getTopicConf(topicName string) (*TopicConf, error) {
	resp, err := http.Get(fmt.Sprintf("http://localhost:8888/topics/kafka/%s/topic.conf", topicName))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to get topic.conf: %d - %s", resp.StatusCode, string(body))
	}

	var conf TopicConf
	if err := json.NewDecoder(resp.Body).Decode(&conf); err != nil {
		return nil, err
	}

	return &conf, nil
}

