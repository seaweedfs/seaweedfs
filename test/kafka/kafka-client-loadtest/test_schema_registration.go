package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("🧪 Testing Schema Registration and Topic Creation")
	
	// Step 1: Register a schema with Schema Registry
	fmt.Println("1️⃣  Registering schema with Schema Registry...")
	
	schema := map[string]interface{}{
		"type": "record",
		"name": "TestEvent",
		"fields": []map[string]interface{}{
			{"name": "id", "type": "string"},
			{"name": "timestamp", "type": "long"},
			{"name": "message", "type": "string"},
		},
	}
	
	schemaJSON, _ := json.Marshal(schema)
	
	registerRequest := map[string]interface{}{
		"schema": string(schemaJSON),
	}
	
	requestBody, _ := json.Marshal(registerRequest)
	
	// Register schema for value
	resp, err := http.Post("http://schema-registry:8081/subjects/test-registered-topic-value/versions", 
		"application/vnd.schemaregistry.v1+json", bytes.NewBuffer(requestBody))
	if err != nil {
		fmt.Printf("❌ Failed to register schema: %v\n", err)
		return
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("❌ Schema registration failed with status %d: %s\n", resp.StatusCode, string(body))
		return
	}
	
	var registerResponse map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&registerResponse)
	schemaID := int(registerResponse["id"].(float64))
	
	fmt.Printf("✅ Schema registered with ID: %d\n", schemaID)
	
	// Step 2: Create Kafka admin client
	fmt.Println("2️⃣  Creating Kafka admin client...")
	
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	
	admin, err := sarama.NewClusterAdmin([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		fmt.Printf("❌ Failed to create admin client: %v\n", err)
		return
	}
	defer admin.Close()
	
	fmt.Println("✅ Admin client created!")
	
	// Step 3: Create topic
	fmt.Println("3️⃣  Creating topic...")
	
	topicName := "test-registered-topic"
	
	err = admin.CreateTopic(topicName, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
	
	if err != nil {
		fmt.Printf("❌ Failed to create topic: %v\n", err)
		return
	}
	
	fmt.Printf("✅ Topic '%s' created!\n", topicName)
	
	// Step 4: Wait a bit for topic to be fully created
	fmt.Println("4️⃣  Waiting for topic initialization...")
	time.Sleep(2 * time.Second)
	
	// Step 5: Check topic configuration
	fmt.Println("5️⃣  Checking topic configuration...")
	
	resp, err = http.Get(fmt.Sprintf("http://localhost:8888/topics/kafka/%s/topic.conf", topicName))
	if err != nil {
		fmt.Printf("❌ Failed to check topic configuration: %v\n", err)
		return
	}
	defer resp.Body.Close()
	
	if resp.StatusCode == 200 {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("📋 Topic configuration:\n%s\n", string(body))
		
		var config map[string]interface{}
		if err := json.Unmarshal(body, &config); err == nil {
			if recordType, exists := config["messageRecordType"]; exists && recordType != nil {
				fmt.Printf("✅ Topic has schema configuration: %v\n", recordType)
			} else {
				fmt.Printf("⚠️  Topic does not have schema configuration (messageRecordType is null)\n")
			}
		}
	} else {
		fmt.Printf("❌ Failed to get topic configuration: HTTP %d\n", resp.StatusCode)
	}
	
	// Step 6: Produce a schematized message
	fmt.Println("6️⃣  Producing schematized message...")
	
	producer, err := sarama.NewSyncProducer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		fmt.Printf("❌ Failed to create producer: %v\n", err)
		return
	}
	defer producer.Close()
	
	// Create Confluent Wire Format message
	messageData := map[string]interface{}{
		"id":        "test-001",
		"timestamp": time.Now().UnixMilli(),
		"message":   "Hello from schema-registered topic!",
	}
	
	messageJSON, _ := json.Marshal(messageData)
	
	// Confluent Wire Format: [magic_byte][schema_id][json_data]
	wireFormatMessage := make([]byte, 0, 5+len(messageJSON))
	wireFormatMessage = append(wireFormatMessage, 0x00) // Magic byte
	
	// Schema ID as 4-byte big-endian
	wireFormatMessage = append(wireFormatMessage, 
		byte(schemaID>>24), byte(schemaID>>16), byte(schemaID>>8), byte(schemaID))
	
	wireFormatMessage = append(wireFormatMessage, messageJSON...)
	
	msg := &sarama.ProducerMessage{
		Topic: topicName,
		Key:   sarama.StringEncoder("test-key"),
		Value: sarama.ByteEncoder(wireFormatMessage),
	}
	
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		fmt.Printf("❌ Failed to produce message: %v\n", err)
		return
	}
	
	fmt.Printf("✅ Message produced to partition %d, offset %d\n", partition, offset)
	
	fmt.Println("🎉 Schema registration and topic creation test completed!")
	
	fmt.Println("\n✅ Summary:")
	fmt.Printf("   - Schema registered with ID %d\n", schemaID)
	fmt.Printf("   - Topic '%s' created\n", topicName)
	fmt.Println("   - Schematized message produced successfully")
	fmt.Println("   - Check topic configuration above to see if schema was applied")
}


