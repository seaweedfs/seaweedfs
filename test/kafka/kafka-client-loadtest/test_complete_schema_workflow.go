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
	fmt.Println("🧪 Testing Complete Schema Management Workflow")
	fmt.Println("This test demonstrates the full schema-aware topic creation process")

	topicName := fmt.Sprintf("complete-schema-test-%d", time.Now().Unix())

	// Step 1: Register a schema with Schema Registry
	fmt.Println("\n1️⃣  Registering schema with Schema Registry...")

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

	// Register schema for value (using the topic name as subject)
	valueSubject := topicName + "-value"
	resp, err := http.Post(fmt.Sprintf("http://schema-registry:8081/subjects/%s/versions", valueSubject),
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

	fmt.Printf("✅ Schema registered with ID: %d for subject: %s\n", schemaID, valueSubject)

	// Step 2: Wait a moment for Schema Registry to process
	fmt.Println("\n2️⃣  Waiting for schema to be available...")
	time.Sleep(2 * time.Second)

	// Step 3: Create Kafka admin client
	fmt.Println("\n3️⃣  Creating Kafka admin client...")

	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0

	admin, err := sarama.NewClusterAdmin([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		fmt.Printf("❌ Failed to create admin client: %v\n", err)
		return
	}
	defer admin.Close()

	fmt.Println("✅ Admin client created!")

	// Step 4: Create topic (this should now trigger schema integration)
	fmt.Printf("\n4️⃣  Creating topic: %s\n", topicName)

	err = admin.CreateTopic(topicName, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)

	if err != nil {
		fmt.Printf("❌ Failed to create topic: %v\n", err)
		return
	}

	fmt.Printf("✅ Topic '%s' created!\n", topicName)

	// Step 5: Wait for topic to be fully created and schema integration to complete
	fmt.Println("\n5️⃣  Waiting for topic initialization and schema integration...")
	time.Sleep(5 * time.Second)

	// Step 6: Check topic configuration
	fmt.Println("\n6️⃣  Checking topic configuration...")

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
				fmt.Printf("✅ SUCCESS: Topic has schema configuration!\n")
				fmt.Printf("   Schema details: %v\n", recordType)
			} else {
				fmt.Printf("⚠️  Topic does not have schema configuration (messageRecordType is null)\n")
				fmt.Printf("   This means the schema lookup failed or the schema wasn't found\n")
			}
		}
	} else {
		fmt.Printf("❌ Failed to get topic configuration: HTTP %d\n", resp.StatusCode)
	}

	// Step 7: Produce a schematized message to verify everything works
	fmt.Println("\n7️⃣  Producing schematized message...")

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
		"message":   "Hello from schema-integrated topic!",
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

	fmt.Println("\n🎉 Complete Schema Management Workflow Test Completed!")

	fmt.Println("\n✅ Summary:")
	fmt.Printf("   - Schema registered with ID %d for subject %s\n", schemaID, valueSubject)
	fmt.Printf("   - Topic '%s' created\n", topicName)
	fmt.Println("   - Schematized message produced successfully")
	fmt.Printf("   - Topic configuration: http://localhost:8888/topics/kafka/%s/topic.conf\n", topicName)

	if resp.StatusCode == 200 {
		fmt.Println("\n🔍 Expected behavior:")
		fmt.Println("   - If schema management is working: messageRecordType should contain schema details")
		fmt.Println("   - If schema management is not working: messageRecordType will be null")
		fmt.Println("   - Check the Kafka Gateway logs for 'Creating topic with schema support' messages")
	}
}


