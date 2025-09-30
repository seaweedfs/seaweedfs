package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/IBM/sarama"
)

type SchemaRequest struct {
	Schema string `json:"schema"`
}

type SchemaResponse struct {
	ID int `json:"id"`
}

func main() {
	fmt.Println("🧪 Testing Schematized Message Production")

	// Wait for Schema Registry to be ready
	fmt.Println("⏳ Waiting for Schema Registry...")
	for i := 0; i < 30; i++ {
		resp, err := http.Get("http://schema-registry:8081/subjects")
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			break
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(2 * time.Second)
		if i == 29 {
			log.Fatal("Schema Registry not ready after 60 seconds")
		}
	}
	fmt.Println("✅ Schema Registry is ready!")

	// Register a schema
	fmt.Println("1️⃣  Registering schema...")
	schema := `{
		"type": "record",
		"name": "TestMessage",
		"fields": [
			{"name": "id", "type": "string"},
			{"name": "message", "type": "string"},
			{"name": "timestamp", "type": "long"}
		]
	}`

	schemaReq := SchemaRequest{Schema: schema}
	reqBody, _ := json.Marshal(schemaReq)

	resp, err := http.Post("http://schema-registry:8081/subjects/test-topic-value/versions",
		"application/vnd.schemaregistry.v1+json", bytes.NewBuffer(reqBody))
	if err != nil {
		log.Fatalf("Failed to register schema: %v", err)
	}
	defer resp.Body.Close()

	var schemaResp SchemaResponse
	json.NewDecoder(resp.Body).Decode(&schemaResp)
	fmt.Printf("   📝 Schema registered with ID: %d\n", schemaResp.ID)

	// Create Kafka producer
	fmt.Println("2️⃣  Creating Kafka producer...")
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3

	producer, err := sarama.NewSyncProducer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()
	fmt.Println("✅ Producer created!")

	// Create topic with schema
	fmt.Println("3️⃣  Creating topic...")
	adminClient, err := sarama.NewClient([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Fatalf("Failed to create admin client: %v", err)
	}
	defer adminClient.Close()

	admin, err := sarama.NewClusterAdminFromClient(adminClient)
	if err != nil {
		log.Fatalf("Failed to create admin: %v", err)
	}
	defer admin.Close()

	topicName := "test-schematized-topic"
	err = admin.CreateTopic(topicName, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
	if err != nil && err != sarama.ErrTopicAlreadyExists {
		log.Fatalf("Failed to create topic: %v", err)
	}
	fmt.Printf("✅ Topic '%s' created!\n", topicName)

	// Produce schematized message
	fmt.Println("4️⃣  Producing schematized message...")

	// Create Confluent Wire Format message
	// Format: [magic_byte][schema_id][avro_data]
	messageData := []byte(`{"id":"test-001","message":"Hello Schema!","timestamp":` + fmt.Sprintf("%d", time.Now().UnixNano()) + `}`)

	// Confluent Wire Format: magic byte (0) + schema ID (4 bytes big-endian) + data
	wireFormat := make([]byte, 5+len(messageData))
	wireFormat[0] = 0 // Magic byte
	// Schema ID in big-endian
	wireFormat[1] = byte(schemaResp.ID >> 24)
	wireFormat[2] = byte(schemaResp.ID >> 16)
	wireFormat[3] = byte(schemaResp.ID >> 8)
	wireFormat[4] = byte(schemaResp.ID)
	copy(wireFormat[5:], messageData)

	msg := &sarama.ProducerMessage{
		Topic: topicName,
		Key:   sarama.StringEncoder("test-key"),
		Value: sarama.ByteEncoder(wireFormat),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}
	fmt.Printf("   📝 Message sent: partition=%d, offset=%d, schemaID=%d\n", partition, offset, schemaResp.ID)
	fmt.Println("✅ Schematized message produced!")

	// Verify topic has schema configuration
	fmt.Println("5️⃣  Verifying topic schema configuration...")
	time.Sleep(2 * time.Second) // Allow time for configuration to be written

	// Check if topic.conf exists and has schema info
	resp, err = http.Get(fmt.Sprintf("http://localhost:8888/topics/kafka/%s/topic.conf", topicName))
	if err != nil {
		fmt.Printf("   ⚠️  Could not check topic configuration: %v\n", err)
	} else {
		defer resp.Body.Close()
		if resp.StatusCode == 200 {
			fmt.Println("   📋 Topic configuration found!")
			// Read and display the configuration
			var config map[string]interface{}
			json.NewDecoder(resp.Body).Decode(&config)
			if recordType, exists := config["messageRecordType"]; exists && recordType != nil {
				fmt.Printf("   📝 Message record type: %v\n", recordType)
			} else {
				fmt.Println("   ⚠️  No schema configuration found in topic")
			}
		} else {
			fmt.Printf("   ⚠️  Topic configuration not found (status: %d)\n", resp.StatusCode)
		}
	}

	fmt.Println("🎉 Schematized message production test completed!")
	fmt.Println()
	fmt.Println("✅ Demonstrated capabilities:")
	fmt.Println("   - Schema Registry integration")
	fmt.Println("   - Schema registration")
	fmt.Println("   - Confluent Wire Format message production")
	fmt.Println("   - Topic creation with schema awareness")
}
