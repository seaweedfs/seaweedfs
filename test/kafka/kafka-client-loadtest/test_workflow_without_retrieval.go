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
	fmt.Println("🧪 Schema-Aware Workflow Test (Skip Schema Retrieval)")
	fmt.Println("Testing: Schema Registry → Topic Creation → Message Production → Consumption")

	// Step 1: Verify Schema Registry is ready
	fmt.Println("\n1️⃣  Step 1: Verify Schema Registry is ready")
	if err := waitForSchemaRegistry(); err != nil {
		log.Fatalf("❌ Schema Registry not ready: %v", err)
	}
	fmt.Println("✅ Schema Registry is ready")

	// Step 2: Register schema in Schema Registry
	fmt.Println("\n2️⃣  Step 2: Register schema in Schema Registry")
	topicName := "workflow-test-topic"
	schemaID, err := registerSchema(topicName)
	if err != nil {
		log.Fatalf("❌ Failed to register schema: %v", err)
	}
	fmt.Printf("✅ Schema registered with ID: %d for subject: %s-value\n", schemaID, topicName)

	// Step 3: Create topic (should now find schema and succeed)
	fmt.Println("\n3️⃣  Step 3: Create topic - Kafka Gateway should find schema in Registry")
	if err := createTopic(topicName); err != nil {
		log.Fatalf("❌ Failed to create topic: %v", err)
	}
	fmt.Printf("✅ Topic '%s' created successfully\n", topicName)

	// Step 4: Verify topic has schema in topic.conf
	fmt.Println("\n4️⃣  Step 4: Verify topic has schema persisted in topic.conf")
	time.Sleep(2 * time.Second) // Wait for topic.conf to be written
	if err := verifyTopicSchema(topicName); err != nil {
		log.Fatalf("❌ Failed to verify topic schema: %v", err)
	}
	fmt.Printf("✅ Topic '%s' has proper schema in topic.conf\n", topicName)

	// Step 5: Produce schematized message
	fmt.Println("\n5️⃣  Step 5: Produce schematized message with Confluent Wire Format")
	if err := produceSchematizedMessage(topicName, schemaID); err != nil {
		log.Fatalf("❌ Failed to produce message: %v", err)
	}
	fmt.Printf("✅ Schematized message produced to topic '%s'\n", topicName)

	// Step 6: Consume and verify message
	fmt.Println("\n6️⃣  Step 6: Consume and verify message")
	if err := consumeAndVerifyMessage(topicName); err != nil {
		log.Fatalf("❌ Failed to consume/verify message: %v", err)
	}
	fmt.Printf("✅ Message consumed and verified from topic '%s'\n", topicName)

	fmt.Println("\n🎉 WORKFLOW TEST PASSED! 🎉")
	fmt.Println("✅ Schema Registry integration working")
	fmt.Println("✅ Topic creation with schema lookup working")
	fmt.Println("✅ Schema persistence to topic.conf working")
	fmt.Println("✅ Schematized message production working")
	fmt.Println("✅ Message consumption working")
	fmt.Println("✅ Timestamp fix working")
}

func waitForSchemaRegistry() error {
	maxRetries := 30
	for i := 0; i < maxRetries; i++ {
		resp, err := http.Get("http://schema-registry:8081/subjects")
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			return nil
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("schema registry not ready after %d retries", maxRetries)
}

func registerSchema(topicName string) (int, error) {
	// Avro schema for test messages
	avroSchema := `{
		"type": "record",
		"name": "WorkflowTestMessage",
		"namespace": "com.seaweedfs.test",
		"fields": [
			{"name": "id", "type": "string"},
			{"name": "timestamp", "type": "long"},
			{"name": "message", "type": "string"},
			{"name": "test_step", "type": "string"}
		]
	}`

	schemaReq := map[string]interface{}{
		"schema": avroSchema,
	}

	jsonData, err := json.Marshal(schemaReq)
	if err != nil {
		return 0, err
	}

	// Register schema for topic value
	subject := topicName + "-value"
	url := fmt.Sprintf("http://schema-registry:8081/subjects/%s/versions", subject)

	resp, err := http.Post(url, "application/vnd.schemaregistry.v1+json", bytes.NewBuffer(jsonData))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("schema registration failed: status=%d, body=%s", resp.StatusCode, string(body))
	}

	var schemaResp struct {
		ID int `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&schemaResp); err != nil {
		return 0, err
	}

	return schemaResp.ID, nil
}

func createTopic(topicName string) error {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0

	admin, err := sarama.NewClusterAdmin([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		return err
	}
	defer admin.Close()

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	return admin.CreateTopic(topicName, topicDetail, false)
}

func verifyTopicSchema(topicName string) error {
	url := fmt.Sprintf("http://localhost:8888/topics/kafka/%s/topic.conf", topicName)
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("failed to get topic.conf: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("topic.conf not found: status=%d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read topic.conf: %v", err)
	}

	var topicConf struct {
		MessageRecordType interface{} `json:"messageRecordType"`
		KeyColumns        []string    `json:"keyColumns"`
	}

	if err := json.Unmarshal(body, &topicConf); err != nil {
		return fmt.Errorf("failed to parse topic.conf: %v", err)
	}

	if topicConf.MessageRecordType == nil {
		return fmt.Errorf("messageRecordType is null - schema not persisted")
	}

	fmt.Printf("   📋 messageRecordType: %v\n", topicConf.MessageRecordType)
	fmt.Printf("   📋 keyColumns: %v\n", topicConf.KeyColumns)

	return nil
}

func produceSchematizedMessage(topicName string, schemaID int) error {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Version = sarama.V2_8_0_0

	producer, err := sarama.NewSyncProducer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		return err
	}
	defer producer.Close()

	// Create Confluent Wire Format message
	// Format: [magic_byte][schema_id][avro_payload]
	magicByte := byte(0x0)
	schemaIDBytes := make([]byte, 4)
	schemaIDBytes[0] = byte(schemaID >> 24)
	schemaIDBytes[1] = byte(schemaID >> 16)
	schemaIDBytes[2] = byte(schemaID >> 8)
	schemaIDBytes[3] = byte(schemaID)

	// Simplified Avro payload (in real scenario, this would be properly Avro-encoded)
	payload := fmt.Sprintf(`{"id": "test-001", "timestamp": %d, "message": "workflow test message", "test_step": "production"}`, time.Now().UnixMilli())

	// Combine into Confluent Wire Format
	confluentMessage := make([]byte, 0, 1+4+len(payload))
	confluentMessage = append(confluentMessage, magicByte)
	confluentMessage = append(confluentMessage, schemaIDBytes...)
	confluentMessage = append(confluentMessage, []byte(payload)...)

	msg := &sarama.ProducerMessage{
		Topic: topicName,
		Key:   sarama.StringEncoder("workflow-test-key"),
		Value: sarama.ByteEncoder(confluentMessage),
	}

	_, _, err = producer.SendMessage(msg)
	return err
}

func consumeAndVerifyMessage(topicName string) error {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		return err
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topicName, 0, sarama.OffsetOldest)
	if err != nil {
		return err
	}
	defer partitionConsumer.Close()

	select {
	case message := <-partitionConsumer.Messages():
		fmt.Printf("   📋 Message consumed: offset=%d\n", message.Offset)
		fmt.Printf("   📋 Timestamp: %s\n", message.Timestamp.Format(time.RFC3339))
		fmt.Printf("   📋 Key: %q\n", string(message.Key))
		fmt.Printf("   📋 Value length: %d bytes\n", len(message.Value))

		// Verify timestamp is reasonable (within 1 hour)
		now := time.Now()
		timeDiff := now.Sub(message.Timestamp)
		if timeDiff < 0 {
			timeDiff = -timeDiff
		}

		if timeDiff > 1*time.Hour {
			return fmt.Errorf("timestamp appears incorrect: %s (diff: %v)", message.Timestamp.Format(time.RFC3339), timeDiff)
		}

		fmt.Printf("   ✅ Timestamp is correct (diff: %v)\n", timeDiff)

		// Verify Confluent Wire Format
		if len(message.Value) < 5 {
			return fmt.Errorf("message too short to be Confluent Wire Format")
		}

		if message.Value[0] != 0x0 {
			return fmt.Errorf("invalid magic byte: expected 0x0, got 0x%02x", message.Value[0])
		}

		fmt.Printf("   ✅ Confluent Wire Format verified\n")

		return nil

	case err := <-partitionConsumer.Errors():
		return fmt.Errorf("consumer error: %v", err)

	case <-time.After(10 * time.Second):
		return fmt.Errorf("timeout waiting for message")
	}
}

