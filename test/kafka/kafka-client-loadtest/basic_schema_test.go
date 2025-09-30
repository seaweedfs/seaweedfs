package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("🧪 Basic Schema Test (Without Schema Registry)")
	fmt.Println("Testing Kafka Gateway with manual schema handling...")
	
	// Step 1: Create topic
	fmt.Println("1️⃣  Creating Kafka topic...")
	topicName := "basic-schema-test"
	if err := createTopic(topicName); err != nil {
		log.Printf("⚠️  Topic creation warning: %v", err)
	} else {
		fmt.Println("✅ Topic created successfully!")
	}
	
	// Step 2: Produce a message with manual schema ID
	fmt.Println("2️⃣  Producing message with manual schema ID...")
	if err := produceSchematizedMessage(topicName, 1001); err != nil {
		log.Printf("❌ Message production failed: %v", err)
		return
	}
	fmt.Println("✅ Message produced successfully!")
	
	// Step 3: Wait and then try to consume
	fmt.Println("3️⃣  Waiting 5 seconds before consuming...")
	time.Sleep(5 * time.Second)
	
	fmt.Println("4️⃣  Attempting to consume message...")
	if err := consumeMessage(topicName); err != nil {
		log.Printf("❌ Message consumption failed: %v", err)
		return
	}
	fmt.Println("✅ Message consumed successfully!")
	
	fmt.Println("🎉 Basic schema test completed successfully!")
	fmt.Println("")
	fmt.Println("✅ Demonstrated capabilities:")
	fmt.Println("   - Kafka Gateway topic creation")
	fmt.Println("   - Confluent Wire Format message production")
	fmt.Println("   - Schema ID encoding/decoding")
	fmt.Println("   - Message consumption with proper deserialization")
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

func produceSchematizedMessage(topicName string, schemaID int) error {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Version = sarama.V2_8_0_0
	
	producer, err := sarama.NewSyncProducer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		return err
	}
	defer producer.Close()
	
	// Create test message
	testMessage := map[string]interface{}{
		"user_id": "test-user-123",
		"action":  "schema_test",
		"timestamp": time.Now().UnixNano(),
		"metadata": map[string]interface{}{
			"test_type": "basic_schema",
			"version":   "1.0",
		},
	}
	
	// Create schematized message using Confluent Wire Format
	schematizedMessage, err := createSchematizedMessage(schemaID, testMessage)
	if err != nil {
		return err
	}
	
	msg := &sarama.ProducerMessage{
		Topic: topicName,
		Key:   sarama.StringEncoder("test-key-001"),
		Value: sarama.ByteEncoder(schematizedMessage),
	}
	
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}
	
	fmt.Printf("   📝 Message sent: partition=%d, offset=%d, schemaID=%d\n", partition, offset, schemaID)
	return nil
}

func consumeMessage(topicName string) error {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Version = sarama.V2_8_0_0
	
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
	
	// Try to read one message with timeout
	timeout := time.After(10 * time.Second)
	
	select {
	case msg := <-partitionConsumer.Messages():
		// Decode schematized message
		decodedMsg, err := decodeSchematizedMessage(msg.Value)
		if err != nil {
			return fmt.Errorf("decode failed: %v", err)
		}
		
		fmt.Printf("   📖 Message received:\n")
		fmt.Printf("      - Key: %s\n", string(msg.Key))
		fmt.Printf("      - Schema ID: %d\n", decodedMsg.SchemaID)
		fmt.Printf("      - Payload: %s\n", string(decodedMsg.Payload))
		
		// Parse JSON payload
		var parsedPayload map[string]interface{}
		if err := json.Unmarshal(decodedMsg.Payload, &parsedPayload); err == nil {
			fmt.Printf("      - Parsed Data: user_id=%s, action=%s\n", 
				parsedPayload["user_id"], parsedPayload["action"])
		}
		
		return nil
		
	case err := <-partitionConsumer.Errors():
		return fmt.Errorf("consumer error: %v", err)
		
	case <-timeout:
		return fmt.Errorf("timeout waiting for message")
	}
}

func createSchematizedMessage(schemaID int, data map[string]interface{}) ([]byte, error) {
	// Create JSON payload
	jsonPayload, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	
	// Create Confluent Wire Format: magic byte (0x0) + schema ID (4 bytes) + payload
	message := make([]byte, 5+len(jsonPayload))
	
	// Magic byte
	message[0] = 0x0
	
	// Schema ID (4 bytes, big-endian)
	message[1] = byte(schemaID >> 24)
	message[2] = byte(schemaID >> 16)
	message[3] = byte(schemaID >> 8)
	message[4] = byte(schemaID)
	
	// Payload
	copy(message[5:], jsonPayload)
	
	return message, nil
}

type SchematizedMessage struct {
	SchemaID int
	Payload  []byte
}

func decodeSchematizedMessage(data []byte) (*SchematizedMessage, error) {
	if len(data) < 5 {
		return nil, fmt.Errorf("message too short for Confluent Wire Format")
	}
	
	// Check magic byte
	if data[0] != 0x0 {
		return nil, fmt.Errorf("invalid magic byte: expected 0x0, got 0x%x", data[0])
	}
	
	// Extract schema ID (4 bytes, big-endian)
	schemaID := int(data[1])<<24 | int(data[2])<<16 | int(data[3])<<8 | int(data[4])
	
	// Extract payload
	payload := data[5:]
	
	return &SchematizedMessage{
		SchemaID: schemaID,
		Payload:  payload,
	}, nil
}


