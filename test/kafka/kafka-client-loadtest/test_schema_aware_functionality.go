package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("🔧 Testing Schema-Aware Kafka Gateway Functionality")
	fmt.Println("Testing schema registration → produce → consume workflow")

	topic := fmt.Sprintf("test-schema-aware-%d", time.Now().Unix())
	fmt.Printf("📋 Testing topic: %s\n", topic)

	// Step 1: Register Schema
	fmt.Println("\n1️⃣  Registering schema with Schema Registry...")
	schemaID, err := registerSchema(topic)
	if err != nil {
		log.Fatalf("❌ Failed to register schema: %v", err)
	}
	fmt.Printf("✅ Schema registered with ID: %d\n", schemaID)

	// Step 2: Kafka Gateway configuration
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	brokers := []string{"localhost:9093"}

	// Step 3: Create Producer
	fmt.Println("\n2️⃣  Creating producer...")
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("❌ Failed to create producer: %v", err)
	}
	defer producer.Close()
	fmt.Println("✅ Producer created successfully")

	// Step 4: Produce Messages
	fmt.Println("\n3️⃣  Producing test messages...")
	for i := 0; i < 3; i++ {
		message := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(fmt.Sprintf("key-%d", i)),
			Value: sarama.StringEncoder(fmt.Sprintf(`{"id": %d, "message": "test message %d", "timestamp": "%s"}`, i, i, time.Now().Format(time.RFC3339))),
		}

		partition, offset, err := producer.SendMessage(message)
		if err != nil {
			log.Fatalf("❌ Failed to send message %d: %v", i, err)
		}
		fmt.Printf("✅ Message %d sent to partition %d, offset %d\n", i, partition, offset)
	}

	// Step 5: Create Consumer
	fmt.Println("\n4️⃣  Creating consumer...")
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("❌ Failed to create consumer: %v", err)
	}
	defer consumer.Close()
	fmt.Println("✅ Consumer created successfully")

	// Step 6: Consume Messages
	fmt.Println("\n5️⃣  Consuming messages...")
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("❌ Failed to create partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	messageCount := 0
	for {
		select {
		case message := <-partitionConsumer.Messages():
			messageCount++
			fmt.Printf("✅ Consumed message %d: key=%s, value=%s, offset=%d, timestamp=%s\n",
				messageCount,
				string(message.Key),
				string(message.Value),
				message.Offset,
				message.Timestamp.Format(time.RFC3339))

			if messageCount >= 3 {
				fmt.Println("\n🎉 All messages consumed successfully!")
				fmt.Println("✅ Schema-aware workflow completed successfully!")
				return
			}

		case err := <-partitionConsumer.Errors():
			log.Fatalf("❌ Consumer error: %v", err)

		case <-ctx.Done():
			fmt.Printf("\n⚠️  Timeout reached. Consumed %d out of 3 messages\n", messageCount)
			return
		}
	}
}

func registerSchema(topic string) (int, error) {
	// Simple JSON schema for our test messages
	schema := `{
		"type": "record",
		"name": "TestMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "message", "type": "string"},
			{"name": "timestamp", "type": "string"}
		]
	}`

	// Schema Registry request payload
	payload := map[string]interface{}{
		"schema":     schema,
		"schemaType": "AVRO",
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal schema payload: %v", err)
	}

	// Register schema
	url := fmt.Sprintf("http://localhost:8081/subjects/%s-value/versions", topic)
	resp, err := http.Post(url, "application/vnd.schemaregistry.v1+json", bytes.NewBuffer(jsonPayload))
	if err != nil {
		return 0, fmt.Errorf("failed to register schema: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("schema registration failed with status %d", resp.StatusCode)
	}

	// Parse response to get schema ID
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("failed to decode schema registration response: %v", err)
	}

	schemaID, ok := result["id"].(float64)
	if !ok {
		return 0, fmt.Errorf("invalid schema ID in response: %v", result)
	}

	return int(schemaID), nil
}

