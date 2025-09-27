package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	brokers := []string{"kafka-gateway:9093"}

	// Create admin client to create topic first
	adminConfig := sarama.NewConfig()
	adminConfig.Version = sarama.V2_8_0_0

	admin, err := sarama.NewClusterAdmin(brokers, adminConfig)
	if err != nil {
		log.Fatalf("Failed to create admin: %v", err)
	}
	defer admin.Close()

	// Create topic
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	err = admin.CreateTopic("test-debug-topic", topicDetail, false)
	if err != nil {
		fmt.Printf("Topic creation failed (may already exist): %v\n", err)
	} else {
		fmt.Println("Topic created successfully")
	}

	time.Sleep(1 * time.Second) // Wait for topic to be available

	// Create producer
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Version = sarama.V2_8_0_0

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Create a simple JSON message
	msg := map[string]interface{}{
		"id":        "test-message-1",
		"timestamp": time.Now().UnixNano(),
		"data":      "Hello, World!",
		"test":      true,
	}

	jsonBytes, err := json.Marshal(msg)
	if err != nil {
		log.Fatalf("Failed to marshal JSON: %v", err)
	}

	fmt.Printf("Original JSON message: %s\n", string(jsonBytes))
	fmt.Printf("JSON bytes (hex): %x\n", jsonBytes)

	// Produce the message
	partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
		Topic: "test-debug-topic",
		Key:   sarama.StringEncoder("test-key"),
		Value: sarama.ByteEncoder(jsonBytes),
	})
	if err != nil {
		log.Fatalf("Failed to produce message: %v", err)
	}

	fmt.Printf("Message sent to partition %d at offset %d\n", partition, offset)

	// Wait a bit for the message to be stored
	time.Sleep(2 * time.Second)

	// Create consumer
	consumerConfig := sarama.NewConfig()
	consumerConfig.Consumer.Return.Errors = true
	consumerConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumerConfig.Version = sarama.V2_8_0_0

	consumer, err := sarama.NewConsumer(brokers, consumerConfig)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Consume from the topic
	partitionConsumer, err := consumer.ConsumePartition("test-debug-topic", 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Failed to create partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	// Wait for the message
	select {
	case msg := <-partitionConsumer.Messages():
		fmt.Printf("Received message:\n")
		fmt.Printf("  Key: %s\n", string(msg.Key))
		fmt.Printf("  Value (string): %s\n", string(msg.Value))
		fmt.Printf("  Value (hex): %x\n", msg.Value)
		fmt.Printf("  Offset: %d\n", msg.Offset)
		fmt.Printf("  Partition: %d\n", msg.Partition)

		// Try to parse as JSON
		var receivedMsg map[string]interface{}
		if err := json.Unmarshal(msg.Value, &receivedMsg); err != nil {
			fmt.Printf("ERROR: Failed to parse as JSON: %v\n", err)

			// Show first few bytes for analysis
			if len(msg.Value) > 0 {
				fmt.Printf("First 20 bytes: %x\n", msg.Value[:min(20, len(msg.Value))])
			}
		} else {
			fmt.Printf("Successfully parsed JSON: %+v\n", receivedMsg)
		}

	case err := <-partitionConsumer.Errors():
		log.Fatalf("Consumer error: %v", err)
	case <-time.After(10 * time.Second):
		log.Fatal("Timeout waiting for message")
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
