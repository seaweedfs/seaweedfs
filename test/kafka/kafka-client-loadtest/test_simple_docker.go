package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

// Simple test that runs inside Docker to test basic Kafka functionality

func main() {
	log.Println("=== Simple Docker Test ===")

	topicName := "test-simple-topic"

	// Step 1: Create topic
	log.Println("Step 1: Creating topic...")
	err := createTopic(topicName)
	if err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}
	log.Printf("✓ Topic '%s' created", topicName)

	// Step 2: Produce a simple message
	log.Println("Step 2: Producing message...")
	err = produceMessage(topicName, "Hello from Docker!")
	if err != nil {
		log.Fatalf("Failed to produce message: %v", err)
	}
	log.Printf("✓ Message produced")

	// Step 3: Consume the message
	log.Println("Step 3: Consuming message...")
	err = consumeMessage(topicName)
	if err != nil {
		log.Fatalf("Failed to consume message: %v", err)
	}
	log.Printf("✓ Message consumed")

	log.Println("✅ Simple test PASSED")
}

func createTopic(topicName string) error {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0

	admin, err := sarama.NewClusterAdmin([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		return fmt.Errorf("failed to create admin client: %w", err)
	}
	defer admin.Close()

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     4,
		ReplicationFactor: 1,
	}

	err = admin.CreateTopic(topicName, topicDetail, false)
	if err != nil && err != sarama.ErrTopicAlreadyExists {
		return fmt.Errorf("failed to create topic: %w", err)
	}

	return nil
}

func produceMessage(topicName, message string) error {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	producer, err := sarama.NewSyncProducer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topicName,
		Key:   sarama.StringEncoder("test-key"),
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	log.Printf("  Message sent to partition %d at offset %d", partition, offset)
	return nil
}

func consumeMessage(topicName string) error {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topicName, 0, sarama.OffsetOldest)
	if err != nil {
		return fmt.Errorf("failed to create partition consumer: %w", err)
	}
	defer partitionConsumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for {
		select {
		case message := <-partitionConsumer.Messages():
			log.Printf("  Received: partition=%d offset=%d value=%s",
				message.Partition, message.Offset, string(message.Value))
			return nil

		case err := <-partitionConsumer.Errors():
			return fmt.Errorf("consumer error: %w", err)

		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for message")
		}
	}
}

