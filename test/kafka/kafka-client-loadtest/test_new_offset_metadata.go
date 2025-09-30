package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("🧪 Testing new offset metadata with fresh topic")

	// Kafka configuration
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Consumer.Return.Errors = true
	config.Version = sarama.V2_6_0_0

	brokers := []string{"loadtest-kafka-gateway-no-schema:9093"}

	// Create admin client to create topic
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create admin client: %v", err)
	}
	defer admin.Close()

	// Create a fresh topic
	topicName := "test-new-offset-metadata"
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	err = admin.CreateTopic(topicName, topicDetail, false)
	if err != nil {
		fmt.Printf("⚠️  Failed to create topic (might already exist): %v\n", err)
	} else {
		fmt.Printf("✅ Created fresh topic: %s\n", topicName)
	}

	// Wait a moment for topic creation
	time.Sleep(2 * time.Second)

	// Create producer
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Produce a message
	message := &sarama.ProducerMessage{
		Topic: topicName,
		Key:   sarama.StringEncoder("test-key-new"),
		Value: sarama.StringEncoder("test-value-new"),
	}

	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}

	fmt.Printf("✅ Successfully sent message: partition=%d, offset=%d\n", partition, offset)
	fmt.Printf("   Key: %s\n", "test-key-new")
	fmt.Printf("   Value: %s\n", "test-value-new")

	// Wait a moment for message to be processed
	time.Sleep(3 * time.Second)

	// Create consumer
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Consume the message
	partitionConsumer, err := consumer.ConsumePartition(topicName, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Failed to create partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	// Set timeout for message consumption
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	select {
	case msg := <-partitionConsumer.Messages():
		fmt.Printf("✅ Successfully received message: partition=%d, offset=%d\n", msg.Partition, msg.Offset)
		fmt.Printf("   Key: %s\n", string(msg.Key))
		fmt.Printf("   Value: %s\n", string(msg.Value))
		fmt.Println("✅ New offset metadata test PASSED!")
	case err := <-partitionConsumer.Errors():
		log.Fatalf("❌ Consumer error: %v", err)
	case <-ctx.Done():
		log.Fatal("❌ Timeout waiting for message")
	}
}
