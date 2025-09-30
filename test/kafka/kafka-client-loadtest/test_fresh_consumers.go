package main

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("Testing fresh consumer scenario...")

	// Create producer
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

	// Create admin client
	adminClient, err := sarama.NewClusterAdmin([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Fatalf("Failed to create admin client: %v", err)
	}
	defer adminClient.Close()

	// Create a unique topic name
	topicName := fmt.Sprintf("test-fresh-consumers-%d", time.Now().Unix())
	fmt.Printf("Creating topic: %s\n", topicName)

	err = adminClient.CreateTopic(topicName, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
	if err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}
	fmt.Println("Topic created successfully")

	// Produce 3 messages
	for i := 0; i < 3; i++ {
		message := fmt.Sprintf("fresh-message-%d", i)
		_, _, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: topicName,
			Key:   sarama.StringEncoder("test-key"),
			Value: sarama.StringEncoder(message),
		})
		if err != nil {
			log.Fatalf("Failed to send message: %v", err)
		}
		fmt.Printf("Produced message %d: %s\n", i, message)
	}

	// Wait for messages to be written
	time.Sleep(2 * time.Second)

	// Create consumer
	consumer, err := sarama.NewConsumer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Create partition consumer
	partitionConsumer, err := consumer.ConsumePartition(topicName, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Failed to create partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	// Consume messages
	fmt.Println("Consuming messages...")
	messageCount := 0
	timeout := time.After(10 * time.Second)

	for {
		select {
		case message := <-partitionConsumer.Messages():
			if message != nil {
				messageCount++
				fmt.Printf("Received message %d: %s (offset: %d)\n", messageCount, string(message.Value), message.Offset)
			}
		case <-timeout:
			fmt.Printf("Timeout reached. Total messages received: %d\n", messageCount)
			return
		}
	}
}


