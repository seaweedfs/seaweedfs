package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	// Create producer
	producerConfig := sarama.NewConfig()
	producerConfig.Producer.Return.Successes = true
	producerConfig.Producer.RequiredAcks = sarama.WaitForAll
	producerConfig.Producer.Retry.Max = 3

	producer, err := sarama.NewSyncProducer([]string{"kafka-gateway:9093"}, producerConfig)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Create topic
	adminClient, err := sarama.NewClusterAdmin([]string{"kafka-gateway:9093"}, producerConfig)
	if err != nil {
		log.Fatalf("Failed to create admin client: %v", err)
	}
	defer adminClient.Close()

	topicName := "test-two-consumers-simple"
	err = adminClient.CreateTopic(topicName, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
	if err != nil {
		log.Printf("Topic creation failed (may already exist): %v", err)
	}

	// Produce messages
	for i := 0; i < 5; i++ {
		message := fmt.Sprintf("test-message-%d", i)
		_, _, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: topicName,
			Key:   sarama.StringEncoder("test-key"),
			Value: sarama.StringEncoder(message),
		})
		if err != nil {
			log.Fatalf("Failed to send message: %v", err)
		}
		fmt.Printf("Produced message %d\n", i)
	}

	// Wait a bit for messages to be written
	time.Sleep(2 * time.Second)

	// Create individual consumers (not consumer groups)
	consumer1Config := sarama.NewConfig()
	consumer1Config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer1, err := sarama.NewConsumer([]string{"kafka-gateway:9093"}, consumer1Config)
	if err != nil {
		log.Fatalf("Failed to create consumer 1: %v", err)
	}
	defer consumer1.Close()

	consumer2, err := sarama.NewConsumer([]string{"kafka-gateway:9093"}, consumer1Config)
	if err != nil {
		log.Fatalf("Failed to create consumer 2: %v", err)
	}
	defer consumer2.Close()

	// Create partition consumers
	partitionConsumer1, err := consumer1.ConsumePartition(topicName, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Failed to create partition consumer 1: %v", err)
	}
	defer partitionConsumer1.Close()

	partitionConsumer2, err := consumer2.ConsumePartition(topicName, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Failed to create partition consumer 2: %v", err)
	}
	defer partitionConsumer2.Close()

	// Consumer counters
	consumer1Count := 0
	consumer2Count := 0

	// Start consumers in goroutines
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case message := <-partitionConsumer1.Messages():
				if message != nil {
					consumer1Count++
					fmt.Printf("Consumer 1 received message: %s\n", string(message.Value))
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case message := <-partitionConsumer2.Messages():
				if message != nil {
					consumer2Count++
					fmt.Printf("Consumer 2 received message: %s\n", string(message.Value))
				}
			}
		}
	}()

	// Wait for messages
	time.Sleep(10 * time.Second)
	fmt.Printf("Consumer 1 received %d messages\n", consumer1Count)
	fmt.Printf("Consumer 2 received %d messages\n", consumer2Count)
}


