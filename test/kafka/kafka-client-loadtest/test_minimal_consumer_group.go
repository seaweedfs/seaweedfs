package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("Testing minimal consumer group...")

	// Very minimal configuration
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.ClientID = "test-minimal-group"

	// Minimal timeouts
	config.Net.DialTimeout = 5 * time.Second
	config.Net.ReadTimeout = 5 * time.Second
	config.Net.WriteTimeout = 5 * time.Second

	// Consumer group settings
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Group.Session.Timeout = 5 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 1 * time.Second

	// Force fresh metadata
	config.Metadata.RefreshFrequency = 1 * time.Second
	config.Metadata.Full = true

	// Create topic first
	fmt.Println("Creating topic...")
	adminClient, err := sarama.NewClient([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Fatalf("Failed to create admin client: %v", err)
	}
	defer adminClient.Close()

	admin, err := sarama.NewClusterAdminFromClient(adminClient)
	if err != nil {
		log.Fatalf("Failed to create cluster admin: %v", err)
	}
	defer admin.Close()

	topicName := fmt.Sprintf("test-minimal-group-%d", time.Now().Unix())
	err = admin.CreateTopic(topicName, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
	if err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}
	defer admin.DeleteTopic(topicName)

	fmt.Printf("Topic %s created\n", topicName)

	// Create consumer group
	fmt.Println("Creating consumer group...")
	consumer, err := sarama.NewConsumerGroup([]string{"kafka-gateway:9093"}, "test-minimal-group", config)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}
	defer consumer.Close()

	fmt.Println("Consumer group created successfully")

	// Start consuming with timeout
	fmt.Println("Starting consumer...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	handler := &minimalHandler{}

	err = consumer.Consume(ctx, []string{topicName}, handler)
	if err != nil {
		log.Fatalf("Consumer error: %v", err)
	}

	fmt.Println("Consumer completed")
}

type minimalHandler struct{}

func (h *minimalHandler) Setup(sarama.ConsumerGroupSession) error {
	fmt.Println("✅ Consumer group session setup")
	return nil
}

func (h *minimalHandler) Cleanup(sarama.ConsumerGroupSession) error {
	fmt.Println("✅ Consumer group session cleanup")
	return nil
}

func (h *minimalHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	fmt.Println("✅ Consuming messages...")

	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				fmt.Println("No more messages")
				return nil
			}
			fmt.Printf("Received message: %s\n", string(message.Value))
			session.MarkMessage(message, "")
		case <-session.Context().Done():
			fmt.Println("Session context done")
			return nil
		}
	}
}


