package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("Testing simple consumer group...")

	// Create topic first
	adminConfig := sarama.NewConfig()
	adminConfig.Version = sarama.V2_8_0_0
	adminConfig.Net.DialTimeout = 10 * time.Second
	adminConfig.Net.ReadTimeout = 10 * time.Second
	adminConfig.Net.WriteTimeout = 10 * time.Second

	adminClient, err := sarama.NewClient([]string{"kafka-gateway:9093"}, adminConfig)
	if err != nil {
		log.Fatalf("Failed to create admin client: %v", err)
	}
	defer adminClient.Close()

	admin, err := sarama.NewClusterAdminFromClient(adminClient)
	if err != nil {
		log.Fatalf("Failed to create cluster admin: %v", err)
	}
	defer admin.Close()

	topicName := fmt.Sprintf("test-simple-group-%d", time.Now().Unix())
	fmt.Printf("Creating topic: %s\n", topicName)

	err = admin.CreateTopic(topicName, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
	if err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}
	defer admin.DeleteTopic(topicName)

	fmt.Println("Topic created successfully")

	// Create consumer group
	consumerConfig := sarama.NewConfig()
	consumerConfig.Version = sarama.V2_8_0_0
	consumerConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	consumerConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumerConfig.Net.DialTimeout = 10 * time.Second
	consumerConfig.Net.ReadTimeout = 10 * time.Second
	consumerConfig.Net.WriteTimeout = 10 * time.Second
	consumerConfig.Consumer.Group.Session.Timeout = 10 * time.Second
	consumerConfig.Consumer.Group.Heartbeat.Interval = 3 * time.Second

	groupID := "test-simple-group"
	fmt.Printf("Creating consumer group: %s\n", groupID)

	consumer, err := sarama.NewConsumerGroup([]string{"kafka-gateway:9093"}, groupID, consumerConfig)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}
	defer consumer.Close()

	fmt.Println("Consumer group created successfully")

	// Start consuming
	fmt.Println("Starting consumer...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	handler := &testHandler{}

	err = consumer.Consume(ctx, []string{topicName}, handler)
	if err != nil {
		log.Fatalf("Consumer error: %v", err)
	}

	fmt.Println("Consumer completed")
}

type testHandler struct{}

func (h *testHandler) Setup(sarama.ConsumerGroupSession) error {
	fmt.Println("Consumer group session setup")
	return nil
}

func (h *testHandler) Cleanup(sarama.ConsumerGroupSession) error {
	fmt.Println("Consumer group session cleanup")
	return nil
}

func (h *testHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	fmt.Println("Consuming messages...")

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
