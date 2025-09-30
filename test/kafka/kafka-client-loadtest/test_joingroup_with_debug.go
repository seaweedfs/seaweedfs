package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("Testing JoinGroup request flow with debug logging...")

	// Enable Sarama debug logging
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	// Create producer first
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

	// Create topic
	topicName := fmt.Sprintf("test-joingroup-debug-%d", time.Now().Unix())
	fmt.Printf("Creating topic: %s\n", topicName)

	err = adminClient.CreateTopic(topicName, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
	if err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}

	// Produce a message
	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: topicName,
		Key:   sarama.StringEncoder("test-key"),
		Value: sarama.StringEncoder("test-message"),
	})
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}
	fmt.Println("Message sent successfully")

	// Wait for message to be written
	time.Sleep(2 * time.Second)

	// Test consumer group with debug logging
	consumerConfig := sarama.NewConfig()
	consumerConfig.Version = sarama.V2_8_0_0
	consumerConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	consumerConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumerConfig.Consumer.Group.Session.Timeout = 10 * time.Second
	consumerConfig.Consumer.Group.Heartbeat.Interval = 3 * time.Second

	// Enable debug logging
	consumerConfig.Net.DialTimeout = 10 * time.Second
	consumerConfig.Net.ReadTimeout = 10 * time.Second
	consumerConfig.Net.WriteTimeout = 10 * time.Second

	fmt.Println("Creating consumer group...")
	consumer, err := sarama.NewConsumerGroup([]string{"kafka-gateway:9093"}, "test-joingroup-debug-group", consumerConfig)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}
	defer consumer.Close()

	fmt.Println("Consumer group created successfully")

	// Consumer handler
	handler := &DebugConsumerGroupHandler{}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fmt.Println("Starting consumer...")
	err = consumer.Consume(ctx, []string{topicName}, handler)
	if err != nil {
		log.Printf("Consumer error: %v", err)
	}

	fmt.Printf("Consumer processed %d messages\n", handler.messageCount)
}

type DebugConsumerGroupHandler struct {
	messageCount int
}

func (h *DebugConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	fmt.Println("Consumer group setup called")
	return nil
}

func (h *DebugConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	fmt.Println("Consumer group cleanup called")
	return nil
}

func (h *DebugConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	fmt.Printf("ConsumeClaim called - partition %d, offset %d to %d\n",
		claim.Partition(), claim.InitialOffset(), claim.HighWaterMarkOffset())

	for message := range claim.Messages() {
		h.messageCount++
		fmt.Printf("Received message %d: %s (offset: %d)\n",
			h.messageCount, string(message.Value), message.Offset)
		session.MarkMessage(message, "")
	}
	return nil
}


