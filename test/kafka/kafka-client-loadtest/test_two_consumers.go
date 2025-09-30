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

	topicName := "test-two-consumers"
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

	// Create first consumer
	consumer1Config := sarama.NewConfig()
	consumer1Config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	consumer1Config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer1, err := sarama.NewConsumerGroup([]string{"kafka-gateway:9093"}, "group-1", consumer1Config)
	if err != nil {
		log.Fatalf("Failed to create consumer 1: %v", err)
	}
	defer consumer1.Close()

	// Create second consumer
	consumer2Config := sarama.NewConfig()
	consumer2Config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	consumer2Config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer2, err := sarama.NewConsumerGroup([]string{"kafka-gateway:9093"}, "group-2", consumer2Config)
	if err != nil {
		log.Fatalf("Failed to create consumer 2: %v", err)
	}
	defer consumer2.Close()

	// Consumer handler
	handler := &ConsumerGroupHandler{id: "unknown"}

	// Start consumers in goroutines
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	go func() {
		handler.id = "consumer-1"
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err := consumer1.Consume(ctx, []string{topicName}, handler)
				if err != nil {
					log.Printf("Consumer 1 error: %v", err)
					return
				}
			}
		}
	}()

	go func() {
		handler.id = "consumer-2"
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err := consumer2.Consume(ctx, []string{topicName}, handler)
				if err != nil {
					log.Printf("Consumer 2 error: %v", err)
					return
				}
			}
		}
	}()

	// Wait for messages
	time.Sleep(10 * time.Second)
	fmt.Printf("Consumer 1 received %d messages\n", handler.consumer1Count)
	fmt.Printf("Consumer 2 received %d messages\n", handler.consumer2Count)
}

type ConsumerGroupHandler struct {
	id            string
	consumer1Count int
	consumer2Count int
}

func (h *ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		if h.id == "consumer-1" {
			h.consumer1Count++
		} else {
			h.consumer2Count++
		}
		fmt.Printf("%s received message: %s\n", h.id, string(message.Value))
		session.MarkMessage(message, "")
	}
	return nil
}


