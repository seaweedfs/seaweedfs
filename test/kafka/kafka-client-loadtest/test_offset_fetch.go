package main

import (
	"context"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	log.Println("=== Testing OffsetFetch with Debug Sarama ===")

	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 100 * time.Millisecond
	config.Consumer.Group.Session.Timeout = 30 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 3 * time.Second

	brokers := []string{"localhost:9093"}
	group := "test-offset-fetch-group"
	topics := []string{"loadtest-topic-0"}

	log.Printf("Creating consumer group: group=%s brokers=%v topics=%v", group, brokers, topics)

	consumerGroup, err := sarama.NewConsumerGroup(brokers, group, config)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}
	defer consumerGroup.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	handler := &testHandler{}

	log.Println("Starting consumer group session...")
	log.Println("Watch for 🔍 [SARAMA-DEBUG] logs to trace OffsetFetch calls")

	go func() {
		for {
			if err := consumerGroup.Consume(ctx, topics, handler); err != nil {
				log.Printf("Error from consumer: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	// Wait for context to be done
	<-ctx.Done()
	log.Println("Test completed")
}

type testHandler struct{}

func (h *testHandler) Setup(session sarama.ConsumerGroupSession) error {
	log.Printf("✓ Consumer group session setup: generation=%d memberID=%s", session.GenerationID(), session.MemberID())
	return nil
}

func (h *testHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	log.Println("Consumer group session cleanup")
	return nil
}

func (h *testHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	log.Printf("✓ Started consuming: topic=%s partition=%d offset=%d", claim.Topic(), claim.Partition(), claim.InitialOffset())

	count := 0
	for message := range claim.Messages() {
		count++
		log.Printf("  Received message #%d: offset=%d", count, message.Offset)
		session.MarkMessage(message, "")

		if count >= 5 {
			log.Println("Received 5 messages, stopping")
			return nil
		}
	}
	return nil
}
