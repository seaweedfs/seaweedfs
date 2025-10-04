package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	// Allow broker to be configured via environment variable
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9093"
	}
	brokers := []string{broker}
	topic := "loadtest-topic-0"
	groupID := "debug-consumer-group"

	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRange()
	config.Consumer.MaxWaitTime = 5 * time.Second
	config.Net.DialTimeout = 10 * time.Second
	config.Net.ReadTimeout = 30 * time.Second
	config.Net.WriteTimeout = 30 * time.Second
	config.ClientID = "debug-consumer"

	log.Printf("🔍 DEBUG CONSUMER: Connecting to %v", brokers)
	log.Printf("🔍 Topic: %s, Group: %s", topic, groupID)

	// Create consumer group
	group, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		log.Fatalf("Error creating consumer group: %v", err)
	}
	defer group.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	consumer := &Consumer{
		ready: make(chan bool),
	}

	go func() {
		for {
			log.Printf("🔍 Starting consume session...")
			if err := group.Consume(ctx, []string{topic}, consumer); err != nil {
				log.Printf("❌ Error from consumer: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready
	log.Printf("✅ Consumer ready and consuming from %s", topic)

	// Run for 30 seconds or until interrupted
	ticker := time.NewTicker(5 * time.Second)
	timeout := time.After(30 * time.Second)

	for {
		select {
		case <-ticker.C:
			log.Printf("📊 Messages consumed so far: %d", consumer.messageCount)
		case <-timeout:
			log.Printf("⏰ 30 seconds elapsed, shutting down...")
			cancel()
			return
		case <-sigterm:
			log.Printf("🛑 Received interrupt signal, shutting down...")
			cancel()
			return
		case err := <-group.Errors():
			log.Printf("❌ Consumer error: %v", err)
		}
	}
}

type Consumer struct {
	ready        chan bool
	messageCount int
}

func (c *Consumer) Setup(session sarama.ConsumerGroupSession) error {
	log.Printf("🔧 Consumer Setup called")
	log.Printf("   MemberID: %s", session.MemberID())
	log.Printf("   GenerationID: %d", session.GenerationID())

	// Log assigned partitions
	for topic, partitions := range session.Claims() {
		log.Printf("   Assigned: %s partitions %v", topic, partitions)
	}

	close(c.ready)
	return nil
}

func (c *Consumer) Cleanup(session sarama.ConsumerGroupSession) error {
	log.Printf("🧹 Consumer Cleanup called")
	return nil
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	log.Printf("🎯 ConsumeClaim started for %s[%d] at offset %d",
		claim.Topic(), claim.Partition(), claim.InitialOffset())

	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				log.Printf("⚠️  Received nil message, channel closed?")
				return nil
			}
			c.messageCount++
			log.Printf("📨 Message %d: topic=%s partition=%d offset=%d key=%s valueLen=%d",
				c.messageCount, message.Topic, message.Partition, message.Offset,
				string(message.Key), len(message.Value))

			// Print first 50 bytes of value
			if len(message.Value) > 0 {
				maxLen := 50
				if len(message.Value) < maxLen {
					maxLen = len(message.Value)
				}
				log.Printf("   Value (first %d bytes): %x", maxLen, message.Value[:maxLen])
			}

			session.MarkMessage(message, "")

		case <-session.Context().Done():
			log.Printf("⚠️  Session context cancelled")
			return nil
		}
	}
}
