package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("🧪 Testing multiple messages to trigger buffer flush")

	// Kafka configuration
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Consumer.Return.Errors = true
	config.Version = sarama.V2_6_0_0

	brokers := []string{"loadtest-kafka-gateway-no-schema:9093"}

	// Create producer
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	topicName := "test-new-offset-metadata"

	// Produce multiple messages to trigger buffer flush
	for i := 0; i < 10; i++ {
		message := &sarama.ProducerMessage{
			Topic: topicName,
			Key:   sarama.StringEncoder(fmt.Sprintf("test-key-%d", i)),
			Value: sarama.StringEncoder(fmt.Sprintf("test-value-%d", i)),
		}

		partition, offset, err := producer.SendMessage(message)
		if err != nil {
			log.Fatalf("Failed to send message %d: %v", i, err)
		}

		fmt.Printf("✅ Message %d: partition=%d, offset=%d\n", i, partition, offset)
	}

	fmt.Println("✅ Sent 10 messages, waiting for buffer flush...")
	
	// Wait for buffer flush
	time.Sleep(10 * time.Second)

	// Create consumer to test reading
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Consume messages
	partitionConsumer, err := consumer.ConsumePartition(topicName, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Failed to create partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	// Read messages with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	messageCount := 0
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			messageCount++
			fmt.Printf("✅ Received message %d: partition=%d, offset=%d, key=%s, value=%s\n", 
				messageCount, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
			
			if messageCount >= 10 {
				fmt.Println("✅ Successfully received all 10 messages!")
				return
			}
		case err := <-partitionConsumer.Errors():
			log.Fatalf("❌ Consumer error: %v", err)
		case <-ctx.Done():
			fmt.Printf("⚠️  Timeout - received %d out of 10 messages\n", messageCount)
			return
		}
	}
}
