package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("🔧 Testing Basic Kafka Gateway Functionality")
	fmt.Println("Testing basic produce/consume without Schema Registry")

	// Kafka Gateway configuration
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	brokers := []string{"localhost:9093"}
	topic := fmt.Sprintf("test-basic-%d", time.Now().Unix())

	fmt.Printf("📋 Testing topic: %s\n", topic)

	// Test 1: Create Producer
	fmt.Println("\n1️⃣  Creating producer...")
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("❌ Failed to create producer: %v", err)
	}
	defer producer.Close()
	fmt.Println("✅ Producer created successfully")

	// Test 2: Produce Messages
	fmt.Println("\n2️⃣  Producing test messages...")
	for i := 0; i < 3; i++ {
		message := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(fmt.Sprintf("key-%d", i)),
			Value: sarama.StringEncoder(fmt.Sprintf(`{"id": %d, "message": "test message %d", "timestamp": "%s"}`, i, i, time.Now().Format(time.RFC3339))),
		}

		partition, offset, err := producer.SendMessage(message)
		if err != nil {
			log.Fatalf("❌ Failed to send message %d: %v", i, err)
		}
		fmt.Printf("✅ Message %d sent to partition %d, offset %d\n", i, partition, offset)
	}

	// Test 3: Create Consumer
	fmt.Println("\n3️⃣  Creating consumer...")
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("❌ Failed to create consumer: %v", err)
	}
	defer consumer.Close()
	fmt.Println("✅ Consumer created successfully")

	// Test 4: Consume Messages
	fmt.Println("\n4️⃣  Consuming messages...")
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("❌ Failed to create partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	messageCount := 0
	for {
		select {
		case message := <-partitionConsumer.Messages():
			messageCount++
			fmt.Printf("✅ Consumed message %d: key=%s, value=%s, offset=%d, timestamp=%s\n",
				messageCount,
				string(message.Key),
				string(message.Value),
				message.Offset,
				message.Timestamp.Format(time.RFC3339))

			if messageCount >= 3 {
				fmt.Println("\n🎉 All messages consumed successfully!")
				return
			}

		case err := <-partitionConsumer.Errors():
			log.Fatalf("❌ Consumer error: %v", err)

		case <-ctx.Done():
			fmt.Printf("\n⚠️  Timeout reached. Consumed %d out of 3 messages\n", messageCount)
			return
		}
	}
}

