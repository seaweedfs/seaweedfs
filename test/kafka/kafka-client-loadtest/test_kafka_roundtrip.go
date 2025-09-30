package main

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("🧪 Testing Kafka Gateway write/read roundtrip")

	// Create a test topic first
	admin, err := sarama.NewClusterAdmin([]string{"loadtest-kafka-gateway-no-schema:9093"}, nil)
	if err != nil {
		log.Fatalf("❌ Failed to create admin client: %v", err)
	}
	defer admin.Close()

	topicName := "test-roundtrip-topic"

	// Delete topic if it exists
	admin.DeleteTopic(topicName)
	time.Sleep(2 * time.Second)

	// Create topic
	err = admin.CreateTopic(topicName, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
	if err != nil {
		log.Printf("⚠️  Failed to create topic (might already exist): %v", err)
	}
	defer admin.DeleteTopic(topicName)

	// Connect to Kafka Gateway
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	producer, err := sarama.NewSyncProducer([]string{"loadtest-kafka-gateway-no-schema:9093"}, config)
	if err != nil {
		log.Fatalf("❌ Failed to create producer: %v", err)
	}
	defer producer.Close()

	consumer, err := sarama.NewConsumer([]string{"loadtest-kafka-gateway-no-schema:9093"}, config)
	if err != nil {
		log.Fatalf("❌ Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Subscribe to topic
	partitionConsumer, err := consumer.ConsumePartition(topicName, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("❌ Failed to consume partition: %v", err)
	}
	defer partitionConsumer.Close()

	// Test message
	testKey := "test-key"
	testValue := "test-value"

	// Send message
	msg := &sarama.ProducerMessage{
		Topic: topicName,
		Key:   sarama.StringEncoder(testKey),
		Value: sarama.StringEncoder(testValue),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Fatalf("❌ Failed to send message: %v", err)
	}

	fmt.Printf("✅ Successfully sent message: partition=%d, offset=%d\n", partition, offset)
	fmt.Printf("   Key: %s\n", testKey)
	fmt.Printf("   Value: %s\n", testValue)

	// Wait for message to be consumed
	select {
	case msg := <-partitionConsumer.Messages():
		fmt.Printf("✅ Successfully received message: partition=%d, offset=%d\n", msg.Partition, msg.Offset)
		fmt.Printf("   Key: %s\n", string(msg.Key))
		fmt.Printf("   Value: %s\n", string(msg.Value))

		if string(msg.Key) != testKey || string(msg.Value) != testValue {
			log.Fatalf("❌ Message content mismatch!")
		}

		fmt.Println("✅ Roundtrip test PASSED!")

	case <-time.After(10 * time.Second):
		log.Fatalf("❌ Timeout waiting for message")
	}
}
