package main

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

const (
	brokerAddress = "localhost:9093"
	topic         = "test-partition-mapping-enhanced"
	partition     = 0
)

func main() {
	fmt.Println("🧪 Testing proper partition mapping")

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Timeout = 5 * time.Second
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// Create a new Kafka client
	client, err := sarama.NewClient([]string{brokerAddress}, config)
	if err != nil {
		log.Fatalf("Failed to create Kafka client: %v", err)
	}
	defer client.Close()

	// Create topic if it doesn't exist
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		log.Fatalf("Failed to create Kafka cluster admin: %v", err)
	}
	defer admin.Close()

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	err = admin.CreateTopic(topic, topicDetail, false)
	if err != nil && err.Error() != "kafka server: Topic with this name already exists" {
		log.Fatalf("Failed to create topic %s: %v", topic, err)
	}
	fmt.Printf("✅ Topic ready: %s\n", topic)

	// Get initial offsets
	fmt.Println("\n📊 Checking initial offsets...")
	earliestOffset, err := client.GetOffset(topic, partition, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Failed to get earliest offset: %v", err)
	}
	latestOffset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Failed to get latest offset: %v", err)
	}
	fmt.Printf("Initial: earliest=%d, latest=%d\n", earliestOffset, latestOffset)

	// Create a producer
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Produce 1 message
	fmt.Println("\n📝 Producing 1 message...")
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: partition,
		Key:       sarama.StringEncoder("partition-test-key"),
		Value:     sarama.StringEncoder("partition-test-value"),
		Timestamp: time.Now(),
	}

	p, o, err := producer.SendMessage(msg)
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}
	fmt.Printf("✅ Message sent: partition=%d, offset=%d\n", p, o)

	// Wait a moment for messages to be processed
	time.Sleep(2 * time.Second)

	// Get offsets after producing messages
	fmt.Println("\n📊 Checking offsets after producing message...")
	earliestOffset, err = client.GetOffset(topic, partition, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Failed to get earliest offset: %v", err)
	}
	latestOffset, err = client.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Failed to get latest offset: %v", err)
	}
	fmt.Printf("After producing: earliest=%d, latest=%d\n", earliestOffset, latestOffset)

	fmt.Println("✅ Partition mapping test completed!")
}
