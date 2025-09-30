package main

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

const (
	brokerAddress = "loadtest-kafka-gateway-no-schema:9093"
	topic         = "test-native-offsets-fresh"
	partition     = 0
)

func main() {
	fmt.Println("🧪 Testing native offset management with fresh topic")

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
	fmt.Printf("✅ Fresh topic ready: %s\n", topic)

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

	// Produce 2 messages
	fmt.Println("\n📝 Producing 2 messages...")
	for i := 0; i < 2; i++ {
		msg := &sarama.ProducerMessage{
			Topic:     topic,
			Partition: partition,
			Key:       sarama.StringEncoder(fmt.Sprintf("native-key-%d", i)),
			Value:     sarama.StringEncoder(fmt.Sprintf("native-value-%d", i)),
			Timestamp: time.Now(),
		}

		p, o, err := producer.SendMessage(msg)
		if err != nil {
			log.Fatalf("Failed to send message %d: %v", i, err)
		}
		fmt.Printf("✅ Message %d: partition=%d, offset=%d\n", i, p, o)
	}

	// Wait a moment for messages to be processed
	time.Sleep(2 * time.Second)

	// Get offsets after producing messages
	fmt.Println("\n📊 Checking offsets after producing messages...")
	earliestOffset, err = client.GetOffset(topic, partition, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Failed to get earliest offset: %v", err)
	}
	latestOffset, err = client.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Failed to get latest offset: %v", err)
	}
	fmt.Printf("After producing: earliest=%d, latest=%d\n", earliestOffset, latestOffset)

	fmt.Println("✅ Native offset management test completed!")
}
