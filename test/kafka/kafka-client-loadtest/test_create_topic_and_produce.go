package main

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("=== Creating Topic and Testing Produce Response Timing ===")

	// Create admin client to create topic
	config := sarama.NewConfig()
	admin, err := sarama.NewClusterAdmin([]string{"localhost:9093"}, config)
	if err != nil {
		log.Fatalf("Failed to create admin client: %v", err)
	}
	defer admin.Close()

	// Create topic
	topicName := "test-response-format"
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	fmt.Printf("Creating topic: %s\n", topicName)
	err = admin.CreateTopic(topicName, topicDetail, false)
	if err != nil {
		log.Printf("Failed to create topic (might already exist): %v", err)
	} else {
		fmt.Println("Topic created successfully")
	}

	// Wait a moment for topic to be ready
	time.Sleep(2 * time.Second)

	// Now test produce with timing
	producerConfig := sarama.NewConfig()
	producerConfig.Producer.Return.Successes = true
	producerConfig.Producer.RequiredAcks = sarama.WaitForAll // acks=-1 like Schema Registry
	producerConfig.Producer.Timeout = 500 * time.Millisecond // Same timeout as Schema Registry

	producer, err := sarama.NewSyncProducer([]string{"localhost:9093"}, producerConfig)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Test 1: Regular message
	fmt.Println("\n=== Test 1: Regular Message ===")
	message := &sarama.ProducerMessage{
		Topic: topicName,
		Key:   sarama.StringEncoder("test-key"),
		Value: sarama.StringEncoder("test-value"),
	}

	start := time.Now()
	partition, offset, err := producer.SendMessage(message)
	duration := time.Since(start)

	if err != nil {
		log.Printf("❌ Failed to send regular message: %v", err)
		log.Printf("Duration: %v", duration)
	} else {
		fmt.Printf("✅ Regular message sent successfully!\n")
		fmt.Printf("Partition: %d, Offset: %d\n", partition, offset)
		fmt.Printf("Duration: %v\n", duration)

		if duration > 500*time.Millisecond {
			fmt.Printf("⚠️  WARNING: Duration exceeds Schema Registry timeout!\n")
		}
	}

	// Test 2: Null value message (like Schema Registry Noop)
	fmt.Println("\n=== Test 2: Null Value Message (Schema Registry Noop style) ===")
	nullMessage := &sarama.ProducerMessage{
		Topic: topicName,
		Key:   sarama.StringEncoder("noop-key"),
		Value: nil, // null value like Schema Registry Noop records
	}

	start = time.Now()
	partition, offset, err = producer.SendMessage(nullMessage)
	duration = time.Since(start)

	if err != nil {
		log.Printf("❌ Failed to send null message: %v", err)
		log.Printf("Duration: %v", duration)
	} else {
		fmt.Printf("✅ Null message sent successfully!\n")
		fmt.Printf("Partition: %d, Offset: %d\n", partition, offset)
		fmt.Printf("Duration: %v\n", duration)

		if duration > 500*time.Millisecond {
			fmt.Printf("⚠️  WARNING: Duration exceeds Schema Registry timeout!\n")
		}
	}

	// Test 3: Multiple rapid messages to test consistency
	fmt.Println("\n=== Test 3: Multiple Rapid Messages ===")
	for i := 0; i < 5; i++ {
		testMsg := &sarama.ProducerMessage{
			Topic: topicName,
			Key:   sarama.StringEncoder(fmt.Sprintf("key-%d", i)),
			Value: sarama.StringEncoder(fmt.Sprintf("value-%d", i)),
		}

		start = time.Now()
		partition, offset, err = producer.SendMessage(testMsg)
		duration = time.Since(start)

		if err != nil {
			log.Printf("❌ Message %d failed: %v (duration: %v)", i, err, duration)
		} else {
			fmt.Printf("✅ Message %d: partition=%d, offset=%d, duration=%v\n", i, partition, offset, duration)
			if duration > 500*time.Millisecond {
				fmt.Printf("⚠️  Message %d exceeds Schema Registry timeout!\n", i)
			}
		}
	}
}

