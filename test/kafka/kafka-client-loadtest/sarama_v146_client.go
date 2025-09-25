package main

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("=== 🧪 COMPREHENSIVE SARAMA v1.46.1 TEST ===")

	// Configure Sarama
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0 // Use a modern Kafka version
	config.Consumer.Return.Errors = true
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3
	config.Producer.Return.Errors = true

	// Test 1: Create client and get metadata
	fmt.Println("\n=== 📋 TEST 1: METADATA REQUEST ===")
	client, err := sarama.NewClient([]string{"localhost:9093"}, config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	fmt.Println("✅ Client created successfully")

	// Get all topics
	topics, err := client.Topics()
	if err != nil {
		log.Fatalf("Failed to get topics: %v", err)
	}
	fmt.Printf("✅ Topics retrieved: %v\n", topics)

	// Get brokers
	brokers := client.Brokers()
	fmt.Printf("✅ Brokers: %d\n", len(brokers))
	for i, broker := range brokers {
		fmt.Printf("  Broker[%d]: %s\n", i, broker.Addr())
	}

	// Test 2: Producer functionality
	fmt.Println("\n=== 📤 TEST 2: PRODUCER ===")
	producer, err := sarama.NewSyncProducer([]string{"localhost:9093"}, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Send a test message
	message := &sarama.ProducerMessage{
		Topic: "test-sarama-v146",
		Key:   sarama.StringEncoder("test-key"),
		Value: sarama.StringEncoder("Hello from Sarama v1.46.1!"),
	}

	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}
	fmt.Printf("✅ Message sent to partition %d at offset %d\n", partition, offset)

	// Test 3: Consumer functionality
	fmt.Println("\n=== 📥 TEST 3: CONSUMER ===")
	consumer, err := sarama.NewConsumer([]string{"localhost:9093"}, config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Get partitions for the test topic
	partitions, err := consumer.Partitions("test-sarama-v146")
	if err != nil {
		fmt.Printf("⚠️ Topic doesn't exist yet (expected): %v\n", err)
	} else {
		fmt.Printf("✅ Topic partitions: %v\n", partitions)
		
		// Try to consume from the first partition
		if len(partitions) > 0 {
			partitionConsumer, err := consumer.ConsumePartition("test-sarama-v146", partitions[0], sarama.OffsetNewest)
			if err != nil {
				fmt.Printf("⚠️ Failed to create partition consumer: %v\n", err)
			} else {
				defer partitionConsumer.Close()
				
				// Try to read a message with timeout
				select {
				case msg := <-partitionConsumer.Messages():
					fmt.Printf("✅ Consumed message: %s\n", string(msg.Value))
				case <-time.After(2 * time.Second):
					fmt.Println("⚠️ No messages received (expected for new topic)")
				}
			}
		}
	}

	// Test 4: Admin operations (if available)
	fmt.Println("\n=== ⚙️ TEST 4: ADMIN OPERATIONS ===")
	clusterAdmin, err := sarama.NewClusterAdmin([]string{"localhost:9093"}, config)
	if err != nil {
		fmt.Printf("⚠️ Failed to create cluster admin: %v\n", err)
	} else {
		defer clusterAdmin.Close()
		
		// Try to create a topic
		topicDetail := &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
		}
		
		err = clusterAdmin.CreateTopic("sarama-v146-admin-test", topicDetail, false)
		if err != nil {
			fmt.Printf("⚠️ Topic creation failed (may already exist): %v\n", err)
		} else {
			fmt.Println("✅ Topic created successfully")
		}
		
		// List topics
		topics, err := clusterAdmin.ListTopics()
		if err != nil {
			fmt.Printf("⚠️ Failed to list topics: %v\n", err)
		} else {
			fmt.Printf("✅ Admin topics list: %d topics\n", len(topics))
		}
	}

	fmt.Println("\n🎯 Sarama v1.46.1 comprehensive test completed!")
}
