package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("Testing offset handling with individual consumers...")

	// Create producer
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3

	producer, err := sarama.NewSyncProducer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Create admin client
	adminClient, err := sarama.NewClusterAdmin([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Fatalf("Failed to create admin client: %v", err)
	}
	defer adminClient.Close()

	// Create topic
	topicName := fmt.Sprintf("test-individual-consumers-%d", time.Now().Unix())
	fmt.Printf("Creating topic: %s\n", topicName)

	err = adminClient.CreateTopic(topicName, &sarama.TopicDetail{
		NumPartitions:     2, // Use 2 partitions
		ReplicationFactor: 1,
	}, false)
	if err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}

	// Produce 10 messages
	fmt.Println("Producing 10 messages...")
	for i := 0; i < 10; i++ {
		message := fmt.Sprintf("individual-test-message-%d", i)
		_, _, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: topicName,
			Key:   sarama.StringEncoder(fmt.Sprintf("key-%d", i)),
			Value: sarama.StringEncoder(message),
		})
		if err != nil {
			log.Fatalf("Failed to send message: %v", err)
		}
		fmt.Printf("Produced message %d: %s\n", i, message)
	}

	// Wait for messages to be written
	time.Sleep(2 * time.Second)

	// Test 1: Single consumer reading all messages
	fmt.Println("\n=== Test 1: Single consumer ===")
	testSingleConsumer(topicName)

	// Wait a bit
	time.Sleep(2 * time.Second)

	// Test 2: Two consumers reading from different partitions
	fmt.Println("\n=== Test 2: Two consumers, different partitions ===")
	testTwoConsumersDifferentPartitions(topicName)

	// Wait a bit
	time.Sleep(2 * time.Second)

	// Test 3: Two consumers reading from same partition (should get different messages)
	fmt.Println("\n=== Test 3: Two consumers, same partition ===")
	testTwoConsumersSamePartition(topicName)
}

func testSingleConsumer(topicName string) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Printf("Failed to create consumer: %v", err)
		return
	}
	defer consumer.Close()

	// Read from both partitions
	partitionConsumer0, err := consumer.ConsumePartition(topicName, 0, sarama.OffsetOldest)
	if err != nil {
		log.Printf("Failed to create partition consumer 0: %v", err)
		return
	}
	defer partitionConsumer0.Close()

	partitionConsumer1, err := consumer.ConsumePartition(topicName, 1, sarama.OffsetOldest)
	if err != nil {
		log.Printf("Failed to create partition consumer 1: %v", err)
		return
	}
	defer partitionConsumer1.Close()

	messageCount := 0
	timeout := time.After(5 * time.Second)

	for {
		select {
		case message := <-partitionConsumer0.Messages():
			if message != nil {
				messageCount++
				fmt.Printf("Consumer received message from partition 0: %s (offset: %d)\n",
					string(message.Value), message.Offset)
			}
		case message := <-partitionConsumer1.Messages():
			if message != nil {
				messageCount++
				fmt.Printf("Consumer received message from partition 1: %s (offset: %d)\n",
					string(message.Value), message.Offset)
			}
		case <-timeout:
			fmt.Printf("Single consumer received %d messages\n", messageCount)
			return
		}
	}
}

func testTwoConsumersDifferentPartitions(topicName string) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// Consumer 1 - partition 0
	consumer1, err := sarama.NewConsumer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Printf("Failed to create consumer 1: %v", err)
		return
	}
	defer consumer1.Close()

	partitionConsumer1, err := consumer1.ConsumePartition(topicName, 0, sarama.OffsetOldest)
	if err != nil {
		log.Printf("Failed to create partition consumer 1: %v", err)
		return
	}
	defer partitionConsumer1.Close()

	// Consumer 2 - partition 1
	consumer2, err := sarama.NewConsumer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Printf("Failed to create consumer 2: %v", err)
		return
	}
	defer consumer2.Close()

	partitionConsumer2, err := consumer2.ConsumePartition(topicName, 1, sarama.OffsetOldest)
	if err != nil {
		log.Printf("Failed to create partition consumer 2: %v", err)
		return
	}
	defer partitionConsumer2.Close()

	var wg sync.WaitGroup
	consumer1Count := 0
	consumer2Count := 0

	wg.Add(2)

	go func() {
		defer wg.Done()
		timeout := time.After(5 * time.Second)
		for {
			select {
			case message := <-partitionConsumer1.Messages():
				if message != nil {
					consumer1Count++
					fmt.Printf("Consumer 1 (partition 0) received: %s (offset: %d)\n",
						string(message.Value), message.Offset)
				}
			case <-timeout:
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		timeout := time.After(5 * time.Second)
		for {
			select {
			case message := <-partitionConsumer2.Messages():
				if message != nil {
					consumer2Count++
					fmt.Printf("Consumer 2 (partition 1) received: %s (offset: %d)\n",
						string(message.Value), message.Offset)
				}
			case <-timeout:
				return
			}
		}
	}()

	wg.Wait()
	fmt.Printf("Consumer 1 received %d messages, Consumer 2 received %d messages\n",
		consumer1Count, consumer2Count)
}

func testTwoConsumersSamePartition(topicName string) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// Both consumers read from partition 0
	consumer1, err := sarama.NewConsumer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Printf("Failed to create consumer 1: %v", err)
		return
	}
	defer consumer1.Close()

	partitionConsumer1, err := consumer1.ConsumePartition(topicName, 0, sarama.OffsetOldest)
	if err != nil {
		log.Printf("Failed to create partition consumer 1: %v", err)
		return
	}
	defer partitionConsumer1.Close()

	consumer2, err := sarama.NewConsumer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Printf("Failed to create consumer 2: %v", err)
		return
	}
	defer consumer2.Close()

	partitionConsumer2, err := consumer2.ConsumePartition(topicName, 0, sarama.OffsetOldest)
	if err != nil {
		log.Printf("Failed to create partition consumer 2: %v", err)
		return
	}
	defer partitionConsumer2.Close()

	var wg sync.WaitGroup
	consumer1Count := 0
	consumer2Count := 0

	wg.Add(2)

	go func() {
		defer wg.Done()
		timeout := time.After(5 * time.Second)
		for {
			select {
			case message := <-partitionConsumer1.Messages():
				if message != nil {
					consumer1Count++
					fmt.Printf("Consumer 1 (partition 0) received: %s (offset: %d)\n",
						string(message.Value), message.Offset)
				}
			case <-timeout:
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		timeout := time.After(5 * time.Second)
		for {
			select {
			case message := <-partitionConsumer2.Messages():
				if message != nil {
					consumer2Count++
					fmt.Printf("Consumer 2 (partition 0) received: %s (offset: %d)\n",
						string(message.Value), message.Offset)
				}
			case <-timeout:
				return
			}
		}
	}()

	wg.Wait()
	fmt.Printf("Consumer 1 received %d messages, Consumer 2 received %d messages\n",
		consumer1Count, consumer2Count)
}


