package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("🧪 Layer 1: Core Kafka Gateway Functionality Test")
	fmt.Println("Testing: Basic produce/consume, timestamp fix, offset management")

	// Test 1: Regular topic (not system topic) to avoid Schema Registry interference
	testRegularTopicTimestamps()

	// Test 2: Multiple partitions
	testMultiPartitionHandling()

	// Test 3: Offset consistency
	testOffsetConsistency()
}

func testRegularTopicTimestamps() {
	fmt.Println("\n📋 Test 1.1: Regular Topic Timestamp Fix Verification")

	topicName := "test-timestamps-" + fmt.Sprintf("%d", time.Now().Unix())

	// Create producer
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Version = sarama.V2_8_0_0
	config.Net.DialTimeout = 10 * time.Second
	config.Net.ReadTimeout = 10 * time.Second
	config.Net.WriteTimeout = 10 * time.Second

	producer, err := sarama.NewSyncProducer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Printf("❌ Failed to create producer: %v", err)
		return
	}
	defer producer.Close()

	// Produce test message
	testKey := []byte(`{"test":"timestamp-key"}`)
	testValue := []byte(`{"test":"timestamp-value","produced_at":"` + time.Now().Format(time.RFC3339) + `"}`)

	msg := &sarama.ProducerMessage{
		Topic: topicName,
		Key:   sarama.ByteEncoder(testKey),
		Value: sarama.ByteEncoder(testValue),
	}

	fmt.Printf("🚀 Producing to topic: %s\n", topicName)
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Printf("❌ Failed to produce: %v", err)
		return
	}

	fmt.Printf("✅ Produced: partition=%d, offset=%d\n", partition, offset)

	// Wait a moment for the message to be stored
	time.Sleep(1 * time.Second)

	// Consume the message back
	consumerConfig := sarama.NewConfig()
	consumerConfig.Version = sarama.V2_8_0_0
	consumerConfig.Consumer.Return.Errors = true
	consumerConfig.Net.DialTimeout = 10 * time.Second
	consumerConfig.Net.ReadTimeout = 10 * time.Second

	consumer, err := sarama.NewConsumer([]string{"kafka-gateway:9093"}, consumerConfig)
	if err != nil {
		log.Printf("❌ Failed to create consumer: %v", err)
		return
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topicName, partition, offset)
	if err != nil {
		log.Printf("❌ Failed to create partition consumer: %v", err)
		return
	}
	defer partitionConsumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fmt.Printf("📖 Consuming from offset %d...\n", offset)

	select {
	case message := <-partitionConsumer.Messages():
		fmt.Printf("✅ Consumed message at offset %d\n", message.Offset)

		// Verify timestamp
		timestamp := message.Timestamp
		fmt.Printf("Timestamp: %s\n", timestamp.Format(time.RFC3339))
		fmt.Printf("Unix timestamp: %d\n", timestamp.Unix())

		now := time.Now()
		timeDiff := now.Sub(timestamp)
		if timeDiff < 0 {
			timeDiff = -timeDiff
		}

		if timeDiff < 1*time.Hour {
			fmt.Printf("✅ TIMESTAMP FIX VERIFIED! Time difference: %v\n", timeDiff)
		} else {
			fmt.Printf("❌ Timestamp still incorrect. Difference: %v\n", timeDiff)
		}

		// Verify content
		if string(message.Key) == string(testKey) && len(message.Value) > 0 {
			fmt.Printf("✅ Message content verified\n")
		} else {
			fmt.Printf("❌ Message content mismatch\n")
		}

	case err := <-partitionConsumer.Errors():
		log.Printf("❌ Consumer error: %v", err)

	case <-ctx.Done():
		fmt.Printf("❌ Timeout - message not received\n")
	}
}

func testMultiPartitionHandling() {
	fmt.Println("\n📋 Test 1.2: Multi-Partition Handling")

	// Create admin client to create topic with multiple partitions
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Net.DialTimeout = 10 * time.Second

	admin, err := sarama.NewClusterAdmin([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Printf("❌ Failed to create admin client: %v", err)
		return
	}
	defer admin.Close()

	topicName := "test-multipart-" + fmt.Sprintf("%d", time.Now().Unix())

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     3,
		ReplicationFactor: 1,
	}

	fmt.Printf("🚀 Creating topic %s with 3 partitions\n", topicName)
	err = admin.CreateTopic(topicName, topicDetail, false)
	if err != nil {
		log.Printf("❌ Failed to create topic: %v", err)
		return
	}

	fmt.Printf("✅ Topic created successfully\n")

	// Wait for topic to be ready
	time.Sleep(2 * time.Second)

	// Test producing to different partitions
	producerConfig := sarama.NewConfig()
	producerConfig.Producer.Return.Successes = true
	producerConfig.Producer.RequiredAcks = sarama.WaitForAll
	producerConfig.Version = sarama.V2_8_0_0
	producerConfig.Producer.Partitioner = sarama.NewManualPartitioner

	producer, err := sarama.NewSyncProducer([]string{"kafka-gateway:9093"}, producerConfig)
	if err != nil {
		log.Printf("❌ Failed to create producer: %v", err)
		return
	}
	defer producer.Close()

	// Produce to each partition
	for partition := int32(0); partition < 3; partition++ {
		msg := &sarama.ProducerMessage{
			Topic:     topicName,
			Partition: partition,
			Key:       sarama.StringEncoder(fmt.Sprintf("key-p%d", partition)),
			Value:     sarama.StringEncoder(fmt.Sprintf("value-p%d-time-%s", partition, time.Now().Format(time.RFC3339))),
		}

		resultPartition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Printf("❌ Failed to produce to partition %d: %v", partition, err)
			continue
		}

		fmt.Printf("✅ Produced to partition %d at offset %d\n", resultPartition, offset)
	}
}

func testOffsetConsistency() {
	fmt.Println("\n📋 Test 1.3: Offset Consistency")

	topicName := "test-offsets-" + fmt.Sprintf("%d", time.Now().Unix())

	// Create producer
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Version = sarama.V2_8_0_0

	producer, err := sarama.NewSyncProducer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Printf("❌ Failed to create producer: %v", err)
		return
	}
	defer producer.Close()

	// Produce multiple messages and track offsets
	var offsets []int64
	for i := 0; i < 5; i++ {
		msg := &sarama.ProducerMessage{
			Topic: topicName,
			Value: sarama.StringEncoder(fmt.Sprintf("message-%d", i)),
		}

		_, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Printf("❌ Failed to produce message %d: %v", i, err)
			continue
		}

		offsets = append(offsets, offset)
		fmt.Printf("Message %d: offset %d\n", i, offset)
	}

	// Verify offsets are sequential
	sequential := true
	for i := 1; i < len(offsets); i++ {
		if offsets[i] != offsets[i-1]+1 {
			sequential = false
			break
		}
	}

	if sequential {
		fmt.Printf("✅ Offsets are sequential\n")
	} else {
		fmt.Printf("❌ Offsets are not sequential: %v\n", offsets)
	}
}

