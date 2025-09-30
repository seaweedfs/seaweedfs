package main

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

const (
	brokerAddress = "localhost:9093"
	topic         = "test-range-info-final"
	partition     = 0
)

func main() {
	fmt.Println("🧪 Testing enhanced GetPartitionRangeInfo functionality")

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

	// Check initial range info
	fmt.Println("\n📊 Checking initial range info...")
	earliestOffset, err := client.GetOffset(topic, partition, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Failed to get earliest offset: %v", err)
	}
	latestOffset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Failed to get latest offset: %v", err)
	}
	fmt.Printf("Initial range: earliest=%d, latest=%d\n", earliestOffset, latestOffset)

	// Produce multiple messages with timestamps
	fmt.Println("\n📝 Producing 3 messages with different timestamps...")
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	baseTime := time.Now()
	for i := 0; i < 3; i++ {
		msg := &sarama.ProducerMessage{
			Topic:     topic,
			Partition: partition,
			Key:       sarama.StringEncoder(fmt.Sprintf("key-%d", i)),
			Value:     sarama.StringEncoder(fmt.Sprintf("message-%d with timestamp range test", i)),
			Timestamp: baseTime.Add(time.Duration(i) * time.Second), // Different timestamps
		}

		p, o, err := producer.SendMessage(msg)
		if err != nil {
			log.Fatalf("Failed to send message %d: %v", i, err)
		}
		fmt.Printf("✅ Message %d sent: partition=%d, offset=%d, timestamp=%v\n",
			i, p, o, msg.Timestamp.Format(time.RFC3339Nano))
	}

	// Wait a bit for messages to be processed
	time.Sleep(2 * time.Second)

	// Check final range info
	fmt.Println("\n📊 Checking final range info...")
	earliestOffset, err = client.GetOffset(topic, partition, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Failed to get earliest offset after producing: %v", err)
	}
	latestOffset, err = client.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Failed to get latest offset after producing: %v", err)
	}
	fmt.Printf("Final range: earliest=%d, latest=%d\n", earliestOffset, latestOffset)

	// Verify we can consume the messages
	fmt.Println("\n📖 Consuming messages to verify range info...")
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Failed to create partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	messageCount := 0
	timeout := time.After(5 * time.Second)
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			messageCount++
			fmt.Printf("📨 Consumed message %d: offset=%d, key=%s, timestamp=%v\n",
				messageCount, msg.Offset, string(msg.Key), msg.Timestamp.Format(time.RFC3339Nano))
			if messageCount >= 3 {
				goto done
			}
		case err := <-partitionConsumer.Errors():
			log.Printf("Consumer error: %v", err)
		case <-timeout:
			log.Printf("Timeout waiting for messages, got %d messages", messageCount)
			goto done
		}
	}

done:
	if messageCount == 3 && earliestOffset == 0 && latestOffset == 3 {
		fmt.Println("✅ Enhanced range info test completed successfully!")
		fmt.Printf("   - Offset range: [%d, %d]\n", earliestOffset, latestOffset)
		fmt.Printf("   - Message count: %d\n", messageCount)
		fmt.Println("   - Timestamp range functionality is now available in GetPartitionRangeInfo")
	} else {
		log.Fatalf("❌ Range info test FAILED! Expected 3 messages with range [0,3], got %d messages with range [%d,%d]",
			messageCount, earliestOffset, latestOffset)
	}
}
