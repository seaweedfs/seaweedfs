package main

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("🔍 Debugging _schemas topic fetch issue")
	fmt.Println("=====================================")

	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true
	config.ClientID = "debug-schemas-fetch"

	// Create client to check offsets
	client, err := sarama.NewClient([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Fatalf("❌ Failed to create client: %v", err)
	}
	defer client.Close()

	topicName := "_schemas"

	fmt.Printf("📊 Checking %s topic...\n", topicName)

	// Check if topic exists and get offset info
	partitions, err := client.Partitions(topicName)
	if err != nil {
		log.Fatalf("❌ Failed to get partitions: %v", err)
	}
	fmt.Printf("📊 Topic %s has %d partitions\n", topicName, len(partitions))

	for _, partition := range partitions {
		oldest, err := client.GetOffset(topicName, partition, sarama.OffsetOldest)
		if err != nil {
			log.Printf("⚠️  Failed to get oldest offset for partition %d: %v", partition, err)
			continue
		}

		newest, err := client.GetOffset(topicName, partition, sarama.OffsetNewest)
		if err != nil {
			log.Printf("⚠️  Failed to get newest offset for partition %d: %v", partition, err)
			continue
		}

		fmt.Printf("📊 Partition %d: oldest=%d, newest=%d, messages=%d\n",
			partition, oldest, newest, newest-oldest)

		// Test consuming from different offsets
		testConsumeFromOffset(topicName, partition, 0, "offset 0")
		testConsumeFromOffset(topicName, partition, 1, "offset 1")
		testConsumeFromOffset(topicName, partition, 2, "offset 2")
		testConsumeFromOffset(topicName, partition, sarama.OffsetOldest, "oldest")
	}
}

func testConsumeFromOffset(topicName string, partition int32, offset int64, description string) {
	fmt.Printf("\n🔍 Testing consume from %s (offset %d)...\n", description, offset)

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.ClientID = fmt.Sprintf("debug-consumer-%s", description)

	consumer, err := sarama.NewConsumer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		fmt.Printf("❌ Failed to create consumer: %v\n", err)
		return
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topicName, partition, offset)
	if err != nil {
		fmt.Printf("❌ Failed to consume partition: %v\n", err)
		return
	}
	defer partitionConsumer.Close()

	fmt.Printf("⏳ Waiting for messages (5s timeout)...\n")

	messageCount := 0
	timeout := time.After(5 * time.Second)

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			messageCount++
			fmt.Printf("✅ Message %d: offset=%d, timestamp=%v, key_len=%d, value_len=%d\n",
				messageCount, msg.Offset, msg.Timestamp, len(msg.Key), len(msg.Value))

			if len(msg.Key) > 0 {
				fmt.Printf("   Key: %s\n", string(msg.Key))
			}

			// Only read first few messages to avoid hanging
			if messageCount >= 3 {
				fmt.Printf("📊 Successfully read %d messages from %s\n", messageCount, description)
				return
			}

		case err := <-partitionConsumer.Errors():
			fmt.Printf("❌ Consumer error: %v\n", err)
			return

		case <-timeout:
			if messageCount == 0 {
				fmt.Printf("⏰ Timeout - no messages received from %s\n", description)
			} else {
				fmt.Printf("📊 Timeout - received %d messages from %s\n", messageCount, description)
			}
			return
		}
	}
}
