package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

// Test offset ledger consistency - verify that high water mark matches available entries
func main() {
	fmt.Println("🔍 Testing Offset Ledger Consistency")
	fmt.Println(strings.Repeat("=", 50))

	// Test 1: Check _schemas topic ledger consistency
	fmt.Println("\n1️⃣  Testing _schemas Topic Ledger Consistency...")
	testSchemasTopicLedgerConsistency()

	// Test 2: Produce messages and verify ledger state
	fmt.Println("\n2️⃣  Testing Produce-Fetch Ledger Consistency...")
	testProduceFetchConsistency()

	// Test 3: Test rapid produce and immediate fetch
	fmt.Println("\n3️⃣  Testing Rapid Produce-Immediate Fetch...")
	testRapidProduceImmediateFetch()

	fmt.Println("\n🎯 Offset Ledger Consistency Tests Completed!")
}

func testSchemasTopicLedgerConsistency() {
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	client, err := sarama.NewClient([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		fmt.Printf("❌ Failed to create client: %v\n", err)
		return
	}
	defer client.Close()

	topic := "_schemas"
	partition := int32(0)

	// Get offset information
	oldest, err := client.GetOffset(topic, partition, sarama.OffsetOldest)
	if err != nil {
		fmt.Printf("❌ Failed to get oldest offset: %v\n", err)
		return
	}

	newest, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		fmt.Printf("❌ Failed to get newest offset: %v\n", err)
		return
	}

	fmt.Printf("   📊 %s Topic State: oldest=%d, newest=%d, expected_available=%d\n",
		topic, oldest, newest, newest-oldest)

	// Try to consume from different offsets to verify availability
	testOffsets := []int64{oldest, oldest + 1, newest - 3, newest - 2, newest - 1}

	for _, testOffset := range testOffsets {
		if testOffset < oldest || testOffset >= newest {
			continue // Skip invalid offsets
		}

		fmt.Printf("   🔍 Testing fetch from offset %d...", testOffset)

		consumer, err := sarama.NewConsumer([]string{"kafka-gateway:9093"}, config)
		if err != nil {
			fmt.Printf(" ❌ Failed to create consumer: %v\n", err)
			continue
		}

		partitionConsumer, err := consumer.ConsumePartition(topic, partition, testOffset)
		if err != nil {
			fmt.Printf(" ❌ Failed to create partition consumer: %v\n", err)
			consumer.Close()
			continue
		}

		// Try to consume one message
		select {
		case message := <-partitionConsumer.Messages():
			fmt.Printf(" ✅ Got message at offset %d (expected %d)\n", message.Offset, testOffset)

		case err := <-partitionConsumer.Errors():
			fmt.Printf(" ❌ Consumer error: %v\n", err)

		case <-time.After(2 * time.Second):
			fmt.Printf(" ❌ Timeout - no message available\n")
		}

		partitionConsumer.Close()
		consumer.Close()
	}
}

func testProduceFetchConsistency() {
	topic := "ledger-consistency-test"
	partition := int32(0)

	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	// Create topic first
	admin, err := sarama.NewClusterAdmin([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		fmt.Printf("❌ Failed to create admin: %v\n", err)
		return
	}
	defer admin.Close()

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	err = admin.CreateTopic(topic, topicDetail, false)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		fmt.Printf("❌ Failed to create topic: %v\n", err)
		return
	}

	fmt.Printf("   ✅ Topic %s created/exists\n", topic)

	// Create producer
	producer, err := sarama.NewSyncProducer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		fmt.Printf("❌ Failed to create producer: %v\n", err)
		return
	}
	defer producer.Close()

	// Create client for offset checking
	client, err := sarama.NewClient([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		fmt.Printf("❌ Failed to create client: %v\n", err)
		return
	}
	defer client.Close()

	// Produce messages and check consistency after each
	for i := 0; i < 5; i++ {
		// Get state before produce
		beforeNewest, _ := client.GetOffset(topic, partition, sarama.OffsetNewest)

		// Produce message
		message := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(fmt.Sprintf("key-%d", i)),
			Value: sarama.StringEncoder(fmt.Sprintf("value-%d", i)),
		}

		produceStart := time.Now()
		producedPartition, producedOffset, err := producer.SendMessage(message)
		produceDuration := time.Since(produceStart)

		if err != nil {
			fmt.Printf("   ❌ Message %d produce failed: %v\n", i, err)
			continue
		}

		// Get state after produce
		afterNewest, _ := client.GetOffset(topic, partition, sarama.OffsetNewest)

		fmt.Printf("   📨 Message %d: partition=%d, offset=%d, duration=%v\n",
			i, producedPartition, producedOffset, produceDuration)
		fmt.Printf("       Before: newest=%d, After: newest=%d, Delta: %d\n",
			beforeNewest, afterNewest, afterNewest-beforeNewest)

		// Immediately try to fetch the message we just produced
		fetchStart := time.Now()
		consumer, err := sarama.NewConsumer([]string{"kafka-gateway:9093"}, config)
		if err != nil {
			fmt.Printf("       ❌ Failed to create consumer: %v\n", err)
			continue
		}

		partitionConsumer, err := consumer.ConsumePartition(topic, partition, producedOffset)
		if err != nil {
			fmt.Printf("       ❌ Failed to create partition consumer: %v\n", err)
			consumer.Close()
			continue
		}

		// Try to consume the message we just produced
		select {
		case fetchedMessage := <-partitionConsumer.Messages():
			fetchDuration := time.Since(fetchStart)
			fmt.Printf("       ✅ Immediate fetch: offset=%d, duration=%v\n",
				fetchedMessage.Offset, fetchDuration)

		case err := <-partitionConsumer.Errors():
			fmt.Printf("       ❌ Immediate fetch error: %v\n", err)

		case <-time.After(1 * time.Second):
			fmt.Printf("       ❌ Immediate fetch timeout\n")
		}

		partitionConsumer.Close()
		consumer.Close()

		time.Sleep(100 * time.Millisecond)
	}
}

func testRapidProduceImmediateFetch() {
	topic := "rapid-consistency-test"
	partition := int32(0)

	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	// Create topic first
	admin, err := sarama.NewClusterAdmin([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		fmt.Printf("❌ Failed to create admin: %v\n", err)
		return
	}
	defer admin.Close()

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	err = admin.CreateTopic(topic, topicDetail, false)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		fmt.Printf("❌ Failed to create topic: %v\n", err)
		return
	}

	// Create producer
	producer, err := sarama.NewSyncProducer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		fmt.Printf("❌ Failed to create producer: %v\n", err)
		return
	}
	defer producer.Close()

	// Rapid produce without delays
	fmt.Printf("   🚀 Rapid producing 10 messages...\n")

	var producedOffsets []int64
	rapidStart := time.Now()

	for i := 0; i < 10; i++ {
		message := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(fmt.Sprintf("rapid-key-%d", i)),
			Value: sarama.StringEncoder(fmt.Sprintf("rapid-value-%d", i)),
		}

		_, offset, err := producer.SendMessage(message)
		if err != nil {
			fmt.Printf("       ❌ Rapid message %d failed: %v\n", i, err)
			continue
		}

		producedOffsets = append(producedOffsets, offset)
		fmt.Printf("       ✅ Rapid message %d: offset=%d\n", i, offset)
	}

	rapidDuration := time.Since(rapidStart)
	fmt.Printf("   📊 Rapid produce completed: %d messages in %v\n", len(producedOffsets), rapidDuration)

	// Immediately try to fetch all produced messages
	fmt.Printf("   🔍 Immediate fetch test...\n")

	consumer, err := sarama.NewConsumer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		fmt.Printf("❌ Failed to create consumer: %v\n", err)
		return
	}
	defer consumer.Close()

	if len(producedOffsets) > 0 {
		startOffset := producedOffsets[0]
		partitionConsumer, err := consumer.ConsumePartition(topic, partition, startOffset)
		if err != nil {
			fmt.Printf("❌ Failed to create partition consumer: %v\n", err)
			return
		}
		defer partitionConsumer.Close()

		fetchedCount := 0
		fetchStart := time.Now()

		for fetchedCount < len(producedOffsets) {
			select {
			case message := <-partitionConsumer.Messages():
				fetchedCount++
				fmt.Printf("       ✅ Fetched[%d]: offset=%d\n", fetchedCount, message.Offset)

			case err := <-partitionConsumer.Errors():
				fmt.Printf("       ❌ Fetch error: %v\n", err)
				break

			case <-time.After(3 * time.Second):
				fmt.Printf("       ❌ Fetch timeout after %d/%d messages\n", fetchedCount, len(producedOffsets))
				break
			}
		}

		fetchDuration := time.Since(fetchStart)
		fmt.Printf("   📊 Immediate fetch result: %d/%d messages in %v\n",
			fetchedCount, len(producedOffsets), fetchDuration)
	}
}

