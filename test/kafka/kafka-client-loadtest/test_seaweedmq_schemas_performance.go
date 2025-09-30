package main

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// Test SeaweedMQ's _schemas topic performance under various load patterns
func main() {
	fmt.Println("⚡ Testing SeaweedMQ _schemas Topic Performance")
	fmt.Println(strings.Repeat("=", 50))

	// Test 1: Basic _schemas topic health
	fmt.Println("\n1️⃣  Testing _schemas Topic Health...")
	testSchemasTopicHealth()

	// Test 2: Single message produce/consume timing
	fmt.Println("\n2️⃣  Testing Single Message Performance...")
	testSingleMessagePerformance()

	// Test 3: Burst message performance
	fmt.Println("\n3️⃣  Testing Burst Message Performance...")
	testBurstMessagePerformance()

	// Test 4: Concurrent producer performance
	fmt.Println("\n4️⃣  Testing Concurrent Producer Performance...")
	testConcurrentProducerPerformance()

	// Test 5: Consumer lag under load
	fmt.Println("\n5️⃣  Testing Consumer Lag Under Load...")
	testConsumerLagUnderLoad()

	// Test 6: Offset ledger consistency
	fmt.Println("\n6️⃣  Testing Offset Ledger Consistency...")
	testOffsetLedgerConsistency()

	fmt.Println("\n🎯 SeaweedMQ _schemas Performance Tests Completed!")
}

func testSchemasTopicHealth() {
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0

	client, err := sarama.NewClient([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		fmt.Printf("❌ Failed to create client: %v\n", err)
		return
	}
	defer client.Close()

	// Check if _schemas topic exists
	topics, err := client.Topics()
	if err != nil {
		fmt.Printf("❌ Failed to get topics: %v\n", err)
		return
	}

	schemasExists := false
	for _, topic := range topics {
		if topic == "_schemas" {
			schemasExists = true
			break
		}
	}

	if !schemasExists {
		fmt.Printf("❌ _schemas topic does not exist\n")
		return
	}

	fmt.Printf("✅ _schemas topic exists\n")

	// Get partition information
	partitions, err := client.Partitions("_schemas")
	if err != nil {
		fmt.Printf("❌ Failed to get partitions: %v\n", err)
		return
	}

	fmt.Printf("✅ _schemas has %d partitions: %v\n", len(partitions), partitions)

	// Get offset information for each partition
	for _, partition := range partitions {
		oldest, err := client.GetOffset("_schemas", partition, sarama.OffsetOldest)
		if err != nil {
			fmt.Printf("❌ Failed to get oldest offset for partition %d: %v\n", partition, err)
			continue
		}

		newest, err := client.GetOffset("_schemas", partition, sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("❌ Failed to get newest offset for partition %d: %v\n", partition, err)
			continue
		}

		fmt.Printf("✅ Partition %d: oldest=%d, newest=%d, messages=%d\n",
			partition, oldest, newest, newest-oldest)
	}
}

func testSingleMessagePerformance() {
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Timeout = 10 * time.Second

	producer, err := sarama.NewSyncProducer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		fmt.Printf("❌ Failed to create producer: %v\n", err)
		return
	}
	defer producer.Close()

	// Test individual message timing
	timings := make([]time.Duration, 0, 10)

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("single-test-key-%d", i)
		value := fmt.Sprintf(`{"test": "single-message-%d", "timestamp": %d}`, i, time.Now().UnixNano())

		message := &sarama.ProducerMessage{
			Topic: "_schemas",
			Key:   sarama.StringEncoder(key),
			Value: sarama.StringEncoder(value),
		}

		start := time.Now()
		partition, offset, err := producer.SendMessage(message)
		duration := time.Since(start)

		timings = append(timings, duration)

		if err != nil {
			fmt.Printf("   ❌ Message %d failed: %v\n", i+1, err)
		} else {
			fmt.Printf("   ✅ Message %d: partition=%d, offset=%d, duration=%v\n",
				i+1, partition, offset, duration)
		}

		time.Sleep(100 * time.Millisecond)
	}

	// Calculate statistics
	var total time.Duration
	var min, max time.Duration = timings[0], timings[0]

	for _, t := range timings {
		total += t
		if t < min {
			min = t
		}
		if t > max {
			max = t
		}
	}

	avg := total / time.Duration(len(timings))
	fmt.Printf("   📊 Single Message Stats: Avg=%v, Min=%v, Max=%v\n", avg, min, max)
}

func testBurstMessagePerformance() {
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Timeout = 30 * time.Second
	config.Producer.Flush.Messages = 10 // Batch messages

	producer, err := sarama.NewSyncProducer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		fmt.Printf("❌ Failed to create producer: %v\n", err)
		return
	}
	defer producer.Close()

	// Send burst of messages
	burstSize := 20
	fmt.Printf("   Sending burst of %d messages...\n", burstSize)

	start := time.Now()
	successCount := 0
	failCount := 0

	for i := 0; i < burstSize; i++ {
		key := fmt.Sprintf("burst-test-key-%d", i)
		value := fmt.Sprintf(`{"test": "burst-message-%d", "timestamp": %d}`, i, time.Now().UnixNano())

		message := &sarama.ProducerMessage{
			Topic: "_schemas",
			Key:   sarama.StringEncoder(key),
			Value: sarama.StringEncoder(value),
		}

		msgStart := time.Now()
		partition, offset, err := producer.SendMessage(message)
		msgDuration := time.Since(msgStart)

		if err != nil {
			fmt.Printf("   ❌ Message %d failed: %v (%v)\n", i+1, err, msgDuration)
			failCount++
		} else {
			fmt.Printf("   ✅ Message %d: partition=%d, offset=%d (%v)\n",
				i+1, partition, offset, msgDuration)
			successCount++
		}
	}

	totalDuration := time.Since(start)
	throughput := float64(successCount) / totalDuration.Seconds()

	fmt.Printf("   📊 Burst Results: %d success, %d failed, Total: %v, Throughput: %.1f msg/sec\n",
		successCount, failCount, totalDuration, throughput)
}

func testConcurrentProducerPerformance() {
	producerCount := 5
	messagesPerProducer := 5

	fmt.Printf("   Starting %d concurrent producers, %d messages each...\n",
		producerCount, messagesPerProducer)

	var wg sync.WaitGroup
	var mu sync.Mutex
	results := make([]time.Duration, 0)
	successCount := 0
	failCount := 0

	start := time.Now()

	for p := 0; p < producerCount; p++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()

			config := sarama.NewConfig()
			config.Version = sarama.V2_6_0_0
			config.Producer.Return.Successes = true
			config.Producer.RequiredAcks = sarama.WaitForAll
			config.Producer.Timeout = 30 * time.Second

			producer, err := sarama.NewSyncProducer([]string{"kafka-gateway:9093"}, config)
			if err != nil {
				mu.Lock()
				failCount += messagesPerProducer
				mu.Unlock()
				fmt.Printf("   ❌ Producer %d failed to start: %v\n", producerID, err)
				return
			}
			defer producer.Close()

			for m := 0; m < messagesPerProducer; m++ {
				key := fmt.Sprintf("concurrent-p%d-m%d", producerID, m)
				value := fmt.Sprintf(`{"producer": %d, "message": %d, "timestamp": %d}`,
					producerID, m, time.Now().UnixNano())

				message := &sarama.ProducerMessage{
					Topic: "_schemas",
					Key:   sarama.StringEncoder(key),
					Value: sarama.StringEncoder(value),
				}

				msgStart := time.Now()
				partition, offset, err := producer.SendMessage(message)
				msgDuration := time.Since(msgStart)

				mu.Lock()
				results = append(results, msgDuration)
				if err != nil {
					failCount++
					fmt.Printf("   ❌ P%d-M%d failed: %v (%v)\n", producerID, m, err, msgDuration)
				} else {
					successCount++
					fmt.Printf("   ✅ P%d-M%d: partition=%d, offset=%d (%v)\n",
						producerID, m, partition, offset, msgDuration)
				}
				mu.Unlock()
			}
		}(p)
	}

	wg.Wait()
	totalDuration := time.Since(start)

	// Calculate statistics
	if len(results) > 0 {
		var total time.Duration
		var min, max time.Duration = results[0], results[0]

		for _, t := range results {
			total += t
			if t < min {
				min = t
			}
			if t > max {
				max = t
			}
		}

		avg := total / time.Duration(len(results))
		throughput := float64(successCount) / totalDuration.Seconds()

		fmt.Printf("   📊 Concurrent Results: %d success, %d failed, Total: %v\n",
			successCount, failCount, totalDuration)
		fmt.Printf("       Avg Latency: %v, Min: %v, Max: %v, Throughput: %.1f msg/sec\n",
			avg, min, max, throughput)
	}
}

func testConsumerLagUnderLoad() {
	// Start a consumer to monitor lag
	go func() {
		config := sarama.NewConfig()
		config.Version = sarama.V2_6_0_0
		config.Consumer.Return.Errors = true

		consumer, err := sarama.NewConsumer([]string{"kafka-gateway:9093"}, config)
		if err != nil {
			log.Printf("Failed to create consumer: %v", err)
			return
		}
		defer consumer.Close()

		// Create a client to get offset information
		config2 := sarama.NewConfig()
		config2.Version = sarama.V2_6_0_0
		client, err := sarama.NewClient([]string{"kafka-gateway:9093"}, config2)
		if err != nil {
			log.Printf("Failed to create client: %v", err)
			return
		}
		defer client.Close()

		// Get starting offset
		startOffset, err := client.GetOffset("_schemas", 0, sarama.OffsetNewest)
		if err != nil {
			log.Printf("Failed to get starting offset: %v", err)
			return
		}

		fmt.Printf("   📊 Starting consumer from offset %d\n", startOffset)

		partitionConsumer, err := consumer.ConsumePartition("_schemas", 0, startOffset)
		if err != nil {
			log.Printf("Failed to create partition consumer: %v", err)
			return
		}
		defer partitionConsumer.Close()

		messageCount := 0
		lastOffset := startOffset

		for {
			select {
			case message := <-partitionConsumer.Messages():
				messageCount++
				lastOffset = message.Offset

				// Check current lag
				newest, _ := client.GetOffset("_schemas", 0, sarama.OffsetNewest)
				lag := newest - message.Offset - 1

				fmt.Printf("   📨 Consumed[%d]: offset=%d, lag=%d, key=%s\n",
					messageCount, message.Offset, lag, string(message.Key))

			case err := <-partitionConsumer.Errors():
				fmt.Printf("   ❌ Consumer error: %v\n", err)
				return

			case <-time.After(15 * time.Second):
				newest, _ := client.GetOffset("_schemas", 0, sarama.OffsetNewest)
				finalLag := newest - lastOffset - 1
				fmt.Printf("   ⏰ Consumer finished: consumed=%d messages, final_lag=%d\n",
					messageCount, finalLag)
				return
			}
		}
	}()

	// Give consumer time to start
	time.Sleep(1 * time.Second)

	// Now produce messages while monitoring consumer lag
	fmt.Printf("   Producing messages while monitoring consumer lag...\n")

	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Timeout = 30 * time.Second

	producer, err := sarama.NewSyncProducer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		fmt.Printf("❌ Failed to create producer: %v\n", err)
		return
	}
	defer producer.Close()

	// Produce messages with varying intervals
	intervals := []time.Duration{
		100 * time.Millisecond,
		50 * time.Millisecond,
		200 * time.Millisecond,
		10 * time.Millisecond,
		500 * time.Millisecond,
	}

	for i, interval := range intervals {
		key := fmt.Sprintf("lag-test-key-%d", i)
		value := fmt.Sprintf(`{"test": "lag-message-%d", "interval": "%v"}`, i, interval)

		message := &sarama.ProducerMessage{
			Topic: "_schemas",
			Key:   sarama.StringEncoder(key),
			Value: sarama.StringEncoder(value),
		}

		start := time.Now()
		partition, offset, err := producer.SendMessage(message)
		duration := time.Since(start)

		if err != nil {
			fmt.Printf("   ❌ Lag test message %d failed: %v\n", i+1, err)
		} else {
			fmt.Printf("   ✅ Lag test message %d: partition=%d, offset=%d (%v)\n",
				i+1, partition, offset, duration)
		}

		time.Sleep(interval)
	}

	// Wait for consumer to finish
	time.Sleep(10 * time.Second)
}

func testOffsetLedgerConsistency() {
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0

	client, err := sarama.NewClient([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		fmt.Printf("❌ Failed to create client: %v\n", err)
		return
	}
	defer client.Close()

	fmt.Printf("   Testing offset ledger consistency...\n")

	// Get initial state
	oldest, err := client.GetOffset("_schemas", 0, sarama.OffsetOldest)
	if err != nil {
		fmt.Printf("❌ Failed to get oldest offset: %v\n", err)
		return
	}

	newest, err := client.GetOffset("_schemas", 0, sarama.OffsetNewest)
	if err != nil {
		fmt.Printf("❌ Failed to get newest offset: %v\n", err)
		return
	}

	fmt.Printf("   📊 Initial state: oldest=%d, newest=%d, available=%d\n",
		oldest, newest, newest-oldest)

	// Produce a few messages and check consistency
	producer, err := sarama.NewSyncProducer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		fmt.Printf("❌ Failed to create producer: %v\n", err)
		return
	}
	defer producer.Close()

	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("consistency-test-key-%d", i)
		value := fmt.Sprintf(`{"test": "consistency-message-%d"}`, i)

		message := &sarama.ProducerMessage{
			Topic: "_schemas",
			Key:   sarama.StringEncoder(key),
			Value: sarama.StringEncoder(value),
		}

		partition, offset, err := producer.SendMessage(message)
		if err != nil {
			fmt.Printf("   ❌ Message %d failed: %v\n", i+1, err)
			continue
		}

		// Check offset consistency immediately after produce
		newNewest, err := client.GetOffset("_schemas", 0, sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("   ❌ Failed to get newest offset after message %d: %v\n", i+1, err)
			continue
		}

		expectedOffset := newest + int64(i) + 1
		fmt.Printf("   ✅ Message %d: partition=%d, offset=%d, newest=%d, expected=%d, consistent=%v\n",
			i+1, partition, offset, newNewest, expectedOffset, newNewest == expectedOffset)
	}

	// Final consistency check
	finalOldest, _ := client.GetOffset("_schemas", 0, sarama.OffsetOldest)
	finalNewest, _ := client.GetOffset("_schemas", 0, sarama.OffsetNewest)

	fmt.Printf("   📊 Final state: oldest=%d, newest=%d, available=%d, added=%d\n",
		finalOldest, finalNewest, finalNewest-finalOldest, finalNewest-newest)
}
