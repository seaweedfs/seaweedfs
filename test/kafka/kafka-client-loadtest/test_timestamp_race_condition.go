package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("🧪 Timestamp Fix Verification via Race Condition Test")
	fmt.Println("Testing: Produce to _schemas and immediately consume before Schema Registry gets it")

	// Strategy: Start consumer first, then produce, to try to beat Schema Registry

	var wg sync.WaitGroup
	var consumedMessage *sarama.ConsumerMessage
	var consumerError error

	// Step 1: Start consumer in background
	wg.Add(1)
	go func() {
		defer wg.Done()
		consumedMessage, consumerError = startConsumerAndWait()
	}()

	// Step 2: Wait a moment for consumer to be ready
	time.Sleep(1 * time.Second)

	// Step 3: Produce message
	fmt.Println("\n📝 Producing test message to _schemas topic")

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Version = sarama.V2_8_0_0

	producer, err := sarama.NewSyncProducer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Fatalf("❌ Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Create unique test message
	currentTime := time.Now()
	uniqueID := currentTime.UnixNano()
	testValue := fmt.Sprintf(`{"test_id": %d, "message": "timestamp race test", "produced_at": "%s"}`,
		uniqueID, currentTime.Format(time.RFC3339))

	msg := &sarama.ProducerMessage{
		Topic: "_schemas",
		Key:   sarama.StringEncoder(fmt.Sprintf("race-test-%d", uniqueID)),
		Value: sarama.StringEncoder(testValue),
	}

	fmt.Printf("🚀 Producing message with ID: %d\n", uniqueID)

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Fatalf("❌ Failed to produce message: %v", err)
	}

	fmt.Printf("✅ Message produced: partition=%d, offset=%d\n", partition, offset)

	// Step 4: Wait for consumer to finish
	fmt.Println("\n⏳ Waiting for consumer to receive message...")
	wg.Wait()

	// Step 5: Analyze results
	if consumerError != nil {
		fmt.Printf("❌ Consumer error: %v\n", consumerError)
		return
	}

	if consumedMessage == nil {
		fmt.Printf("❌ No message was consumed (timeout)\n")
		return
	}

	fmt.Printf("\n🎯 MESSAGE ANALYSIS:\n")
	fmt.Printf("   Consumed offset: %d\n", consumedMessage.Offset)
	fmt.Printf("   Consumed key: %q\n", string(consumedMessage.Key))
	fmt.Printf("   Consumed value: %q\n", string(consumedMessage.Value))

	// Check if this is our test message or a Schema Registry message
	messageKey := string(consumedMessage.Key)
	messageValue := string(consumedMessage.Value)

	if fmt.Sprintf("race-test-%d", uniqueID) == messageKey {
		fmt.Printf("🎉 SUCCESS! We caught our own message before Schema Registry!\n")

		// Now test the timestamp
		consumedTimestamp := consumedMessage.Timestamp
		fmt.Printf("\n📊 TIMESTAMP VERIFICATION:\n")
		fmt.Printf("   Produced at: %s\n", currentTime.Format(time.RFC3339))
		fmt.Printf("   Consumed timestamp: %s\n", consumedTimestamp.Format(time.RFC3339))
		fmt.Printf("   Unix timestamp: %d\n", consumedTimestamp.Unix())

		// Calculate time difference
		timeDiff := consumedTimestamp.Sub(currentTime)
		if timeDiff < 0 {
			timeDiff = -timeDiff
		}

		fmt.Printf("   Time difference: %v\n", timeDiff)

		// Verify timestamp is reasonable
		if timeDiff < 1*time.Hour {
			fmt.Printf("🎉 TIMESTAMP FIX VERIFIED! ✅\n")
			fmt.Printf("✅ Timestamp is within reasonable range\n")
			fmt.Printf("✅ Nanoseconds → Milliseconds conversion working\n")
		} else {
			fmt.Printf("❌ Timestamp fix failed - time difference too large\n")
			if consumedTimestamp.Year() > 10000 {
				fmt.Printf("❌ This is the old nanosecond bug (year %d)\n", consumedTimestamp.Year())
			}
		}

	} else if messageKey == `{"keytype":"NOOP","magic":0}` || messageKey == `{"keytype":"SCHEMA","magic":0}` {
		fmt.Printf("❌ Schema Registry consumed our message first\n")
		fmt.Printf("   This is expected behavior - Schema Registry is very fast\n")
		fmt.Printf("   But it confirms the produce/consume cycle is working\n")

		// Even though it's a Schema Registry message, we can still check the timestamp
		consumedTimestamp := consumedMessage.Timestamp
		fmt.Printf("\n📊 TIMESTAMP CHECK (Schema Registry message):\n")
		fmt.Printf("   Timestamp: %s\n", consumedTimestamp.Format(time.RFC3339))
		fmt.Printf("   Unix timestamp: %d\n", consumedTimestamp.Unix())

		if consumedTimestamp.Year() > 10000 {
			fmt.Printf("❌ Old timestamp bug still present (year %d)\n", consumedTimestamp.Year())
		} else {
			fmt.Printf("✅ Timestamp appears reasonable\n")
		}

	} else {
		fmt.Printf("❓ Unknown message type\n")
		fmt.Printf("   Key: %q\n", messageKey)
		fmt.Printf("   Value preview: %q\n", messageValue[:min(100, len(messageValue))])
	}
}

func startConsumerAndWait() (*sarama.ConsumerMessage, error) {
	// Get current high water mark first
	client, err := sarama.NewClient([]string{"kafka-gateway:9093"}, sarama.NewConfig())
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %v", err)
	}
	defer client.Close()

	// Get the current newest offset to start consuming from there
	newestOffset, err := client.GetOffset("_schemas", 0, sarama.OffsetNewest)
	if err != nil {
		return nil, fmt.Errorf("failed to get newest offset: %v", err)
	}

	fmt.Printf("📋 Starting consumer from offset: %d\n", newestOffset)

	// Create consumer
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Start consuming from the newest offset
	partitionConsumer, err := consumer.ConsumePartition("_schemas", 0, newestOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to create partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	fmt.Printf("✅ Consumer ready, waiting for messages...\n")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	select {
	case message := <-partitionConsumer.Messages():
		return message, nil

	case err := <-partitionConsumer.Errors():
		return nil, fmt.Errorf("consumer error: %v", err)

	case <-ctx.Done():
		return nil, fmt.Errorf("timeout waiting for message")
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

