package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("🧪 Simple Timestamp Fix Verification")
	fmt.Println("Testing timestamp fix on a regular topic without Schema Registry interference")

	// Use a simple topic name
	topicName := "timestamp-test"

	// Step 1: Produce a message
	fmt.Println("\n📝 Step 1: Producing test message")

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Version = sarama.V2_8_0_0
	config.Net.DialTimeout = 10 * time.Second
	config.Net.ReadTimeout = 10 * time.Second
	config.Net.WriteTimeout = 10 * time.Second

	producer, err := sarama.NewSyncProducer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Fatalf("❌ Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Create test message with current timestamp in the value
	currentTime := time.Now()
	testValue := fmt.Sprintf(`{"message": "timestamp test", "produced_at": "%s", "unix_ms": %d}`,
		currentTime.Format(time.RFC3339), currentTime.UnixMilli())

	msg := &sarama.ProducerMessage{
		Topic: topicName,
		Key:   sarama.StringEncoder("timestamp-key"),
		Value: sarama.StringEncoder(testValue),
	}

	fmt.Printf("🚀 Producing message to topic: %s\n", topicName)
	fmt.Printf("📋 Message content: %s\n", testValue)

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Fatalf("❌ Failed to produce message: %v", err)
	}

	fmt.Printf("✅ Message produced successfully!\n")
	fmt.Printf("   Partition: %d, Offset: %d\n", partition, offset)

	// Step 2: Wait a moment for the message to be stored
	fmt.Println("\n⏳ Waiting for message to be stored...")
	time.Sleep(2 * time.Second)

	// Step 3: Consume the message back
	fmt.Println("\n📖 Step 2: Consuming the message back")

	consumerConfig := sarama.NewConfig()
	consumerConfig.Version = sarama.V2_8_0_0
	consumerConfig.Consumer.Return.Errors = true
	consumerConfig.Net.DialTimeout = 10 * time.Second
	consumerConfig.Net.ReadTimeout = 10 * time.Second

	consumer, err := sarama.NewConsumer([]string{"kafka-gateway:9093"}, consumerConfig)
	if err != nil {
		log.Fatalf("❌ Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topicName, partition, offset)
	if err != nil {
		log.Fatalf("❌ Failed to create partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	fmt.Printf("🔍 Consuming from partition %d, offset %d...\n", partition, offset)

	select {
	case message := <-partitionConsumer.Messages():
		fmt.Printf("\n🎯 MESSAGE CONSUMED SUCCESSFULLY!\n")
		fmt.Printf("   Partition: %d, Offset: %d\n", message.Partition, message.Offset)

		// The critical test: verify the timestamp
		consumedTimestamp := message.Timestamp
		fmt.Printf("\n📊 TIMESTAMP ANALYSIS:\n")
		fmt.Printf("   Consumed timestamp: %s\n", consumedTimestamp.Format(time.RFC3339))
		fmt.Printf("   Consumed Unix timestamp: %d\n", consumedTimestamp.Unix())
		fmt.Printf("   Consumed Unix millis: %d\n", consumedTimestamp.UnixMilli())

		fmt.Printf("\n📊 COMPARISON:\n")
		fmt.Printf("   Produced at: %s\n", currentTime.Format(time.RFC3339))
		fmt.Printf("   Consumed at: %s\n", consumedTimestamp.Format(time.RFC3339))

		// Calculate time difference
		timeDiff := consumedTimestamp.Sub(currentTime)
		if timeDiff < 0 {
			timeDiff = -timeDiff
		}

		fmt.Printf("   Time difference: %v\n", timeDiff)

		// Verify timestamp is reasonable (within 1 hour of current time)
		now := time.Now()
		oneHourAgo := now.Add(-1 * time.Hour)
		oneHourFromNow := now.Add(1 * time.Hour)

		if consumedTimestamp.After(oneHourAgo) && consumedTimestamp.Before(oneHourFromNow) {
			fmt.Printf("\n🎉 SUCCESS! TIMESTAMP FIX IS WORKING!\n")
			fmt.Printf("✅ Timestamp is within reasonable range\n")
			fmt.Printf("✅ Nanoseconds → Milliseconds conversion successful\n")

			if timeDiff < 10*time.Second {
				fmt.Printf("⭐ EXCELLENT! Timestamp is very accurate (within 10 seconds)\n")
			} else if timeDiff < 1*time.Minute {
				fmt.Printf("✅ GOOD! Timestamp is accurate (within 1 minute)\n")
			} else {
				fmt.Printf("✅ OK! Timestamp is reasonable (within 1 hour)\n")
			}
		} else {
			fmt.Printf("\n❌ TIMESTAMP FIX FAILED!\n")
			fmt.Printf("❌ Timestamp is outside reasonable range\n")
			fmt.Printf("   Expected between: %s and %s\n",
				oneHourAgo.Format(time.RFC3339), oneHourFromNow.Format(time.RFC3339))

			// Check if it's the old bug (year 55741184)
			if consumedTimestamp.Year() > 10000 {
				fmt.Printf("❌ This looks like the old nanosecond bug (year %d)\n", consumedTimestamp.Year())
			}
		}

		// Verify message content
		fmt.Printf("\n📋 MESSAGE CONTENT:\n")
		fmt.Printf("   Key: %q\n", string(message.Key))
		fmt.Printf("   Value: %q\n", string(message.Value))

		if string(message.Key) == "timestamp-key" {
			fmt.Printf("✅ Key matches\n")
		} else {
			fmt.Printf("❌ Key mismatch\n")
		}

		if string(message.Value) == testValue {
			fmt.Printf("✅ Value matches\n")
		} else {
			fmt.Printf("❌ Value mismatch\n")
		}

	case err := <-partitionConsumer.Errors():
		log.Printf("❌ Consumer error: %v", err)

	case <-ctx.Done():
		fmt.Printf("❌ TIMEOUT - Message was not consumed within 15 seconds\n")
		fmt.Printf("   This could indicate:\n")
		fmt.Printf("   - Message was not stored properly\n")
		fmt.Printf("   - Offset ledger issue\n")
		fmt.Printf("   - Fetch path problem\n")
	}

	fmt.Println("\n✅ Test completed!")
}

