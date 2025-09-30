package main

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("=== Testing Produce Response Timing on _schemas Topic ===")

	// Test produce with same settings as Schema Registry
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll // acks=-1 like Schema Registry
	config.Producer.Timeout = 500 * time.Millisecond // Same timeout as Schema Registry

	producer, err := sarama.NewSyncProducer([]string{"localhost:9093"}, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Test 1: Regular message to _schemas topic
	fmt.Println("\n=== Test 1: Regular Message to _schemas ===")
	message := &sarama.ProducerMessage{
		Topic: "_schemas",
		Key:   sarama.StringEncoder("test-key"),
		Value: sarama.StringEncoder("test-value"),
	}

	start := time.Now()
	partition, offset, err := producer.SendMessage(message)
	duration := time.Since(start)

	if err != nil {
		log.Printf("❌ Failed to send regular message: %v", err)
		log.Printf("Duration: %v", duration)

		if duration >= 500*time.Millisecond {
			fmt.Printf("🔥 TIMEOUT: Duration equals/exceeds Schema Registry timeout!\n")
		}
	} else {
		fmt.Printf("✅ Regular message sent successfully!\n")
		fmt.Printf("Partition: %d, Offset: %d\n", partition, offset)
		fmt.Printf("Duration: %v\n", duration)

		if duration > 400*time.Millisecond {
			fmt.Printf("⚠️  WARNING: Duration close to Schema Registry timeout!\n")
		}
	}

	// Test 2: Null value message (like Schema Registry Noop)
	fmt.Println("\n=== Test 2: Null Value Message (Schema Registry Noop style) ===")
	nullMessage := &sarama.ProducerMessage{
		Topic: "_schemas",
		Key:   sarama.StringEncoder("noop-test-key"),
		Value: nil, // null value like Schema Registry Noop records
	}

	start = time.Now()
	partition, offset, err = producer.SendMessage(nullMessage)
	duration = time.Since(start)

	if err != nil {
		log.Printf("❌ Failed to send null message: %v", err)
		log.Printf("Duration: %v", duration)

		if duration >= 500*time.Millisecond {
			fmt.Printf("🔥 TIMEOUT: Duration equals/exceeds Schema Registry timeout!\n")
		}
	} else {
		fmt.Printf("✅ Null message sent successfully!\n")
		fmt.Printf("Partition: %d, Offset: %d\n", partition, offset)
		fmt.Printf("Duration: %v\n", duration)

		if duration > 400*time.Millisecond {
			fmt.Printf("⚠️  WARNING: Duration close to Schema Registry timeout!\n")
		}
	}

	// Test 3: Test with exact Schema Registry serialization format
	fmt.Println("\n=== Test 3: Schema Registry-like Serialized Key ===")
	// This simulates what Schema Registry actually sends
	schemaRegistryMessage := &sarama.ProducerMessage{
		Topic: "_schemas",
		Key:   sarama.StringEncoder(`{"keytype":"NOOP","magic":0}`), // Schema Registry Noop key format
		Value: nil,                                                  // null value
	}

	start = time.Now()
	partition, offset, err = producer.SendMessage(schemaRegistryMessage)
	duration = time.Since(start)

	if err != nil {
		log.Printf("❌ Failed to send Schema Registry-like message: %v", err)
		log.Printf("Duration: %v", duration)

		if duration >= 500*time.Millisecond {
			fmt.Printf("🔥 TIMEOUT: Duration equals/exceeds Schema Registry timeout!\n")
		}
	} else {
		fmt.Printf("✅ Schema Registry-like message sent successfully!\n")
		fmt.Printf("Partition: %d, Offset: %d\n", partition, offset)
		fmt.Printf("Duration: %v\n", duration)

		if duration > 400*time.Millisecond {
			fmt.Printf("⚠️  WARNING: Duration close to Schema Registry timeout!\n")
		}
	}

	fmt.Println("\n=== Summary ===")
	fmt.Println("If all messages succeed with duration < 500ms, then our Kafka Gateway")
	fmt.Println("response format and timing should work with Schema Registry.")
	fmt.Println("If any message times out or takes > 500ms, that's the issue.")
}

