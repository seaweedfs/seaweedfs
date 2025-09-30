package main

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("=== Testing Kafka Gateway from Inside Docker Network ===")

	// Use the internal Docker hostname
	brokers := []string{"kafka-gateway:9093"}

	// Test produce with same settings as Schema Registry
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll // acks=-1 like Schema Registry
	config.Producer.Timeout = 500 * time.Millisecond // Same timeout as Schema Registry

	// Enable debug logging to see what's happening
	sarama.Logger = log.New(log.Writer(), "[SARAMA] ", log.LstdFlags)

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Test 1: Regular message to _schemas topic
	fmt.Println("\n=== Test 1: Regular Message to _schemas ===")
	message := &sarama.ProducerMessage{
		Topic: "_schemas",
		Key:   sarama.StringEncoder("test-docker-key"),
		Value: sarama.StringEncoder("test-docker-value"),
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
		Key:   sarama.StringEncoder("noop-docker-key"),
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

	// Test 3: Rapid succession (like Schema Registry bulk operations)
	fmt.Println("\n=== Test 3: Rapid Succession Messages ===")
	for i := 0; i < 3; i++ {
		testMsg := &sarama.ProducerMessage{
			Topic: "_schemas",
			Key:   sarama.StringEncoder(fmt.Sprintf("rapid-key-%d", i)),
			Value: sarama.StringEncoder(fmt.Sprintf("rapid-value-%d", i)),
		}

		start = time.Now()
		partition, offset, err = producer.SendMessage(testMsg)
		duration = time.Since(start)

		if err != nil {
			log.Printf("❌ Rapid message %d failed: %v (duration: %v)", i, err, duration)
			if duration >= 500*time.Millisecond {
				fmt.Printf("🔥 TIMEOUT: Message %d equals/exceeds Schema Registry timeout!\n", i)
			}
		} else {
			fmt.Printf("✅ Rapid message %d: partition=%d, offset=%d, duration=%v\n", i, partition, offset, duration)
			if duration > 400*time.Millisecond {
				fmt.Printf("⚠️  Rapid message %d close to Schema Registry timeout!\n", i)
			}
		}
	}

	fmt.Println("\n=== Summary ===")
	fmt.Println("This test simulates exactly what Schema Registry does:")
	fmt.Println("- Uses acks=-1 (WaitForAll)")
	fmt.Println("- Uses 500ms timeout")
	fmt.Println("- Sends to _schemas topic")
	fmt.Println("- Tests null values (Noop records)")
	fmt.Println("")
	fmt.Println("If all tests pass with duration < 500ms, our Kafka Gateway")
	fmt.Println("should work with Schema Registry!")
}

