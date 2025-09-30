package main

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("=== Testing Kafka Gateway Produce Response Format ===")

	// Create a simple producer to send a message and capture the response
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll // This is acks=-1
	config.Producer.Timeout = 1 * time.Second        // Give it more time than Schema Registry's 500ms
	// Use default version which should work with our gateway

	// Enable debug logging
	sarama.Logger = log.New(log.Writer(), "[SARAMA] ", log.LstdFlags)

	producer, err := sarama.NewSyncProducer([]string{"localhost:9093"}, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Send a simple message to test the response format
	message := &sarama.ProducerMessage{
		Topic: "test-response-format",
		Key:   sarama.StringEncoder("test-key"),
		Value: sarama.StringEncoder("test-value"),
	}

	fmt.Println("Sending test message...")
	start := time.Now()
	partition, offset, err := producer.SendMessage(message)
	duration := time.Since(start)

	if err != nil {
		log.Printf("Failed to send message: %v", err)
		log.Printf("Duration: %v", duration)

		// Try to get more details about the error
		if netErr, ok := err.(net.Error); ok {
			log.Printf("Network error - Timeout: %v, Temporary: %v", netErr.Timeout(), netErr.Temporary())
		}
	} else {
		fmt.Printf("Message sent successfully!\n")
		fmt.Printf("Partition: %d, Offset: %d\n", partition, offset)
		fmt.Printf("Duration: %v\n", duration)
	}

	// Test with a null value message (like Schema Registry Noop records)
	fmt.Println("\nTesting null value message (like Schema Registry Noop)...")
	nullMessage := &sarama.ProducerMessage{
		Topic: "test-response-format",
		Key:   sarama.StringEncoder("noop-key"),
		Value: nil, // null value
	}

	start = time.Now()
	partition, offset, err = producer.SendMessage(nullMessage)
	duration = time.Since(start)

	if err != nil {
		log.Printf("Failed to send null message: %v", err)
		log.Printf("Duration: %v", duration)
	} else {
		fmt.Printf("Null message sent successfully!\n")
		fmt.Printf("Partition: %d, Offset: %d\n", partition, offset)
		fmt.Printf("Duration: %v\n", duration)
	}
}
