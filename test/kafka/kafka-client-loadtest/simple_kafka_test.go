package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	// Test basic Kafka Gateway functionality without schema enforcement
	fmt.Println("🧪 Testing Kafka Gateway basic functionality...")

	brokers := []string{"localhost:9093"}
	topic := "basic-test-topic"

	// Test 1: Create topic by producing a message
	fmt.Println("📤 Testing message production...")

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  brokers,
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
	defer writer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte("test-key-1"),
		Value: []byte("Hello from basic Kafka Gateway test!"),
	})

	if err != nil {
		log.Printf("❌ Failed to produce message: %v", err)
		return
	}
	fmt.Println("✅ Message produced successfully!")

	// Test 2: List topics to verify topic creation
	fmt.Println("📋 Checking if topic was created...")

	conn, err := kafka.Dial("tcp", "localhost:9093")
	if err != nil {
		log.Printf("❌ Failed to connect: %v", err)
		return
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		log.Printf("❌ Failed to read partitions: %v", err)
		return
	}

	topicFound := false
	for _, p := range partitions {
		if p.Topic == topic {
			topicFound = true
			fmt.Printf("✅ Topic '%s' found with partition %d\n", p.Topic, p.ID)
			break
		}
	}

	if !topicFound {
		fmt.Printf("⚠️ Topic '%s' not found in partition list\n", topic)
	}

	// Test 3: Consume the message
	fmt.Println("📥 Testing message consumption...")

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: "basic-test-group",
	})
	defer reader.Close()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()

	message, err := reader.ReadMessage(ctx2)
	if err != nil {
		log.Printf("❌ Failed to consume message: %v", err)
	} else {
		fmt.Printf("✅ Message consumed: key=%s, value=%s\n",
			string(message.Key), string(message.Value))
	}

	fmt.Println("🎉 Basic Kafka Gateway test completed!")
}
