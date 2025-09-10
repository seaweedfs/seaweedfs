package kafka

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
	"github.com/segmentio/kafka-go"
)

// TestKafkaGoClient_DirectProduceConsume bypasses CreateTopics and tests produce/consume directly
func TestKafkaGoClient_DirectProduceConsume(t *testing.T) {
	// Start the gateway server
	srv := gateway.NewServer(gateway.Options{
		Listen:       ":0",
		UseSeaweedMQ: false,
	})

	if err := srv.Start(); err != nil {
		t.Fatalf("Failed to start gateway: %v", err)
	}
	defer srv.Close()

	brokerAddr := srv.Addr()
	t.Logf("Gateway running on %s", brokerAddr)

	topicName := "direct-test-topic"

	// Pre-create the topic by making a direct call to our gateway's topic registry
	// This simulates topic already existing (like pre-created topics)
	if err := createTopicDirectly(srv, topicName); err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Test basic produce without kafka-go's CreateTopics API
	messages := []kafka.Message{
		{
			Key:   []byte("test-key-1"),
			Value: []byte("Hello from direct produce test!"),
		},
		{
			Key:   []byte("test-key-2"),
			Value: []byte("Second test message"),
		},
	}

	t.Logf("Testing direct produce to topic %s", topicName)

	// Produce messages
	if err := produceMessagesDirect(brokerAddr, topicName, messages); err != nil {
		t.Fatalf("Failed to produce messages: %v", err)
	}

	t.Logf("Successfully produced %d messages", len(messages))

	// Consume messages
	t.Logf("Testing direct consume from topic %s", topicName)

	consumedMessages, err := consumeMessagesDirect(brokerAddr, topicName, len(messages))
	if err != nil {
		t.Fatalf("Failed to consume messages: %v", err)
	}

	// Validate consumed messages
	if len(consumedMessages) != len(messages) {
		t.Errorf("Expected %d messages, got %d", len(messages), len(consumedMessages))
	}

	for i, msg := range consumedMessages {
		if i < len(messages) {
			expectedValue := string(messages[i].Value)
			actualValue := string(msg.Value)
			t.Logf("Message %d: key=%s, value=%s", i, string(msg.Key), actualValue)

			if actualValue != expectedValue {
				t.Errorf("Message %d: expected value %q, got %q", i, expectedValue, actualValue)
			}
		}
	}

	t.Logf("✅ Direct produce/consume test PASSED with %d messages", len(consumedMessages))
}

// createTopicDirectly creates a topic by directly adding it to the handler's registry
func createTopicDirectly(srv interface{}, topicName string) error {
	gatewayServer, ok := srv.(*gateway.Server)
	if !ok {
		return fmt.Errorf("invalid server type")
	}

	// Get the handler and directly add the topic
	handler := gatewayServer.GetHandler()
	if handler == nil {
		return fmt.Errorf("handler is nil")
	}

	// Add the topic with 1 partition
	handler.AddTopicForTesting(topicName, 1)

	fmt.Printf("DEBUG: Topic %s created directly in handler registry\n", topicName)
	return nil
}

func produceMessagesDirect(brokerAddr, topicName string, messages []kafka.Message) error {
	// Use kafka-go Writer which should use Produce API directly
	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokerAddr),
		Topic:    topicName,
		Balancer: &kafka.LeastBytes{},

		// Increase timeouts to see if kafka-go eventually makes other requests
		WriteTimeout: 20 * time.Second,
		ReadTimeout:  20 * time.Second,

		// Enable detailed logging
		Logger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			if strings.Contains(msg, "produce") || strings.Contains(msg, "Produce") {
				fmt.Printf("PRODUCER: "+msg+"\n", args...)
			}
		}),
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			fmt.Printf("PRODUCER ERROR: "+msg+"\n", args...)
		}),
	}
	defer writer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	fmt.Printf("DEBUG: Writing %d messages to topic %s\n", len(messages), topicName)

	err := writer.WriteMessages(ctx, messages...)
	if err != nil {
		fmt.Printf("DEBUG: WriteMessages failed: %v\n", err)
		return err
	}

	fmt.Printf("DEBUG: WriteMessages completed successfully\n")
	return nil
}

func consumeMessagesDirect(brokerAddr, topicName string, expectedCount int) ([]kafka.Message, error) {
	// Use kafka-go Reader which should use Fetch API directly
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddr},
		Topic:   topicName,

		// Start from the beginning
		StartOffset: kafka.FirstOffset,

		// Enable detailed logging
		Logger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			if strings.Contains(msg, "fetch") || strings.Contains(msg, "Fetch") {
				fmt.Printf("CONSUMER: "+msg+"\n", args...)
			}
		}),
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			fmt.Printf("CONSUMER ERROR: "+msg+"\n", args...)
		}),
	})
	defer reader.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	fmt.Printf("DEBUG: Reading up to %d messages from topic %s\n", expectedCount, topicName)

	var messages []kafka.Message
	for i := 0; i < expectedCount; i++ {
		fmt.Printf("DEBUG: Reading message %d/%d\n", i+1, expectedCount)

		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			fmt.Printf("DEBUG: ReadMessage %d failed: %v\n", i+1, err)
			return messages, fmt.Errorf("read message %d: %w", i+1, err)
		}

		fmt.Printf("DEBUG: Successfully read message %d: %d bytes\n", i+1, len(msg.Value))
		messages = append(messages, msg)
	}

	fmt.Printf("DEBUG: Successfully read all %d messages\n", len(messages))
	return messages, nil
}
