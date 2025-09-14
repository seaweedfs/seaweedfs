package kafka

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
	"github.com/segmentio/kafka-go"
)

// TestKafkaGoClient_BasicProduceConsume tests our gateway with real kafka-go client
func TestKafkaGoClient_BasicProduceConsume(t *testing.T) {
	// Start the gateway server
	srv := gateway.NewTestServer(gateway.Options{
		Listen: ":0", // Use random port
	})

	if err := srv.Start(); err != nil {
		t.Fatalf("Failed to start gateway: %v", err)
	}
	defer srv.Close()

	// Get the actual address
	brokerAddr := srv.Addr()
	t.Logf("Gateway running on %s", brokerAddr)

	// Create topic first
	topicName := "test-kafka-go-topic"
	if err := createTopicWithKafkaGo(brokerAddr, topicName); err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Test basic produce
	messages := []kafka.Message{
		{
			Key:   []byte("key1"),
			Value: []byte("Hello, Kafka Gateway!"),
		},
		{
			Key:   []byte("key2"),
			Value: []byte("This is message 2"),
		},
		{
			Key:   []byte("key3"),
			Value: []byte("Final test message"),
		},
	}

	if err := produceMessages(brokerAddr, topicName, messages); err != nil {
		t.Fatalf("Failed to produce messages: %v", err)
	}

	// Test basic consume
	consumedMessages, err := consumeMessages(brokerAddr, topicName, len(messages))
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
			if actualValue != expectedValue {
				t.Errorf("Message %d: expected value %q, got %q", i, expectedValue, actualValue)
			}
		}
	}

	t.Logf("Successfully produced and consumed %d messages", len(consumedMessages))

	// Add a small delay to ensure all kafka-go goroutines are cleaned up
	fmt.Printf("DEBUG: Waiting for cleanup...\n")
	time.Sleep(2 * time.Second)
	fmt.Printf("DEBUG: Cleanup wait completed\n")
}

// TestKafkaGoClient_ConsumerGroups tests consumer group functionality
func TestKafkaGoClient_ConsumerGroups(t *testing.T) {
	// Start the gateway server
	srv := gateway.NewTestServer(gateway.Options{
		Listen: ":0",
	})

	if err := srv.Start(); err != nil {
		t.Fatalf("Failed to start gateway: %v", err)
	}
	defer srv.Close()

	brokerAddr := srv.Addr()
	topicName := "test-consumer-group-topic"
	groupID := "test-consumer-group"

	// Create topic
	if err := createTopicWithKafkaGo(brokerAddr, topicName); err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Produce some messages
	messages := []kafka.Message{
		{Value: []byte("group-message-1")},
		{Value: []byte("group-message-2")},
		{Value: []byte("group-message-3")},
	}

	if err := produceMessages(brokerAddr, topicName, messages); err != nil {
		t.Fatalf("Failed to produce messages: %v", err)
	}

	// Test consumer group
	consumedMessages, err := consumeWithGroup(brokerAddr, topicName, groupID, len(messages))
	if err != nil {
		t.Fatalf("Failed to consume with group: %v", err)
	}

	if len(consumedMessages) != len(messages) {
		t.Errorf("Expected %d messages, got %d", len(messages), len(consumedMessages))
	}

	t.Logf("Consumer group successfully consumed %d messages", len(consumedMessages))
}

// TestKafkaGoClient_MultiplePartitions tests behavior with multiple partitions
func TestKafkaGoClient_MultiplePartitions(t *testing.T) {
	t.Skip("TODO: Enable once partition support is improved")

	// This test will be enabled once we fix partition handling
	// For now, our implementation assumes single partition per topic
}

// TestKafkaGoClient_OffsetManagement tests offset commit/fetch operations
func TestKafkaGoClient_OffsetManagement(t *testing.T) {
	// Start the gateway server
	srv := gateway.NewTestServer(gateway.Options{
		Listen: ":0",
	})

	if err := srv.Start(); err != nil {
		t.Fatalf("Failed to start gateway: %v", err)
	}
	defer srv.Close()

	brokerAddr := srv.Addr()
	topicName := "test-offset-topic"
	groupID := "test-offset-group"

	// Create topic
	if err := createTopicWithKafkaGo(brokerAddr, topicName); err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Produce messages
	messages := []kafka.Message{
		{Value: []byte("offset-message-1")},
		{Value: []byte("offset-message-2")},
		{Value: []byte("offset-message-3")},
		{Value: []byte("offset-message-4")},
		{Value: []byte("offset-message-5")},
	}

	if err := produceMessages(brokerAddr, topicName, messages); err != nil {
		t.Fatalf("Failed to produce messages: %v", err)
	}

	// Consume only first 3 messages and commit offset
	partialMessages, err := consumeWithGroupAndCommit(brokerAddr, topicName, groupID, 3)
	if err != nil {
		t.Fatalf("Failed to consume with offset commit: %v", err)
	}

	if len(partialMessages) != 3 {
		t.Errorf("Expected 3 messages, got %d", len(partialMessages))
	}

	// Create new consumer with same group ID - should resume from committed offset
	remainingMessages, err := consumeWithGroup(brokerAddr, topicName, groupID, 2)
	if err != nil {
		t.Fatalf("Failed to consume remaining messages: %v", err)
	}

	if len(remainingMessages) != 2 {
		t.Errorf("Expected 2 remaining messages, got %d", len(remainingMessages))
	}

	t.Logf("Offset management test passed: consumed %d + %d messages",
		len(partialMessages), len(remainingMessages))
}

// Helper functions

func createTopicWithKafkaGo(brokerAddr, topicName string) error {
	// Create connection with timeout
	dialer := &kafka.Dialer{
		Timeout:   5 * time.Second,
		DualStack: true,
	}

	conn, err := dialer.Dial("tcp", brokerAddr)
	if err != nil {
		return fmt.Errorf("dial broker: %w", err)
	}
	defer conn.Close()

	// Set read/write deadlines for debugging
	conn.SetDeadline(time.Now().Add(10 * time.Second))

	fmt.Printf("DEBUG: Connected to broker at %s\n", brokerAddr)

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topicName,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}

	fmt.Printf("DEBUG: Creating topic %s with 1 partition\n", topicName)
	err = conn.CreateTopics(topicConfigs...)
	if err != nil {
		return fmt.Errorf("create topic: %w", err)
	}

	fmt.Printf("DEBUG: Topic %s created successfully\n", topicName)
	return nil
}

func produceMessages(brokerAddr, topicName string, messages []kafka.Message) error {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokerAddr),
		Topic:    topicName,
		Balancer: &kafka.LeastBytes{},
		// Enable detailed logging for debugging protocol issues
		Logger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			if strings.Contains(msg, "error") || strings.Contains(msg, "failed") {
				fmt.Printf("PRODUCER ERROR: "+msg+"\n", args...)
			}
		}),
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			fmt.Printf("PRODUCER ERROR: "+msg+"\n", args...)
		}),
	}
	defer func() {
		fmt.Printf("DEBUG: Closing kafka writer\n")
		if err := writer.Close(); err != nil {
			fmt.Printf("DEBUG: Error closing writer: %v\n", err)
		}
		fmt.Printf("DEBUG: Kafka writer closed\n")
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := writer.WriteMessages(ctx, messages...)
	fmt.Printf("DEBUG: WriteMessages completed with error: %v\n", err)
	return err
}

func consumeMessages(brokerAddr, topicName string, expectedCount int) ([]kafka.Message, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddr},
		Topic:   topicName,
		// Start from the beginning
		StartOffset: kafka.FirstOffset,
		// Enable detailed logging for debugging protocol issues
		Logger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			if strings.Contains(msg, "error") || strings.Contains(msg, "failed") {
				fmt.Printf("CONSUMER ERROR: "+msg+"\n", args...)
			}
		}),
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			fmt.Printf("CONSUMER ERROR: "+msg+"\n", args...)
		}),
	})
	defer func() {
		fmt.Printf("DEBUG: Closing kafka reader\n")
		if err := reader.Close(); err != nil {
			fmt.Printf("DEBUG: Error closing reader: %v\n", err)
		}
		fmt.Printf("DEBUG: Kafka reader closed\n")
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var messages []kafka.Message
	for i := 0; i < expectedCount; i++ {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			fmt.Printf("DEBUG: Error reading message %d: %v\n", i, err)
			return messages, fmt.Errorf("read message %d: %w", i, err)
		}
		messages = append(messages, msg)
	}

	fmt.Printf("DEBUG: Successfully read %d messages\n", len(messages))
	return messages, nil
}

func consumeWithGroup(brokerAddr, topicName, groupID string, expectedCount int) ([]kafka.Message, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddr},
		Topic:   topicName,
		GroupID: groupID,
		// Enable detailed logging for debugging protocol issues
		Logger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			if strings.Contains(msg, "error") || strings.Contains(msg, "failed") {
				fmt.Printf("GROUP CONSUMER ERROR: "+msg+"\n", args...)
			}
		}),
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			fmt.Printf("GROUP CONSUMER ERROR: "+msg+"\n", args...)
		}),
	})
	defer reader.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var messages []kafka.Message
	for i := 0; i < expectedCount; i++ {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			return messages, fmt.Errorf("read message %d: %w", i, err)
		}
		messages = append(messages, msg)
	}

	return messages, nil
}

func consumeWithGroupAndCommit(brokerAddr, topicName, groupID string, expectedCount int) ([]kafka.Message, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddr},
		Topic:   topicName,
		GroupID: groupID,
		// Enable detailed logging for debugging protocol issues
		Logger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			if strings.Contains(msg, "error") || strings.Contains(msg, "failed") {
				fmt.Printf("GROUP CONSUMER WITH COMMIT ERROR: "+msg+"\n", args...)
			}
		}),
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			fmt.Printf("GROUP CONSUMER WITH COMMIT ERROR: "+msg+"\n", args...)
		}),
	})
	defer reader.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var messages []kafka.Message
	for i := 0; i < expectedCount; i++ {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			return messages, fmt.Errorf("read message %d: %w", i, err)
		}
		messages = append(messages, msg)

		// Commit the message
		if err := reader.CommitMessages(ctx, msg); err != nil {
			return messages, fmt.Errorf("commit message %d: %w", i, err)
		}
	}

	return messages, nil
}

// waitForPort waits for a TCP port to become available
func waitForPort(addr string) error {
	for i := 0; i < 50; i++ { // Wait up to 5 seconds
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("port %s not available after 5 seconds", addr)
}
