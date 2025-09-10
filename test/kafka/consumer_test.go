package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
	"github.com/segmentio/kafka-go"
)

// TestKafkaGoReader tests if kafka-go Reader has different validation than Writer
func TestKafkaGoReader(t *testing.T) {
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

	// Pre-create topic
	topicName := "reader-test-topic"
	handler := srv.GetHandler()
	handler.AddTopicForTesting(topicName, 1)

	// Create a Reader (consumer) with detailed logging
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddr},
		Topic:   topicName,
		Logger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			fmt.Printf("KAFKA-GO READER LOG: "+msg+"\n", args...)
		}),
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			fmt.Printf("KAFKA-GO READER ERROR: "+msg+"\n", args...)
		}),
	})
	defer reader.Close()

	// Try to read messages (this will trigger Metadata requests)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fmt.Printf("\n=== STARTING kafka-go READER TEST ===\n")

	// This should trigger Metadata and Fetch requests
	message, err := reader.ReadMessage(ctx)

	fmt.Printf("\n=== kafka-go READER COMPLETED ===\n")

	if err != nil {
		t.Logf("ReadMessage result: %v", err)
		// Check if it's a timeout (expected for empty topic) vs validation error
		if err == context.DeadlineExceeded {
			t.Log("✅ Reader timed out waiting for messages - this suggests Metadata validation passed!")
		}
	} else {
		t.Logf("ReadMessage succeeded: key=%s, value=%s", string(message.Key), string(message.Value))
	}
}
