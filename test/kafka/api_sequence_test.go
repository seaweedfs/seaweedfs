package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
	"github.com/segmentio/kafka-go"
)

// TestKafkaGateway_APISequence logs all API requests that kafka-go makes
func TestKafkaGateway_APISequence(t *testing.T) {
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
	topicName := "api-sequence-topic"
	handler := srv.GetHandler()
	handler.AddTopicForTesting(topicName, 1)

	// Create a writer and try to write a single message  
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokerAddr),
		Topic:        topicName,
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
		// Enable ALL kafka-go logging to see internal validation issues
		Logger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			fmt.Printf("KAFKA-GO LOG: "+msg+"\n", args...)
		}),
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			fmt.Printf("KAFKA-GO ERROR: "+msg+"\n", args...)
		}),
	}
	defer writer.Close()

	// Try to write a single message and log the full API sequence
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	fmt.Printf("\n=== STARTING kafka-go WRITE ATTEMPT ===\n")

	err := writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte("test-key"),
		Value: []byte("test-value"),
	})

	fmt.Printf("\n=== kafka-go WRITE COMPLETED ===\n")

	if err != nil {
		t.Logf("WriteMessages result: %v", err)
	} else {
		t.Logf("WriteMessages succeeded!")
	}
}
