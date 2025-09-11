package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
	"github.com/segmentio/kafka-go"
)

// TestConsumerOnly tests only the consumer workflow to debug SyncGroup issue
func TestConsumerOnly(t *testing.T) {
	// Start gateway server
	gatewayServer := gateway.NewServer(gateway.Options{
		Listen: ":0", // random port
	})

	go func() {
		if err := gatewayServer.Start(); err != nil {
			t.Errorf("Gateway server error: %v", err)
		}
	}()
	defer gatewayServer.Close()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	// Get the actual listening address
	host, port := gatewayServer.GetListenerAddr()
	brokerAddr := fmt.Sprintf("%s:%d", host, port)
	t.Logf("Gateway running on %s", brokerAddr)

	// Get handler and configure it
	handler := gatewayServer.GetHandler()
	handler.SetBrokerAddress(host, port)

	// Add test topic and some fake messages
	topicName := "consumer-test-topic"
	handler.AddTopicForTesting(topicName, 1)

	t.Log("=== STARTING CONSUMER ONLY TEST ===")

	// Create consumer
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{brokerAddr},
		Topic:    topicName,
		GroupID:  "consumer-test-group",
		MinBytes: 1,
		MaxBytes: 10e6,            // 10MB
		MaxWait:  2 * time.Second, // Short wait to see pattern quickly
	})
	defer reader.Close()

	// Try to read a message with a short timeout
	// This should trigger the consumer group workflow
	t.Log("Attempting to read message (will trigger consumer group workflow)...")

	// Use context with timeout instead of SetDeadline
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	_, err := reader.ReadMessage(ctx)
	if err != nil {
		t.Logf("ReadMessage failed (expected): %v", err)
		// This is expected since we don't have real messages
	} else {
		t.Log("ReadMessage succeeded unexpectedly")
	}

	t.Log("=== CONSUMER TEST COMPLETED ===")
}
