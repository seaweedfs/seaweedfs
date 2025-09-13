package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
	"github.com/segmentio/kafka-go"
)

func TestDebugConsumerGroupWorkflow(t *testing.T) {
	// Start gateway
	gatewayServer := gateway.NewTestServer(gateway.Options{
		Listen: "127.0.0.1:0",
	})

	go func() {
		if err := gatewayServer.Start(); err != nil {
			t.Errorf("Failed to start gateway: %v", err)
		}
	}()
	defer gatewayServer.Close()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	host, port := gatewayServer.GetListenerAddr()
	addr := fmt.Sprintf("%s:%d", host, port)
	t.Logf("Gateway running on %s", addr)

	// Add test topic
	handler := gatewayServer.GetHandler()
	handler.AddTopicForTesting("debug-topic", 1)
	t.Logf("Added topic: debug-topic")

	// Create a simple consumer that will trigger the consumer group workflow
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{addr},
		Topic:    "debug-topic",
		GroupID:  "debug-group",
		MinBytes: 1,
		MaxBytes: 1024,
	})
	defer reader.Close()

	// Try to read a message (this will trigger the consumer group workflow)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	t.Logf("=== Starting consumer group workflow ===")

	// This should trigger: FindCoordinator -> JoinGroup -> (assignTopicPartitions -> readPartitions) -> SyncGroup
	_, err := reader.ReadMessage(ctx)

	if err != nil {
		if err == context.DeadlineExceeded {
			t.Logf("Expected timeout - checking if SyncGroup was called")
		} else {
			t.Logf("ReadMessage error: %v", err)
		}
	} else {
		t.Logf("Unexpected success - message read")
	}

	t.Logf("=== Consumer group workflow completed ===")
}
