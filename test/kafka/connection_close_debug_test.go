package kafka

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
)

// TestConnectionCloseDebug captures the exact moment kafka-go closes the connection
func TestConnectionCloseDebug(t *testing.T) {
	// Start gateway server
	gatewayServer := gateway.NewTestServer(gateway.Options{
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

	// Add test topic
	topicName := "close-debug-topic"
	handler.AddTopicForTesting(topicName, 1)

	t.Log("=== Testing connection close timing ===")
	
	// Create a custom dialer that logs connection events
	dialer := &kafka.Dialer{
		Timeout: 5 * time.Second,
		Resolver: &net.Resolver{},
	}
	
	// Create reader with very short timeouts to see the pattern quickly
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{brokerAddr},
		Topic:    topicName,
		GroupID:  "close-debug-group",
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  1 * time.Second, // Very short wait
		Dialer:   dialer,
	})
	defer reader.Close()

	// Try to read with a very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	
	t.Log("Starting ReadMessage - this should trigger the connection close pattern...")
	
	_, err := reader.ReadMessage(ctx)
	if err != nil {
		t.Logf("ReadMessage failed (expected): %v", err)
		t.Logf("Error type: %T", err)
		
		// Check if it's a specific type of error that gives us clues
		if netErr, ok := err.(net.Error); ok {
			t.Logf("Network error - Timeout: %v, Temporary: %v", netErr.Timeout(), netErr.Temporary())
		}
	} else {
		t.Log("ReadMessage succeeded unexpectedly")
	}

	t.Log("=== Connection close debug completed ===")
	
	// The key insight is in the debug logs above - we should see the exact pattern
	// of when kafka-go closes connections after JoinGroup responses
}
