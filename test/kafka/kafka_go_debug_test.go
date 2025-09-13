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

// TestKafkaGoDeepDebug attempts to get more detailed error information from kafka-go
func TestKafkaGoDeepDebug(t *testing.T) {
	// Start gateway
	gatewayServer := gateway.NewServer(gateway.Options{
		Listen: "127.0.0.1:0",
	})

	go gatewayServer.Start()
	defer gatewayServer.Close()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	host, port := gatewayServer.GetListenerAddr()
	addr := fmt.Sprintf("%s:%d", host, port)
	t.Logf("Gateway running on %s", addr)

	// Add test topic
	handler := gatewayServer.GetHandler()
	handler.AddTopicForTesting("debug-topic", 1)

	// Test 1: Try different kafka-go connection approaches
	t.Logf("=== Test 1: Basic Dial ===")
	testBasicDial(addr, t)

	t.Logf("=== Test 2: Dialer with Timeout ===")
	testDialerWithTimeout(addr, t)

	t.Logf("=== Test 3: Reader ReadPartitions ===")
	testReaderReadPartitions(addr, t)
}

func testBasicDial(addr string, t *testing.T) {
	conn, err := kafka.Dial("tcp", addr)
	if err != nil {
		t.Errorf("Basic dial failed: %v", err)
		return
	}
	defer conn.Close()

	// Set a deadline to avoid hanging
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	t.Logf("Basic dial successful")

	// Try ReadPartitions with error details
	partitions, err := conn.ReadPartitions("debug-topic")
	if err != nil {
		t.Errorf("ReadPartitions failed: %v", err)
		
		// Check if it's a specific type of error
		switch e := err.(type) {
		case net.Error:
			t.Errorf("Network error: Timeout=%v, Temporary=%v", e.Timeout(), e.Temporary())
		case *net.OpError:
			t.Errorf("Operation error: Op=%s, Net=%s, Source=%v, Addr=%v, Err=%v", 
				e.Op, e.Net, e.Source, e.Addr, e.Err)
		default:
			t.Errorf("Error type: %T", err)
		}
		return
	}

	t.Logf("ReadPartitions successful: %d partitions", len(partitions))
}

func testDialerWithTimeout(addr string, t *testing.T) {
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		t.Errorf("Dialer dial failed: %v", err)
		return
	}
	defer conn.Close()

	t.Logf("Dialer dial successful")

	// Try ReadPartitions
	partitions, err := conn.ReadPartitions("debug-topic")
	if err != nil {
		t.Errorf("Dialer ReadPartitions failed: %v", err)
		return
	}

	t.Logf("Dialer ReadPartitions successful: %d partitions", len(partitions))
}

func testReaderReadPartitions(addr string, t *testing.T) {
	// Create a Reader and try to get partitions
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{addr},
		Topic:   "debug-topic",
		GroupID: "debug-group",
	})
	defer reader.Close()

	// Try to read partitions using the Reader's connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// This should internally call ReadPartitions
	_, err := reader.ReadMessage(ctx)
	if err != nil {
		t.Errorf("Reader ReadMessage failed: %v", err)
		
		// Check error details
		if ctx.Err() == context.DeadlineExceeded {
			t.Errorf("Context deadline exceeded - likely hanging on ReadPartitions")
		}
		return
	}

	t.Logf("Reader ReadMessage successful")
}
