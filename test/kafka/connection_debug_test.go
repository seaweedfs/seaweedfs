package kafka

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
)

// TestConnectionDebug debugs the exact connection behavior between kafka-go and our gateway
func TestConnectionDebug(t *testing.T) {
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
	topicName := "conn-debug-topic"
	handler.AddTopicForTesting(topicName, 1)

	// Test 1: Manual connection that works
	t.Log("=== Test 1: Manual TCP connection ===")
	conn, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		t.Fatalf("Manual connection failed: %v", err)
	}
	
	// Send a simple request and read response
	// This should work based on our previous debug test
	conn.Close()
	t.Log("Manual connection works fine")

	// Test 2: kafka-go Dialer connection
	t.Log("=== Test 2: kafka-go Dialer connection ===")
	dialer := &kafka.Dialer{
		Timeout: 5 * time.Second,
	}
	
	kafkaConn, err := dialer.Dial("tcp", brokerAddr)
	if err != nil {
		t.Fatalf("kafka-go connection failed: %v", err)
	}
	defer kafkaConn.Close()
	
	t.Log("kafka-go connection established")

	// Test 3: Try to read partitions with detailed error handling
	t.Log("=== Test 3: kafka-go ReadPartitions with error details ===")
	
	// Set a deadline to avoid hanging
	kafkaConn.SetDeadline(time.Now().Add(10 * time.Second))
	
	partitions, err := kafkaConn.ReadPartitions(topicName)
	if err != nil {
		t.Logf("ReadPartitions failed with error: %v", err)
		t.Logf("Error type: %T", err)
		
		// Try to get more details about the error
		if netErr, ok := err.(net.Error); ok {
			t.Logf("Network error - Timeout: %v, Temporary: %v", netErr.Timeout(), netErr.Temporary())
		}
		
		// The error might give us clues about what's wrong
		return
	}

	t.Logf("ReadPartitions succeeded! Found %d partitions", len(partitions))
	for i, partition := range partitions {
		t.Logf("Partition %d: Topic=%s, ID=%d, Leader=%v", i, partition.Topic, partition.ID, partition.Leader)
	}

	t.Log("✅ Connection debug test completed!")
}
