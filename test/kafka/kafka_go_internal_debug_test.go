package kafka

import (
	"bufio"
	"bytes"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
	"github.com/segmentio/kafka-go"
)

// TestKafkaGoInternalDebug attempts to debug kafka-go's internal parsing by intercepting the read operations
func TestKafkaGoInternalDebug(t *testing.T) {
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
	handler.AddTopicForTesting("internal-debug-topic", 1)

	// Test: Manually simulate what kafka-go does
	t.Logf("=== Simulating kafka-go ReadPartitions workflow ===")

	conn, err := kafka.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	defer conn.Close()

	// Get the underlying connection to intercept reads
	t.Logf("Testing with manual response capture...")

	// Try to capture the exact response bytes that kafka-go receives
	testManualMetadataRequest(addr, t)
}

func testManualMetadataRequest(addr string, t *testing.T) {
	// Create a raw TCP connection to capture exact bytes
	conn, err := kafka.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	defer conn.Close()

	t.Logf("=== Manual Metadata Request Test ===")

	// First, let's see what happens when we call ReadPartitions and capture any intermediate state
	// We'll use reflection to access internal fields if possible

	// Try to access the internal reader
	connValue := reflect.ValueOf(conn).Elem()
	t.Logf("Connection type: %T", conn)
	t.Logf("Connection fields: %d", connValue.NumField())

	for i := 0; i < connValue.NumField(); i++ {
		field := connValue.Type().Field(i)
		if field.Name == "rbuf" || field.Name == "rlock" {
			t.Logf("Found field: %s (type: %s)", field.Name, field.Type)
		}
	}

	// Try ReadPartitions with detailed error capture
	t.Logf("Calling ReadPartitions...")
	partitions, err := conn.ReadPartitions("internal-debug-topic")
	if err != nil {
		t.Logf("ReadPartitions failed: %v", err)

		// Try to get more details about the error
		t.Logf("Error type: %T", err)
		t.Logf("Error string: %s", err.Error())

		// Check if it's related to bufio
		if err.Error() == "multiple Read calls return no data or error" {
			t.Logf("This is the bufio.Reader error we're looking for!")
			t.Logf("This typically means the underlying connection is closed or not providing data")
		}

		return
	}

	t.Logf("ReadPartitions succeeded: %d partitions", len(partitions))
}

// TestBufferedReaderBehavior tests how bufio.Reader behaves with our response
func TestBufferedReaderBehavior(t *testing.T) {
	// Create a sample Metadata v1 response like our gateway sends
	sampleResponse := []byte{
		// Correlation ID (4 bytes)
		0x00, 0x00, 0x00, 0x02,
		// Brokers count (4 bytes) = 1
		0x00, 0x00, 0x00, 0x01,
		// Broker: NodeID (4 bytes) = 1
		0x00, 0x00, 0x00, 0x01,
		// Host length (2 bytes) = 9
		0x00, 0x09,
		// Host "127.0.0.1"
		0x31, 0x32, 0x37, 0x2e, 0x30, 0x2e, 0x30, 0x2e, 0x31,
		// Port (4 bytes) = 50000
		0x00, 0x00, 0xc3, 0x50,
		// Rack (2 bytes) = 0 (empty string)
		0x00, 0x00,
		// Controller ID (4 bytes) = 1
		0x00, 0x00, 0x00, 0x01,
		// Topics count (4 bytes) = 1
		0x00, 0x00, 0x00, 0x01,
		// Topic: Error code (2 bytes) = 0
		0x00, 0x00,
		// Topic name length (2 bytes) = 19
		0x00, 0x13,
		// Topic name "internal-debug-topic"
		0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2d, 0x64, 0x65, 0x62, 0x75, 0x67, 0x2d, 0x74, 0x6f, 0x70, 0x69, 0x63,
		// IsInternal (1 byte) = 0
		0x00,
		// Partitions count (4 bytes) = 1
		0x00, 0x00, 0x00, 0x01,
		// Partition: Error code (2 bytes) = 0
		0x00, 0x00,
		// Partition ID (4 bytes) = 0
		0x00, 0x00, 0x00, 0x00,
		// Leader ID (4 bytes) = 1
		0x00, 0x00, 0x00, 0x01,
		// Replicas count (4 bytes) = 1
		0x00, 0x00, 0x00, 0x01,
		// Replica ID (4 bytes) = 1
		0x00, 0x00, 0x00, 0x01,
		// ISR count (4 bytes) = 1
		0x00, 0x00, 0x00, 0x01,
		// ISR ID (4 bytes) = 1
		0x00, 0x00, 0x00, 0x01,
	}

	t.Logf("Sample response length: %d bytes", len(sampleResponse))
	t.Logf("Sample response hex: %x", sampleResponse)

	// Test reading this with bufio.Reader
	reader := bufio.NewReader(bytes.NewReader(sampleResponse))

	// Try to read correlation ID
	correlationBytes, err := reader.Peek(4)
	if err != nil {
		t.Errorf("Failed to peek correlation ID: %v", err)
		return
	}
	t.Logf("Correlation ID bytes: %x", correlationBytes)

	// Discard the correlation ID
	n, err := reader.Discard(4)
	if err != nil {
		t.Errorf("Failed to discard correlation ID: %v", err)
		return
	}
	t.Logf("Discarded %d bytes", n)

	// Try to read brokers count
	brokersBytes, err := reader.Peek(4)
	if err != nil {
		t.Errorf("Failed to peek brokers count: %v", err)
		return
	}
	t.Logf("Brokers count bytes: %x", brokersBytes)

	// Continue reading to see if we can parse the entire response
	remaining := reader.Buffered()
	t.Logf("Remaining buffered bytes: %d", remaining)

	// Read all remaining bytes
	allBytes, err := reader.ReadBytes(0x01) // Read until we find a 0x01 byte (which should be common)
	if err != nil {
		t.Logf("ReadBytes error (expected): %v", err)
	}
	t.Logf("Read %d bytes before error", len(allBytes))
}
