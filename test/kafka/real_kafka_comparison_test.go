package kafka

import (
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// TestRealKafkaComparison tests kafka-go against a real Kafka broker (if available)
// This test will be skipped if no real Kafka broker is running on localhost:9092
func TestRealKafkaComparison(t *testing.T) {
	// Try to connect to a real Kafka broker
	dialer := &kafka.Dialer{
		Timeout: 2 * time.Second,
	}
	
	conn, err := dialer.Dial("tcp", "localhost:9092")
	if err != nil {
		t.Skipf("No real Kafka broker available on localhost:9092: %v", err)
		return
	}
	defer conn.Close()

	t.Log("=== Testing kafka-go ReadPartitions against real Kafka ===")
	
	// Test ReadPartitions against real Kafka
	partitions, err := conn.ReadPartitions("__consumer_offsets") // This topic should exist
	if err != nil {
		// Try a different approach - create a test topic first
		t.Logf("ReadPartitions failed for __consumer_offsets: %v", err)
		
		// Try to read all partitions
		partitions, err = conn.ReadPartitions()
		if err != nil {
			t.Fatalf("ReadPartitions failed: %v", err)
		}
	}

	t.Logf("ReadPartitions succeeded! Found %d partitions", len(partitions))
	for i, partition := range partitions {
		if i < 5 { // Limit output
			t.Logf("Partition %d: Topic=%s, ID=%d, Leader=%v", i, partition.Topic, partition.ID, partition.Leader)
		}
	}

	// If we get here, kafka-go's ReadPartitions works fine with a real Kafka broker
	t.Log("✅ kafka-go ReadPartitions works with real Kafka!")
}
