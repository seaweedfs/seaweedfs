package kafka

import (
	"fmt"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
)

// TestKafkaGoMetadataV1Compatibility tests if our Metadata v1 response is compatible with kafka-go
func TestKafkaGoMetadataV1Compatibility(t *testing.T) {
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

	// Add test topic
	topicName := "metadata-test-topic"
	handler.AddTopicForTesting(topicName, 1)

	// Test kafka-go's ability to read partitions (which uses Metadata v1/v6)
	// This is what kafka-go does after JoinGroup in consumer groups
	t.Log("=== Testing kafka-go ReadPartitions (Metadata v1) ===")
	
	// Create a connection to test ReadPartitions
	dialer := &kafka.Dialer{
		Timeout: 5 * time.Second,
	}
	conn, err := dialer.Dial("tcp", brokerAddr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// ReadPartitions uses Metadata v1 internally
	partitions, err := conn.ReadPartitions(topicName)
	if err != nil {
		t.Fatalf("ReadPartitions failed: %v", err)
	}

	t.Logf("ReadPartitions succeeded! Found %d partitions", len(partitions))
	for i, partition := range partitions {
		t.Logf("Partition %d: Topic=%s, ID=%d, Leader=%v", i, partition.Topic, partition.ID, partition.Leader)
	}

	// If we get here, our Metadata v1 response is compatible with kafka-go!
	t.Log("✅ Metadata v1 compatibility test PASSED!")
}
