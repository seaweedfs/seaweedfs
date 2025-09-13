package kafka

import (
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
	"github.com/segmentio/kafka-go"
)

func TestDebugReadPartitions(t *testing.T) {
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
	handler.AddTopicForTesting("readpartitions-topic", 1)
	t.Logf("Added topic: readpartitions-topic")

	// Test direct readPartitions call (this is what assignTopicPartitions calls)
	conn, err := kafka.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	t.Logf("=== Testing direct readPartitions call ===")

	// This is the exact call that assignTopicPartitions makes
	partitions, err := conn.ReadPartitions("readpartitions-topic")

	if err != nil {
		t.Logf("❌ ReadPartitions failed: %v", err)
		t.Logf("This explains why kafka-go disconnects after JoinGroup!")
	} else {
		t.Logf("✅ ReadPartitions succeeded: %d partitions", len(partitions))
		for i, p := range partitions {
			t.Logf("  Partition[%d]: Topic=%s, ID=%d, Leader=%s:%d", i, p.Topic, p.ID, p.Leader.Host, p.Leader.Port)
		}
	}
}
