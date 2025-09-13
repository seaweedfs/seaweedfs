package kafka

import (
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
	"github.com/segmentio/kafka-go"
)

func TestMetadataV6Debug(t *testing.T) {
	// Start gateway
	gatewayServer := gateway.NewServer(gateway.Options{Listen: "127.0.0.1:0"})
	go func() {
		if err := gatewayServer.Start(); err != nil {
			t.Errorf("Failed to start gateway: %v", err)
		}
	}()
	defer gatewayServer.Close()

	time.Sleep(100 * time.Millisecond)

	host, port := gatewayServer.GetListenerAddr()
	addr := fmt.Sprintf("%s:%d", host, port)
	topic := "metadata-debug-topic"
	gatewayServer.GetHandler().AddTopicForTesting(topic, 1)

	// Create a simple kafka-go client that just gets metadata
	conn, err := kafka.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Get metadata - this should work without loops
	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		t.Fatalf("Failed to read partitions: %v", err)
	}

	t.Logf("Successfully read %d partitions for topic %s", len(partitions), topic)
	for _, p := range partitions {
		t.Logf("Partition %d: Leader=%d, Replicas=%v, ISR=%v",
			p.ID, p.Leader.ID, p.Replicas, p.Isr)
	}
}
