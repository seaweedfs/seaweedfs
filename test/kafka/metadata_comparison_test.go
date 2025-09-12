package kafka

import (
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
)

func TestMetadataResponseComparison(t *testing.T) {
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

	// Add the same topic for both tests
	topic := "comparison-topic"
	gatewayServer.GetHandler().AddTopicForTesting(topic, 1)

	t.Logf("=== COMPARISON TEST ===")
	t.Logf("Gateway: %s", addr)
	t.Logf("Topic: %s", topic)

	// The key insight: Both Sarama and kafka-go should get the SAME metadata response
	// But Sarama works and kafka-go doesn't - this suggests kafka-go has stricter validation

	// Let's examine what our current Metadata v4 response looks like
	t.Logf("Run Sarama test and kafka-go test separately to compare logs")
	t.Logf("Look for differences in:")
	t.Logf("1. Response byte counts")
	t.Logf("2. Broker ID consistency")
	t.Logf("3. Partition leader/ISR values")
	t.Logf("4. Error codes")

	// This test is just for documentation - the real comparison happens in logs
}
