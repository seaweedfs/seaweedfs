package kafka

import (
	"fmt"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
)

func TestDebugProduceV7Format(t *testing.T) {
	// Start gateway
	gatewayServer := gateway.NewServer(gateway.Options{
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
	brokerAddr := fmt.Sprintf("%s:%d", host, port)
	t.Logf("Gateway running on %s", brokerAddr)

	// Add test topic
	gatewayHandler := gatewayServer.GetHandler()
	topicName := "debug-produce-topic"
	gatewayHandler.AddTopicForTesting(topicName, 1)
	t.Logf("Added topic: %s", topicName)

	// Configure Sarama for Kafka 2.1.0 (which uses Produce v7)
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	t.Logf("=== Testing single Sarama Produce v7 request ===")

	// Create producer
	producer, err := sarama.NewSyncProducer([]string{brokerAddr}, config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Send a single message to capture the exact request format
	msg := &sarama.ProducerMessage{
		Topic: topicName,
		Key:   sarama.StringEncoder("test-key"),
		Value: sarama.StringEncoder("test-value"),
	}

	t.Logf("Sending message to topic: %s", topicName)
	partition, offset, err := producer.SendMessage(msg)
	
	if err != nil {
		t.Logf("❌ Produce failed (expected): %v", err)
		t.Logf("This allows us to see the debug output of the malformed request parsing")
	} else {
		t.Logf("✅ Produce succeeded: partition=%d, offset=%d", partition, offset)
	}

	t.Logf("Check the debug output above to see the actual Produce v7 request format")
}
