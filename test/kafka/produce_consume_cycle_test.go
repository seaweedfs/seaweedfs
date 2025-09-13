package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
)

func TestKafkaProduceConsumeE2E(t *testing.T) {
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
	topicName := "e2e-test-topic"
	handler.AddTopicForTesting(topicName, 1)
	
	// Test messages
	testMessages := []string{
		"Hello Kafka Gateway!",
		"This is message 2",
		"Final test message",
	}
	
	// === PRODUCE PHASE ===
	t.Log("=== STARTING PRODUCE PHASE ===")
	
	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokerAddr),
		Topic:    topicName,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()
	
	// Produce messages
	for i, msg := range testMessages {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(msg),
		})
		cancel()
		if err != nil {
			t.Fatalf("Failed to produce message %d: %v", i, err)
		}
		t.Logf("Produced message %d: %s", i, msg)
	}
	
	t.Log("=== PRODUCE PHASE COMPLETED ===")
	
	// === CONSUME PHASE ===
	t.Log("=== STARTING CONSUME PHASE ===")
	
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddr},
		Topic:   topicName,
		GroupID: "test-consumer-group",
	})
	defer reader.Close()
	
	// Consume messages
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	consumedMessages := make([]string, 0, len(testMessages))
	
	for i := 0; i < len(testMessages); i++ {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("Failed to consume message %d: %v", i, err)
		}
		
		consumedMsg := string(msg.Value)
		consumedMessages = append(consumedMessages, consumedMsg)
		t.Logf("Consumed message %d: %s (offset: %d)", i, consumedMsg, msg.Offset)
	}
	
	t.Log("=== CONSUME PHASE COMPLETED ===")
	
	// === VERIFICATION ===
	if len(consumedMessages) != len(testMessages) {
		t.Fatalf("Expected %d messages, got %d", len(testMessages), len(consumedMessages))
	}
	
	for i, expected := range testMessages {
		if consumedMessages[i] != expected {
			t.Errorf("Message %d mismatch: expected %q, got %q", i, expected, consumedMessages[i])
		}
	}
	
	t.Log("✅ SUCCESS: Complete produce/consume cycle working!")
}
