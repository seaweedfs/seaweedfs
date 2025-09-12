package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
	"github.com/segmentio/kafka-go"
)

func TestKafkaGo_ProduceOnly(t *testing.T) {
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
	topic := "kgo-produce-only"
	gatewayServer.GetHandler().AddTopicForTesting(topic, 1)

	w := &kafka.Writer{
		Addr:         kafka.TCP(addr),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 50 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}
	defer w.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := w.WriteMessages(ctx, kafka.Message{Key: []byte("k"), Value: []byte("v")})
	if err != nil {
		t.Fatalf("kafka-go produce failed: %v", err)
	}
}

func TestKafkaGo_ProduceConsume(t *testing.T) {
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
	topic := "kgo-produce-consume"
	gatewayServer.GetHandler().AddTopicForTesting(topic, 1)

	// Test messages
	testMessages := []kafka.Message{
		{Key: []byte("key1"), Value: []byte("Hello World!")},
		{Key: []byte("key2"), Value: []byte("Kafka Gateway Test")},
		{Key: []byte("key3"), Value: []byte("Final Message")},
	}

	t.Logf("=== Testing kafka-go Producer ===")

	// Produce messages
	w := &kafka.Writer{
		Addr:         kafka.TCP(addr),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 50 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}
	defer w.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := w.WriteMessages(ctx, testMessages...)
	if err != nil {
		t.Fatalf("kafka-go produce failed: %v", err)
	}
	t.Logf("✅ Successfully produced %d messages", len(testMessages))

	t.Logf("=== Testing kafka-go Consumer ===")

	// Consume messages
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{addr},
		Topic:       topic,
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
	})
	defer r.Close()

	consumedMessages := make([]kafka.Message, 0, len(testMessages))
	consumeCtx, consumeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer consumeCancel()

	for i := 0; i < len(testMessages); i++ {
		msg, err := r.ReadMessage(consumeCtx)
		if err != nil {
			t.Fatalf("kafka-go consume failed at message %d: %v", i, err)
		}
		consumedMessages = append(consumedMessages, msg)
		t.Logf("✅ Consumed message %d: key=%s, value=%s, offset=%d", 
			i, string(msg.Key), string(msg.Value), msg.Offset)
	}

	// Validate messages
	if len(consumedMessages) != len(testMessages) {
		t.Fatalf("Expected %d messages, got %d", len(testMessages), len(consumedMessages))
	}

	for i, consumed := range consumedMessages {
		expected := testMessages[i]
		if string(consumed.Key) != string(expected.Key) {
			t.Errorf("Message %d key mismatch: got %s, want %s", 
				i, string(consumed.Key), string(expected.Key))
		}
		if string(consumed.Value) != string(expected.Value) {
			t.Errorf("Message %d value mismatch: got %s, want %s", 
				i, string(consumed.Value), string(expected.Value))
		}
	}

	t.Logf("🎉 SUCCESS: kafka-go produce-consume test completed with %d messages", len(consumedMessages))
}

func TestKafkaGo_ConsumerGroup(t *testing.T) {
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
	topic := "kgo-consumer-group"
	groupID := "test-group"
	gatewayServer.GetHandler().AddTopicForTesting(topic, 1)

	// Test messages
	testMessages := []kafka.Message{
		{Key: []byte("cg-key1"), Value: []byte("Consumer Group Message 1")},
		{Key: []byte("cg-key2"), Value: []byte("Consumer Group Message 2")},
		{Key: []byte("cg-key3"), Value: []byte("Consumer Group Message 3")},
	}

	t.Logf("=== Testing kafka-go Producer for Consumer Group ===")

	// Produce messages
	w := &kafka.Writer{
		Addr:         kafka.TCP(addr),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 50 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}
	defer w.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := w.WriteMessages(ctx, testMessages...)
	if err != nil {
		t.Fatalf("kafka-go produce failed: %v", err)
	}
	t.Logf("✅ Successfully produced %d messages for consumer group", len(testMessages))

	t.Logf("=== Testing kafka-go Consumer Group ===")

	// Consume messages with consumer group
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{addr},
		Topic:       topic,
		GroupID:     groupID,
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
	})
	defer r.Close()

	consumedMessages := make([]kafka.Message, 0, len(testMessages))
	consumeCtx, consumeCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer consumeCancel()

	for i := 0; i < len(testMessages); i++ {
		msg, err := r.ReadMessage(consumeCtx)
		if err != nil {
			t.Fatalf("kafka-go consumer group failed at message %d: %v", i, err)
		}
		consumedMessages = append(consumedMessages, msg)
		t.Logf("✅ Consumer group consumed message %d: key=%s, value=%s, offset=%d", 
			i, string(msg.Key), string(msg.Value), msg.Offset)
	}

	// Validate messages
	if len(consumedMessages) != len(testMessages) {
		t.Fatalf("Expected %d messages, got %d", len(testMessages), len(consumedMessages))
	}

	for i, consumed := range consumedMessages {
		expected := testMessages[i]
		if string(consumed.Key) != string(expected.Key) {
			t.Errorf("Message %d key mismatch: got %s, want %s", 
				i, string(consumed.Key), string(expected.Key))
		}
		if string(consumed.Value) != string(expected.Value) {
			t.Errorf("Message %d value mismatch: got %s, want %s", 
				i, string(consumed.Value), string(expected.Value))
		}
	}

	t.Logf("🎉 SUCCESS: kafka-go consumer group test completed with %d messages", len(consumedMessages))
}
