package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
)

func TestConsumerGroup_Debug(t *testing.T) {
	// Start Kafka gateway
	gatewayServer := gateway.NewTestServer(gateway.Options{Listen: ":0"})

	go func() {
		if err := gatewayServer.Start(); err != nil {
			t.Logf("Gateway server error: %v", err)
		}
	}()
	defer gatewayServer.Close()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)
	host, port := gatewayServer.GetListenerAddr()
	brokerAddr := fmt.Sprintf("%s:%d", host, port)

	// Test configuration
	topicName := "debug-test"
	groupID := "debug-group"

	// Add topic for testing
	gatewayServer.GetHandler().AddTopicForTesting(topicName, 1)

	// Create Sarama config
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true
	config.Producer.Return.Successes = true

	// Produce one test message
	t.Logf("=== Producing 1 test message ===")
	producer, err := sarama.NewSyncProducer([]string{brokerAddr}, config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	message := &sarama.ProducerMessage{
		Topic: topicName,
		Key:   sarama.StringEncoder("debug-key"),
		Value: sarama.StringEncoder("Debug Message"),
	}

	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		t.Fatalf("Failed to produce message: %v", err)
	}
	t.Logf("✅ Produced message: partition=%d, offset=%d", partition, offset)

	// Create a simple consumer group handler
	handler := &DebugHandler{
		messages: make(chan *sarama.ConsumerMessage, 1),
		ready:    make(chan bool),
		t:        t,
	}

	// Start one consumer
	t.Logf("=== Starting 1 consumer in group '%s' ===", groupID)

	consumerGroup, err := sarama.NewConsumerGroup([]string{brokerAddr}, groupID, config)
	if err != nil {
		t.Fatalf("Failed to create consumer group: %v", err)
	}
	defer consumerGroup.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start consuming in a goroutine
	go func() {
		t.Logf("Starting consumption...")
		err := consumerGroup.Consume(ctx, []string{topicName}, handler)
		if err != nil && err != context.DeadlineExceeded {
			t.Logf("Consumer error: %v", err)
		}
		t.Logf("Consumption finished")
	}()

	// Wait for consumer to be ready or timeout
	t.Logf("Waiting for consumer to be ready...")
	select {
	case <-handler.ready:
		t.Logf("✅ Consumer is ready!")

		// Try to consume the message
		select {
		case msg := <-handler.messages:
			t.Logf("✅ Consumed message: key=%s, value=%s, offset=%d",
				string(msg.Key), string(msg.Value), msg.Offset)
		case <-time.After(5 * time.Second):
			t.Logf("⚠️ No message received within timeout")
		}

	case <-time.After(8 * time.Second):
		t.Logf("❌ Timeout waiting for consumer to be ready")
	}

	t.Logf("🎉 Debug test completed")
}

// DebugHandler implements sarama.ConsumerGroupHandler for debugging
type DebugHandler struct {
	messages chan *sarama.ConsumerMessage
	ready    chan bool
	t        *testing.T
}

func (h *DebugHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.t.Logf("🔧 Consumer group session setup - Generation: %d, Claims: %v",
		session.GenerationID(), session.Claims())
	close(h.ready)
	return nil
}

func (h *DebugHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	h.t.Logf("🧹 Consumer group session cleanup")
	return nil
}

func (h *DebugHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	h.t.Logf("🍽️ Starting to consume partition %d from offset %d",
		claim.Partition(), claim.InitialOffset())

	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				h.t.Logf("📭 Received nil message, ending consumption")
				return nil
			}
			h.t.Logf("📨 Received message: key=%s, value=%s, offset=%d",
				string(message.Key), string(message.Value), message.Offset)
			h.messages <- message
			session.MarkMessage(message, "")
		case <-session.Context().Done():
			h.t.Logf("🛑 Session context done")
			return nil
		}
	}
}
