package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
)

func TestSaramaE2EProduceConsume(t *testing.T) {
	// Start gateway with test server (creates in-memory test handler)
	gatewayServer := gateway.NewTestServer(gateway.Options{
		Listen: "127.0.0.1:0",
	})

	// Note: NewTestServer creates an in-memory handler for testing only
	// Production deployments use NewServer() and require real SeaweedMQ masters

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
	topicName := "sarama-e2e-topic"
	gatewayHandler.AddTopicForTesting(topicName, 1)
	t.Logf("Added topic: %s", topicName)

	// Configure Sarama for Kafka 0.11 baseline (matches our current Produce response ordering)
	config := sarama.NewConfig()
	config.Version = sarama.V0_11_0_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Consumer.Return.Errors = true

	t.Logf("=== Testing Sarama Producer ===")

	// Create producer
	producer, err := sarama.NewSyncProducer([]string{brokerAddr}, config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Produce messages
	messages := []string{"Hello Sarama", "Message 2", "Final message"}
	for i, msgText := range messages {
		msg := &sarama.ProducerMessage{
			Topic: topicName,
			Key:   sarama.StringEncoder(fmt.Sprintf("key-%d", i)),
			Value: sarama.StringEncoder(msgText),
		}

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			t.Fatalf("Failed to produce message %d: %v", i, err)
		}
		t.Logf("✅ Produced message %d: partition=%d, offset=%d", i, partition, offset)
	}

	t.Logf("=== Testing Sarama Consumer ===")

	// Create consumer
	consumer, err := sarama.NewConsumer([]string{brokerAddr}, config)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Get partition consumer
	partitionConsumer, err := consumer.ConsumePartition(topicName, 0, sarama.OffsetOldest)
	if err != nil {
		t.Fatalf("Failed to create partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	// Consume messages
	consumedCount := 0
	timeout := time.After(5 * time.Second)

	for consumedCount < len(messages) {
		select {
		case msg := <-partitionConsumer.Messages():
			t.Logf("✅ Consumed message %d: key=%s, value=%s, offset=%d",
				consumedCount, string(msg.Key), string(msg.Value), msg.Offset)

			// Verify message content matches what we produced
			expectedValue := messages[consumedCount]
			if string(msg.Value) != expectedValue {
				t.Errorf("Message %d mismatch: got %s, want %s",
					consumedCount, string(msg.Value), expectedValue)
			}

			consumedCount++

		case err := <-partitionConsumer.Errors():
			t.Fatalf("Consumer error: %v", err)

		case <-timeout:
			t.Fatalf("Timeout waiting for messages. Consumed %d/%d", consumedCount, len(messages))
		}
	}

	t.Logf("🎉 SUCCESS: Sarama E2E test completed! Produced and consumed %d messages", len(messages))
}

func TestSaramaConsumerGroup(t *testing.T) {
	// Start gateway with test server
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
	brokerAddr := fmt.Sprintf("%s:%d", host, port)
	t.Logf("Gateway running on %s", brokerAddr)

	// Add test topic
	gatewayHandler := gatewayServer.GetHandler()
	topicName := "sarama-cg-topic"
	gatewayHandler.AddTopicForTesting(topicName, 1)
	t.Logf("Added topic: %s", topicName)

	// Configure Sarama
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true
	// Producer configuration for SyncProducer
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	t.Logf("=== Testing Sarama Consumer Group ===")

	// Create consumer group
	consumerGroup, err := sarama.NewConsumerGroup([]string{brokerAddr}, "test-group", config)
	if err != nil {
		t.Fatalf("Failed to create consumer group: %v", err)
	}
	defer consumerGroup.Close()

	// Consumer group handler
	consumerHandler := &TestConsumerGroupHandler{
		t:        t,
		messages: make(chan string, 10),
	}

	// Start consuming (this will test FindCoordinator, JoinGroup, SyncGroup workflow)
	go func() {
		ctx := context.Background()
		for {
			err := consumerGroup.Consume(ctx, []string{topicName}, consumerHandler)
			if err != nil {
				t.Logf("Consumer group error: %v", err)
				return
			}
		}
	}()

	// Give consumer group time to initialize
	time.Sleep(2 * time.Second)

	// Produce a test message
	producer, err := sarama.NewSyncProducer([]string{brokerAddr}, config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topicName,
		Value: sarama.StringEncoder("Consumer group test message"),
	}

	_, _, err = producer.SendMessage(msg)
	if err != nil {
		t.Fatalf("Failed to produce message: %v", err)
	}
	t.Logf("✅ Produced message for consumer group")

	// Wait for message consumption
	select {
	case receivedMsg := <-consumerHandler.messages:
		t.Logf("✅ Consumer group received message: %s", receivedMsg)
		if receivedMsg != "Consumer group test message" {
			t.Errorf("Message mismatch: got %s, want %s", receivedMsg, "Consumer group test message")
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("Timeout waiting for consumer group message")
	}

	t.Logf("🎉 SUCCESS: Sarama Consumer Group test completed!")
}

// TestConsumerGroupHandler implements sarama.ConsumerGroupHandler
type TestConsumerGroupHandler struct {
	t        *testing.T
	messages chan string
}

func (h *TestConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	h.t.Logf("Consumer group setup")
	return nil
}

func (h *TestConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	h.t.Logf("Consumer group cleanup")
	return nil
}

func (h *TestConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		h.t.Logf("Received message: %s", string(message.Value))
		h.messages <- string(message.Value)
		session.MarkMessage(message, "")
	}
	return nil
}
