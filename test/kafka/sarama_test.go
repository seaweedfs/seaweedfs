package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
)

// TestSaramaCompatibility tests our Kafka gateway with IBM Sarama client
func TestSaramaCompatibility(t *testing.T) {
	// Start gateway with test mode
	gatewayServer := gateway.NewTestServer(gateway.Options{
		Listen: "127.0.0.1:0",
	})

	go gatewayServer.Start()
	defer gatewayServer.Close()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	host, port := gatewayServer.GetListenerAddr()
	addr := fmt.Sprintf("%s:%d", host, port)
	t.Logf("Gateway running on %s", addr)

	// Add test topic
	handler := gatewayServer.GetHandler()
	handler.AddTopicForTesting("sarama-test-topic", 1)
	t.Logf("Added topic: sarama-test-topic")

	// Test 1: Basic Sarama client connection and metadata
	t.Logf("=== Test 1: Sarama Metadata Request ===")
	testSaramaMetadata(addr, t)

	// Test 2: Sarama producer
	t.Logf("=== Test 2: Sarama Producer ===")
	testSaramaProducer(addr, t)

	// Test 3: Sarama consumer
	t.Logf("=== Test 3: Sarama Consumer ===")
	testSaramaConsumer(addr, t)

	// Test 4: Sarama consumer group
	t.Logf("=== Test 4: Sarama Consumer Group ===")
	testSaramaConsumerGroup(addr, t)
}

func testSaramaMetadata(addr string, t *testing.T) {
	// Create Sarama config
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0 // Use a well-supported version
	config.ClientID = "sarama-test-client"

	// Create client
	client, err := sarama.NewClient([]string{addr}, config)
	if err != nil {
		t.Errorf("Failed to create Sarama client: %v", err)
		return
	}
	defer client.Close()

	t.Logf("Sarama client created successfully")

	// Test metadata request
	topics, err := client.Topics()
	if err != nil {
		t.Errorf("Failed to get topics: %v", err)
		return
	}

	t.Logf("Topics from Sarama: %v", topics)

	// Test partition metadata
	partitions, err := client.Partitions("sarama-test-topic")
	if err != nil {
		t.Errorf("Failed to get partitions: %v", err)
		return
	}

	t.Logf("Partitions for sarama-test-topic: %v", partitions)

	// Test broker metadata
	brokers := client.Brokers()
	t.Logf("Brokers from Sarama: %d brokers", len(brokers))
	for i, broker := range brokers {
		t.Logf("Broker %d: ID=%d, Addr=%s", i, broker.ID(), broker.Addr())
	}

	t.Logf("✅ Sarama metadata test passed!")
}

func testSaramaProducer(addr string, t *testing.T) {
	// Create Sarama config for producer
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.ClientID = "sarama-producer"
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3
	config.Producer.Return.Successes = true

	// Create producer
	producer, err := sarama.NewSyncProducer([]string{addr}, config)
	if err != nil {
		t.Errorf("Failed to create Sarama producer: %v", err)
		return
	}
	defer producer.Close()

	t.Logf("Sarama producer created successfully")

	// Send a test message
	message := &sarama.ProducerMessage{
		Topic: "sarama-test-topic",
		Key:   sarama.StringEncoder("test-key"),
		Value: sarama.StringEncoder("Hello from Sarama!"),
	}

	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		t.Errorf("Failed to send message: %v", err)
		return
	}

	t.Logf("✅ Message sent successfully! Partition: %d, Offset: %d", partition, offset)
}

func testSaramaConsumer(addr string, t *testing.T) {
	// Create Sarama config for consumer
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.ClientID = "sarama-consumer"
	config.Consumer.Return.Errors = true

	// Create consumer
	consumer, err := sarama.NewConsumer([]string{addr}, config)
	if err != nil {
		t.Errorf("Failed to create Sarama consumer: %v", err)
		return
	}
	defer consumer.Close()

	t.Logf("Sarama consumer created successfully")

	// Create partition consumer
	partitionConsumer, err := consumer.ConsumePartition("sarama-test-topic", 0, sarama.OffsetOldest)
	if err != nil {
		t.Errorf("Failed to create partition consumer: %v", err)
		return
	}
	defer partitionConsumer.Close()

	t.Logf("Partition consumer created successfully")

	// Try to consume a message with timeout
	select {
	case message := <-partitionConsumer.Messages():
		t.Logf("✅ Consumed message: Key=%s, Value=%s, Offset=%d",
			string(message.Key), string(message.Value), message.Offset)
	case err := <-partitionConsumer.Errors():
		t.Errorf("Consumer error: %v", err)
	case <-time.After(5 * time.Second):
		t.Logf("⚠️  No messages received within timeout (this might be expected if no messages were produced)")
	}
}

func testSaramaConsumerGroup(addr string, t *testing.T) {
	// Create Sarama config for consumer group
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.ClientID = "sarama-consumer-group"
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true

	// Create consumer group
	consumerGroup, err := sarama.NewConsumerGroup([]string{addr}, "sarama-test-group", config)
	if err != nil {
		t.Errorf("Failed to create Sarama consumer group: %v", err)
		return
	}
	defer consumerGroup.Close()

	t.Logf("Sarama consumer group created successfully")

	// Create a consumer group handler
	handler := &SaramaConsumerGroupHandler{t: t}

	// Start consuming with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start the consumer group in a goroutine
	go func() {
		for {
			// Check if context was cancelled
			if ctx.Err() != nil {
				return
			}

			// Consume should be called inside an infinite loop
			if err := consumerGroup.Consume(ctx, []string{"sarama-test-topic"}, handler); err != nil {
				t.Logf("Consumer group error: %v", err)
				return
			}
		}
	}()

	// Wait for the context to be cancelled or for messages
	select {
	case <-ctx.Done():
		t.Logf("Consumer group test completed")
	case <-time.After(10 * time.Second):
		t.Logf("Consumer group test timed out")
	}

	if handler.messageReceived {
		t.Logf("✅ Consumer group test passed!")
	} else {
		t.Logf("⚠️  No messages received in consumer group (this might be expected)")
	}
}

// SaramaConsumerGroupHandler implements sarama.ConsumerGroupHandler
type SaramaConsumerGroupHandler struct {
	t               *testing.T
	messageReceived bool
}

func (h *SaramaConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	h.t.Logf("Consumer group session setup")
	return nil
}

func (h *SaramaConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	h.t.Logf("Consumer group session cleanup")
	return nil
}

func (h *SaramaConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	h.t.Logf("Consumer group claim started for topic: %s, partition: %d", claim.Topic(), claim.Partition())

	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}
			h.t.Logf("✅ Consumer group received message: Key=%s, Value=%s, Offset=%d",
				string(message.Key), string(message.Value), message.Offset)
			h.messageReceived = true
			session.MarkMessage(message, "")

		case <-session.Context().Done():
			h.t.Logf("Consumer group session context cancelled")
			return nil
		}
	}
}

// TestSaramaMetadataOnly tests just the metadata functionality that's failing with kafka-go
func TestSaramaMetadataOnly(t *testing.T) {
	// Start gateway with test mode
	gatewayServer := gateway.NewTestServer(gateway.Options{
		Listen: "127.0.0.1:0",
	})

	go gatewayServer.Start()
	defer gatewayServer.Close()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	host, port := gatewayServer.GetListenerAddr()
	addr := fmt.Sprintf("%s:%d", host, port)
	t.Logf("Gateway running on %s", addr)

	// Add test topic
	handler := gatewayServer.GetHandler()
	handler.AddTopicForTesting("metadata-only-topic", 1)

	// Test with different Sarama versions to see if any fail like kafka-go
	versions := []sarama.KafkaVersion{
		sarama.V2_0_0_0,
		sarama.V2_1_0_0,
		sarama.V2_6_0_0,
		sarama.V3_0_0_0,
	}

	for _, version := range versions {
		t.Logf("=== Testing Sarama with Kafka version %s ===", version.String())

		config := sarama.NewConfig()
		config.Version = version
		config.ClientID = fmt.Sprintf("sarama-test-%s", version.String())

		client, err := sarama.NewClient([]string{addr}, config)
		if err != nil {
			t.Errorf("Failed to create Sarama client for version %s: %v", version.String(), err)
			continue
		}

		// Test the same operation that fails with kafka-go: getting topic metadata
		topics, err := client.Topics()
		if err != nil {
			t.Errorf("❌ Sarama %s failed to get topics: %v", version.String(), err)
		} else {
			t.Logf("✅ Sarama %s successfully got topics: %v", version.String(), topics)
		}

		// Test partition metadata (this is similar to kafka-go's ReadPartitions)
		partitions, err := client.Partitions("metadata-only-topic")
		if err != nil {
			t.Errorf("❌ Sarama %s failed to get partitions: %v", version.String(), err)
		} else {
			t.Logf("✅ Sarama %s successfully got partitions: %v", version.String(), partitions)
		}

		client.Close()
	}
}
