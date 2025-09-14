package kafka

import (
	"fmt"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
)

// TestSaramaBasic tests basic Sarama functionality without consumer groups
func TestSaramaBasic(t *testing.T) {
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
	handler.AddTopicForTesting("sarama-basic-topic", 1)
	t.Logf("Added topic: sarama-basic-topic")

	// Test 1: Basic Sarama client connection and metadata
	t.Logf("=== Test 1: Sarama Metadata Request ===")
	testSaramaBasicMetadata(addr, t)

	// Test 2: Sarama producer
	t.Logf("=== Test 2: Sarama Producer ===")
	testSaramaBasicProducer(addr, t)

	// Test 3: Sarama consumer (without consumer groups)
	t.Logf("=== Test 3: Sarama Consumer ===")
	testSaramaBasicConsumer(addr, t)

	t.Logf("🎉 All Sarama basic tests passed!")
}

func testSaramaBasicMetadata(addr string, t *testing.T) {
	// Create Sarama config
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0 // Use a well-supported version
	config.ClientID = "sarama-basic-client"

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
	partitions, err := client.Partitions("sarama-basic-topic")
	if err != nil {
		t.Errorf("Failed to get partitions: %v", err)
		return
	}

	t.Logf("Partitions for sarama-basic-topic: %v", partitions)

	// Test broker metadata
	brokers := client.Brokers()
	t.Logf("Brokers from Sarama: %d brokers", len(brokers))
	for i, broker := range brokers {
		t.Logf("Broker %d: ID=%d, Addr=%s", i, broker.ID(), broker.Addr())
	}

	t.Logf("✅ Sarama metadata test passed!")
}

func testSaramaBasicProducer(addr string, t *testing.T) {
	// Create Sarama config for producer
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.ClientID = "sarama-basic-producer"
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	// Create producer
	producer, err := sarama.NewSyncProducer([]string{addr}, config)
	if err != nil {
		t.Errorf("Failed to create Sarama producer: %v", err)
		return
	}
	defer producer.Close()

	t.Logf("Sarama producer created successfully")

	// Send a message
	msg := &sarama.ProducerMessage{
		Topic: "sarama-basic-topic",
		Key:   sarama.StringEncoder("basic-key"),
		Value: sarama.StringEncoder("Hello from Sarama Basic!"),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		t.Errorf("Failed to send message: %v", err)
		return
	}

	t.Logf("✅ Message sent successfully! Partition: %d, Offset: %d", partition, offset)
}

func testSaramaBasicConsumer(addr string, t *testing.T) {
	// Create Sarama config for consumer
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.ClientID = "sarama-basic-consumer"
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
	partitionConsumer, err := consumer.ConsumePartition("sarama-basic-topic", 0, sarama.OffsetOldest)
	if err != nil {
		t.Errorf("Failed to create partition consumer: %v", err)
		return
	}
	defer partitionConsumer.Close()

	t.Logf("Partition consumer created successfully")

	// Consume one message with timeout
	select {
	case msg := <-partitionConsumer.Messages():
		t.Logf("✅ Consumed message: Key=%s, Value=%s, Offset=%d", string(msg.Key), string(msg.Value), msg.Offset)
	case err := <-partitionConsumer.Errors():
		t.Errorf("Consumer error: %v", err)
	case <-time.After(5 * time.Second):
		t.Errorf("Timeout waiting for message")
	}
}
