package integration

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/test/kafka/internal/testutil"
)

// TestClientCompatibility tests compatibility between different Kafka clients
func TestClientCompatibility(t *testing.T) {
	gateway := testutil.NewGatewayTestServer(t, testutil.GatewayOptions{})
	defer gateway.CleanupAndClose()

	addr := gateway.StartAndWait()

	// Add test topics
	topics := []string{
		testutil.GenerateUniqueTopicName("kafka-go-topic"),
		testutil.GenerateUniqueTopicName("sarama-topic"),
		testutil.GenerateUniqueTopicName("mixed-topic"),
	}
	gateway.AddTestTopics(topics...)

	t.Run("KafkaGo_ProduceConsume", func(t *testing.T) {
		testKafkaGoProduceConsume(t, addr, topics[0])
	})

	t.Run("Sarama_ProduceConsume", func(t *testing.T) {
		testSaramaProduceConsume(t, addr, topics[1])
	})

	t.Run("CrossClient_KafkaGoToSarama", func(t *testing.T) {
		testCrossClientKafkaGoToSarama(t, addr, topics[2])
	})

	t.Run("CrossClient_SaramaToKafkaGo", func(t *testing.T) {
		testCrossClientSaramaToKafkaGo(t, addr, topics[2])
	})
}

func testKafkaGoProduceConsume(t *testing.T, addr, topic string) {
	client := testutil.NewKafkaGoClient(t, addr)
	msgGen := testutil.NewMessageGenerator()

	// Generate test messages
	messages := msgGen.GenerateKafkaGoMessages(3)

	// Produce messages
	err := client.ProduceMessages(topic, messages)
	testutil.AssertNoError(t, err, "Failed to produce messages")

	// Consume messages
	consumed, err := client.ConsumeMessages(topic, len(messages))
	testutil.AssertNoError(t, err, "Failed to consume messages")

	// Validate content
	err = testutil.ValidateKafkaGoMessageContent(messages, consumed)
	testutil.AssertNoError(t, err, "Message content validation failed")
}

func testSaramaProduceConsume(t *testing.T, addr, topic string) {
	client := testutil.NewSaramaClient(t, addr)
	msgGen := testutil.NewMessageGenerator()

	// Generate test messages
	messages := msgGen.GenerateStringMessages(3)

	// Produce messages
	err := client.ProduceMessages(topic, messages)
	testutil.AssertNoError(t, err, "Failed to produce messages")

	// Consume messages
	consumed, err := client.ConsumeMessages(topic, 0, len(messages))
	testutil.AssertNoError(t, err, "Failed to consume messages")

	// Validate content
	err = testutil.ValidateMessageContent(messages, consumed)
	testutil.AssertNoError(t, err, "Message content validation failed")
}

func testCrossClientKafkaGoToSarama(t *testing.T, addr, topic string) {
	kafkaGoClient := testutil.NewKafkaGoClient(t, addr)
	saramaClient := testutil.NewSaramaClient(t, addr)
	msgGen := testutil.NewMessageGenerator()

	// Produce with kafka-go
	messages := msgGen.GenerateKafkaGoMessages(2)
	err := kafkaGoClient.ProduceMessages(topic, messages)
	testutil.AssertNoError(t, err, "Failed to produce with kafka-go")

	// Consume with Sarama
	consumed, err := saramaClient.ConsumeMessages(topic, 0, len(messages))
	testutil.AssertNoError(t, err, "Failed to consume with Sarama")

	// Validate that we got the expected number of messages
	testutil.AssertEqual(t, len(messages), len(consumed), "Message count mismatch")
}

func testCrossClientSaramaToKafkaGo(t *testing.T, addr, topic string) {
	kafkaGoClient := testutil.NewKafkaGoClient(t, addr)
	saramaClient := testutil.NewSaramaClient(t, addr)
	msgGen := testutil.NewMessageGenerator()

	// Produce with Sarama
	messages := msgGen.GenerateStringMessages(2)
	err := saramaClient.ProduceMessages(topic, messages)
	testutil.AssertNoError(t, err, "Failed to produce with Sarama")

	// Consume with kafka-go
	consumed, err := kafkaGoClient.ConsumeMessages(topic, len(messages))
	testutil.AssertNoError(t, err, "Failed to consume with kafka-go")

	// Validate that we got the expected number of messages
	testutil.AssertEqual(t, len(messages), len(consumed), "Message count mismatch")
}
