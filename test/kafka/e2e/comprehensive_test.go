package e2e

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/test/kafka/internal/testutil"
)

// TestComprehensiveE2E tests complete end-to-end workflows
// This test will use SMQ backend if SEAWEEDFS_MASTERS is available, otherwise mock
func TestComprehensiveE2E(t *testing.T) {
	gateway := testutil.NewGatewayTestServerWithSMQ(t, testutil.SMQAvailable)
	defer gateway.CleanupAndClose()

	addr := gateway.StartAndWait()

	// Log which backend we're using
	if gateway.IsSMQMode() {
		t.Logf("Running comprehensive E2E tests with SMQ backend")
	} else {
		t.Logf("Running comprehensive E2E tests with mock backend")
	}

	// Create topics for different test scenarios
	topics := []string{
		testutil.GenerateUniqueTopicName("e2e-kafka-go"),
		testutil.GenerateUniqueTopicName("e2e-sarama"),
		testutil.GenerateUniqueTopicName("e2e-mixed"),
	}
	gateway.AddTestTopics(topics...)

	t.Run("KafkaGo_to_KafkaGo", func(t *testing.T) {
		testKafkaGoToKafkaGo(t, addr, topics[0])
	})

	t.Run("Sarama_to_Sarama", func(t *testing.T) {
		testSaramaToSarama(t, addr, topics[1])
	})

	t.Run("KafkaGo_to_Sarama", func(t *testing.T) {
		testKafkaGoToSarama(t, addr, topics[2])
	})

	t.Run("Sarama_to_KafkaGo", func(t *testing.T) {
		testSaramaToKafkaGo(t, addr, topics[2])
	})
}

func testKafkaGoToKafkaGo(t *testing.T, addr, topic string) {
	client := testutil.NewKafkaGoClient(t, addr)
	msgGen := testutil.NewMessageGenerator()

	// Generate test messages
	messages := msgGen.GenerateKafkaGoMessages(2)

	// Produce with kafka-go
	err := client.ProduceMessages(topic, messages)
	testutil.AssertNoError(t, err, "kafka-go produce failed")

	// Consume with kafka-go
	consumed, err := client.ConsumeMessages(topic, len(messages))
	testutil.AssertNoError(t, err, "kafka-go consume failed")

	// Validate message content
	err = testutil.ValidateKafkaGoMessageContent(messages, consumed)
	testutil.AssertNoError(t, err, "Message content validation failed")

	t.Logf("kafka-go to kafka-go test PASSED")
}

func testSaramaToSarama(t *testing.T, addr, topic string) {
	client := testutil.NewSaramaClient(t, addr)
	msgGen := testutil.NewMessageGenerator()

	// Generate test messages
	messages := msgGen.GenerateStringMessages(2)

	// Produce with Sarama
	err := client.ProduceMessages(topic, messages)
	testutil.AssertNoError(t, err, "Sarama produce failed")

	// Consume with Sarama
	consumed, err := client.ConsumeMessages(topic, 0, len(messages))
	testutil.AssertNoError(t, err, "Sarama consume failed")

	// Validate message content
	err = testutil.ValidateMessageContent(messages, consumed)
	testutil.AssertNoError(t, err, "Message content validation failed")

	t.Logf("Sarama to Sarama test PASSED")
}

func testKafkaGoToSarama(t *testing.T, addr, topic string) {
	kafkaGoClient := testutil.NewKafkaGoClient(t, addr)
	saramaClient := testutil.NewSaramaClient(t, addr)
	msgGen := testutil.NewMessageGenerator()

	// Produce with kafka-go
	messages := msgGen.GenerateKafkaGoMessages(2)
	err := kafkaGoClient.ProduceMessages(topic, messages)
	testutil.AssertNoError(t, err, "kafka-go produce failed")

	// Consume with Sarama
	consumed, err := saramaClient.ConsumeMessages(topic, 0, len(messages))
	testutil.AssertNoError(t, err, "Sarama consume failed")

	// Validate that we got the expected number of messages
	testutil.AssertEqual(t, len(messages), len(consumed), "Message count mismatch")

	t.Logf("kafka-go to Sarama test PASSED")
}

func testSaramaToKafkaGo(t *testing.T, addr, topic string) {
	kafkaGoClient := testutil.NewKafkaGoClient(t, addr)
	saramaClient := testutil.NewSaramaClient(t, addr)
	msgGen := testutil.NewMessageGenerator()

	// Produce with Sarama
	messages := msgGen.GenerateStringMessages(2)
	err := saramaClient.ProduceMessages(topic, messages)
	testutil.AssertNoError(t, err, "Sarama produce failed")

	// Consume with kafka-go
	consumed, err := kafkaGoClient.ConsumeMessages(topic, len(messages))
	testutil.AssertNoError(t, err, "kafka-go consume failed")

	// Validate that we got the expected number of messages
	testutil.AssertEqual(t, len(messages), len(consumed), "Message count mismatch")

	t.Logf("Sarama to kafka-go test PASSED")
}
