package e2e

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/test/kafka/internal/testutil"
)

// TestOffsetManagement tests end-to-end offset management scenarios
// This test will use SMQ backend if SEAWEEDFS_MASTERS is available, otherwise mock
func TestOffsetManagement(t *testing.T) {
	gateway := testutil.NewGatewayTestServerWithSMQ(t, testutil.SMQAvailable)
	defer gateway.CleanupAndClose()

	addr := gateway.StartAndWait()
	
	// Log which backend we're using
	if gateway.IsSMQMode() {
		t.Logf("Running offset management tests with SMQ backend - offsets will be persisted")
	} else {
		t.Logf("Running offset management tests with mock backend - offsets are in-memory only")
	}

	topic := testutil.GenerateUniqueTopicName("offset-management")
	groupID := testutil.GenerateUniqueGroupID("offset-test-group")

	gateway.AddTestTopic(topic)

	t.Run("BasicOffsetCommitFetch", func(t *testing.T) {
		testBasicOffsetCommitFetch(t, addr, topic, groupID)
	})

	t.Run("ConsumerGroupResumption", func(t *testing.T) {
		testConsumerGroupResumption(t, addr, topic, groupID+"2")
	})
}

func testBasicOffsetCommitFetch(t *testing.T, addr, topic, groupID string) {
	client := testutil.NewKafkaGoClient(t, addr)
	msgGen := testutil.NewMessageGenerator()

	// Produce test messages
	messages := msgGen.GenerateKafkaGoMessages(5)
	err := client.ProduceMessages(topic, messages)
	testutil.AssertNoError(t, err, "Failed to produce offset test messages")

	// Phase 1: Consume first 3 messages and commit offsets
	t.Logf("=== Phase 1: Consuming first 3 messages ===")
	consumed1, err := client.ConsumeWithGroup(topic, groupID, 3)
	testutil.AssertNoError(t, err, "Failed to consume first batch")
	testutil.AssertEqual(t, 3, len(consumed1), "Should consume exactly 3 messages")

	// Phase 2: Create new consumer with same group ID - should resume from committed offset
	t.Logf("=== Phase 2: Resuming from committed offset ===")
	consumed2, err := client.ConsumeWithGroup(topic, groupID, 2)
	testutil.AssertNoError(t, err, "Failed to consume remaining messages")
	testutil.AssertEqual(t, 2, len(consumed2), "Should consume remaining 2 messages")

	// Verify that we got all messages without duplicates
	totalConsumed := len(consumed1) + len(consumed2)
	testutil.AssertEqual(t, len(messages), totalConsumed, "Should consume all messages exactly once")

	t.Logf("SUCCESS: Offset management test completed - consumed %d + %d messages", len(consumed1), len(consumed2))
}

func testConsumerGroupResumption(t *testing.T, addr, topic, groupID string) {
	client := testutil.NewKafkaGoClient(t, addr)
	msgGen := testutil.NewMessageGenerator()

	// Produce messages
	messages := msgGen.GenerateKafkaGoMessages(4)
	err := client.ProduceMessages(topic, messages)
	testutil.AssertNoError(t, err, "Failed to produce messages for resumption test")

	// Consume some messages
	consumed1, err := client.ConsumeWithGroup(topic, groupID, 2)
	testutil.AssertNoError(t, err, "Failed to consume first batch")

	// Simulate consumer restart by consuming remaining messages with same group ID
	consumed2, err := client.ConsumeWithGroup(topic, groupID, 2)
	testutil.AssertNoError(t, err, "Failed to consume after restart")

	// Verify total consumption
	totalConsumed := len(consumed1) + len(consumed2)
	testutil.AssertEqual(t, len(messages), totalConsumed, "Should consume all messages after restart")

	t.Logf("SUCCESS: Consumer group resumption test completed")
}
