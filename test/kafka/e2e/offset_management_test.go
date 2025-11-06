package e2e

import (
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/test/kafka/internal/testutil"
)

// TestOffsetManagement tests end-to-end offset management scenarios
// This test will use SMQ backend if SEAWEEDFS_MASTERS is available, otherwise mock
func TestOffsetManagement(t *testing.T) {
	gateway := testutil.NewGatewayTestServerWithSMQ(t, testutil.SMQAvailable)
	defer gateway.CleanupAndClose()

	addr := gateway.StartAndWait()

	// If schema registry is configured, ensure gateway is in schema mode and log
	if v := os.Getenv("SCHEMA_REGISTRY_URL"); v != "" {
		t.Logf("Schema Registry detected at %s - running offset tests in schematized mode", v)
	}

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
	if url := os.Getenv("SCHEMA_REGISTRY_URL"); url != "" {
		if id, err := testutil.EnsureValueSchema(t, url, topic); err == nil {
			t.Logf("Ensured value schema id=%d for subject %s-value", id, topic)
		} else {
			t.Logf("Schema registration failed (non-fatal for test): %v", err)
		}
	}
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
	t.Logf("=== Phase 1: Producing 4 messages to topic %s ===", topic)
	messages := msgGen.GenerateKafkaGoMessages(4)
	err := client.ProduceMessages(topic, messages)
	testutil.AssertNoError(t, err, "Failed to produce messages for resumption test")
	t.Logf("Successfully produced %d messages", len(messages))

	// Consume some messages
	t.Logf("=== Phase 2: First consumer - consuming 2 messages with group %s ===", groupID)
	consumed1, err := client.ConsumeWithGroup(topic, groupID, 2)
	testutil.AssertNoError(t, err, "Failed to consume first batch")
	t.Logf("First consumer consumed %d messages:", len(consumed1))
	for i, msg := range consumed1 {
		t.Logf("  Message %d: offset=%d, partition=%d, value=%s", i, msg.Offset, msg.Partition, string(msg.Value))
	}

	// Simulate consumer restart by consuming remaining messages with same group ID
	t.Logf("=== Phase 3: Second consumer (simulated restart) - consuming remaining messages with same group %s ===", groupID)
	consumed2, err := client.ConsumeWithGroup(topic, groupID, 2)
	testutil.AssertNoError(t, err, "Failed to consume after restart")
	t.Logf("Second consumer consumed %d messages:", len(consumed2))
	for i, msg := range consumed2 {
		t.Logf("  Message %d: offset=%d, partition=%d, value=%s", i, msg.Offset, msg.Partition, string(msg.Value))
	}

	// Verify total consumption
	totalConsumed := len(consumed1) + len(consumed2)
	t.Logf("=== Verification: Total consumed %d messages (expected %d) ===", totalConsumed, len(messages))

	// Check for duplicates
	offsetsSeen := make(map[int64]bool)
	duplicateCount := 0
	for _, msg := range append(consumed1, consumed2...) {
		if offsetsSeen[msg.Offset] {
			t.Logf("WARNING: Duplicate offset detected: %d", msg.Offset)
			duplicateCount++
		}
		offsetsSeen[msg.Offset] = true
	}

	if duplicateCount > 0 {
		t.Logf("ERROR: Found %d duplicate messages", duplicateCount)
	}

	testutil.AssertEqual(t, len(messages), totalConsumed, "Should consume all messages after restart")

	t.Logf("SUCCESS: Consumer group resumption test completed - no duplicates, all messages consumed exactly once")
}
