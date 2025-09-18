package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/seaweedfs/seaweedfs/test/kafka/internal/testutil"
)

// TestConsumerGroups tests consumer group functionality
// This test requires SeaweedFS masters to be running and will skip if not available
func TestConsumerGroups(t *testing.T) {
	gateway := testutil.NewGatewayTestServerWithSMQ(t, testutil.SMQRequired)
	defer gateway.CleanupAndClose()

	addr := gateway.StartAndWait()

	t.Logf("Running consumer group tests with SMQ backend for offset persistence")

	t.Run("BasicFunctionality", func(t *testing.T) {
		testConsumerGroupBasicFunctionality(t, addr)
	})

	t.Run("OffsetCommitAndFetch", func(t *testing.T) {
		testConsumerGroupOffsetCommitAndFetch(t, addr)
	})

	t.Run("Rebalancing", func(t *testing.T) {
		testConsumerGroupRebalancing(t, addr)
	})
}

func testConsumerGroupBasicFunctionality(t *testing.T, addr string) {
	topicName := testutil.GenerateUniqueTopicName("consumer-group-basic")
	groupID := testutil.GenerateUniqueGroupID("basic-group")

	client := testutil.NewSaramaClient(t, addr)
	msgGen := testutil.NewMessageGenerator()

	// Create topic and produce messages
	err := client.CreateTopic(topicName, 1, 1)
	testutil.AssertNoError(t, err, "Failed to create topic")

	messages := msgGen.GenerateStringMessages(9) // 3 messages per consumer
	err = client.ProduceMessages(topicName, messages)
	testutil.AssertNoError(t, err, "Failed to produce messages")

	// Test with multiple consumers in the same group
	numConsumers := 3
	handler := &ConsumerGroupHandler{
		messages: make(chan *sarama.ConsumerMessage, len(messages)),
		ready:    make(chan bool),
		t:        t,
	}

	var wg sync.WaitGroup
	consumerErrors := make(chan error, numConsumers)

	for i := 0; i < numConsumers; i++ {
		wg.Add(1)
		go func(consumerID int) {
			defer wg.Done()

			consumerGroup, err := sarama.NewConsumerGroup([]string{addr}, groupID, client.GetConfig())
			if err != nil {
				consumerErrors <- fmt.Errorf("consumer %d: failed to create consumer group: %v", consumerID, err)
				return
			}
			defer consumerGroup.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			err = consumerGroup.Consume(ctx, []string{topicName}, handler)
			if err != nil && err != context.DeadlineExceeded {
				consumerErrors <- fmt.Errorf("consumer %d: consumption error: %v", consumerID, err)
				return
			}
		}(i)
	}

	// Wait for consumers to be ready
	readyCount := 0
	for readyCount < numConsumers {
		select {
		case <-handler.ready:
			readyCount++
		case <-time.After(5 * time.Second):
			t.Fatalf("Timeout waiting for consumers to be ready")
		}
	}

	// Collect consumed messages
	consumedMessages := make([]*sarama.ConsumerMessage, 0, len(messages))
	messageTimeout := time.After(10 * time.Second)

	for len(consumedMessages) < len(messages) {
		select {
		case msg := <-handler.messages:
			consumedMessages = append(consumedMessages, msg)
		case err := <-consumerErrors:
			t.Fatalf("Consumer error: %v", err)
		case <-messageTimeout:
			t.Fatalf("Timeout waiting for messages. Got %d/%d messages", len(consumedMessages), len(messages))
		}
	}

	wg.Wait()

	// Verify all messages were consumed exactly once
	testutil.AssertEqual(t, len(messages), len(consumedMessages), "Message count mismatch")

	// Verify message uniqueness (no duplicates)
	messageKeys := make(map[string]bool)
	for _, msg := range consumedMessages {
		key := string(msg.Key)
		if messageKeys[key] {
			t.Errorf("Duplicate message key: %s", key)
		}
		messageKeys[key] = true
	}
}

func testConsumerGroupOffsetCommitAndFetch(t *testing.T, addr string) {
	topicName := testutil.GenerateUniqueTopicName("offset-commit-test")
	groupID := testutil.GenerateUniqueGroupID("offset-group")

	client := testutil.NewSaramaClient(t, addr)
	msgGen := testutil.NewMessageGenerator()

	// Create topic and produce messages
	err := client.CreateTopic(topicName, 1, 1)
	testutil.AssertNoError(t, err, "Failed to create topic")

	messages := msgGen.GenerateStringMessages(5)
	err = client.ProduceMessages(topicName, messages)
	testutil.AssertNoError(t, err, "Failed to produce messages")

	// First consumer: consume first 3 messages and commit offsets
	handler1 := &OffsetTestHandler{
		messages:  make(chan *sarama.ConsumerMessage, len(messages)),
		ready:     make(chan bool),
		stopAfter: 3,
		t:         t,
	}

	consumerGroup1, err := sarama.NewConsumerGroup([]string{addr}, groupID, client.GetConfig())
	testutil.AssertNoError(t, err, "Failed to create first consumer group")

	ctx1, cancel1 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel1()

	go func() {
		err := consumerGroup1.Consume(ctx1, []string{topicName}, handler1)
		if err != nil && err != context.DeadlineExceeded {
			t.Logf("First consumer error: %v", err)
		}
	}()

	// Wait for first consumer to be ready and consume messages
	<-handler1.ready
	consumedCount := 0
	for consumedCount < 3 {
		select {
		case <-handler1.messages:
			consumedCount++
		case <-time.After(5 * time.Second):
			t.Fatalf("Timeout waiting for first consumer messages")
		}
	}

	consumerGroup1.Close()
	cancel1()
	time.Sleep(500 * time.Millisecond) // Wait for cleanup

	// Stop the first consumer after N messages
	// Allow a brief moment for commit/heartbeat to flush
	time.Sleep(1 * time.Second)

	// Start a second consumer in the same group to verify resumption from committed offset
	handler2 := &OffsetTestHandler{
		messages:  make(chan *sarama.ConsumerMessage, len(messages)),
		ready:     make(chan bool),
		stopAfter: 2,
		t:         t,
	}
	consumerGroup2, err := sarama.NewConsumerGroup([]string{addr}, groupID, client.GetConfig())
	testutil.AssertNoError(t, err, "Failed to create second consumer group")
	defer consumerGroup2.Close()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()

	go func() {
		err := consumerGroup2.Consume(ctx2, []string{topicName}, handler2)
		if err != nil && err != context.DeadlineExceeded {
			t.Logf("Second consumer error: %v", err)
		}
	}()

	// Wait for second consumer and collect remaining messages
	<-handler2.ready
	secondConsumerMessages := make([]*sarama.ConsumerMessage, 0)
	consumedCount = 0
	for consumedCount < 2 {
		select {
		case msg := <-handler2.messages:
			consumedCount++
			secondConsumerMessages = append(secondConsumerMessages, msg)
		case <-time.After(5 * time.Second):
			t.Fatalf("Timeout waiting for second consumer messages. Got %d/2", consumedCount)
		}
	}

	// Verify second consumer started from correct offset
	if len(secondConsumerMessages) > 0 {
		firstMessageOffset := secondConsumerMessages[0].Offset
		if firstMessageOffset < 3 {
			t.Fatalf("Second consumer should start from offset >= 3: got %d", firstMessageOffset)
		}
	}
}

func testConsumerGroupRebalancing(t *testing.T, addr string) {
	topicName := testutil.GenerateUniqueTopicName("rebalancing-test")
	groupID := testutil.GenerateUniqueGroupID("rebalance-group")

	client := testutil.NewSaramaClient(t, addr)
	msgGen := testutil.NewMessageGenerator()

	// Create topic with multiple partitions for rebalancing
	err := client.CreateTopic(topicName, 4, 1) // 4 partitions
	testutil.AssertNoError(t, err, "Failed to create topic")

	// Produce messages to all partitions
	messages := msgGen.GenerateStringMessages(12) // 3 messages per partition
	for i, msg := range messages {
		partition := int32(i % 4)
		err = client.ProduceMessageToPartition(topicName, partition, msg)
		testutil.AssertNoError(t, err, "Failed to produce message")
	}

	t.Logf("Produced %d messages across 4 partitions", len(messages))

	// Test scenario 1: Single consumer gets all partitions
	t.Run("SingleConsumerAllPartitions", func(t *testing.T) {
		testSingleConsumerAllPartitions(t, addr, topicName, groupID+"-single")
	})

	// Test scenario 2: Add second consumer, verify rebalancing
	t.Run("TwoConsumersRebalance", func(t *testing.T) {
		testTwoConsumersRebalance(t, addr, topicName, groupID+"-two")
	})

	// Test scenario 3: Remove consumer, verify rebalancing
	t.Run("ConsumerLeaveRebalance", func(t *testing.T) {
		testConsumerLeaveRebalance(t, addr, topicName, groupID+"-leave")
	})

	// Test scenario 4: Multiple consumers join simultaneously
	t.Run("MultipleConsumersJoin", func(t *testing.T) {
		testMultipleConsumersJoin(t, addr, topicName, groupID+"-multi")
	})
}

// ConsumerGroupHandler implements sarama.ConsumerGroupHandler
type ConsumerGroupHandler struct {
	messages  chan *sarama.ConsumerMessage
	ready     chan bool
	readyOnce sync.Once
	t         *testing.T
}

func (h *ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	h.t.Logf("Consumer group session setup")
	h.readyOnce.Do(func() {
		close(h.ready)
	})
	return nil
}

func (h *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	h.t.Logf("Consumer group session cleanup")
	return nil
}

func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}
			h.messages <- message
			session.MarkMessage(message, "")
		case <-session.Context().Done():
			return nil
		}
	}
}

// OffsetTestHandler implements sarama.ConsumerGroupHandler for offset testing
type OffsetTestHandler struct {
	messages  chan *sarama.ConsumerMessage
	ready     chan bool
	readyOnce sync.Once
	stopAfter int
	consumed  int
	t         *testing.T
}

func (h *OffsetTestHandler) Setup(sarama.ConsumerGroupSession) error {
	h.t.Logf("Offset test consumer setup")
	h.readyOnce.Do(func() {
		close(h.ready)
	})
	return nil
}

func (h *OffsetTestHandler) Cleanup(sarama.ConsumerGroupSession) error {
	h.t.Logf("Offset test consumer cleanup")
	return nil
}

func (h *OffsetTestHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}
			h.consumed++
			h.messages <- message
			session.MarkMessage(message, "")

			// Stop after consuming the specified number of messages
			if h.consumed >= h.stopAfter {
				h.t.Logf("Stopping consumer after %d messages", h.consumed)
				// Ensure commits are flushed before exiting the claim
				session.Commit()
				return nil
			}
		case <-session.Context().Done():
			return nil
		}
	}
}
