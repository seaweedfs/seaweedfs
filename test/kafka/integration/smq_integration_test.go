package integration

import (
	"context"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/seaweedfs/seaweedfs/test/kafka/internal/testutil"
)

// TestSMQIntegration tests that the Kafka gateway properly integrates with SeaweedMQ
// This test REQUIRES SeaweedFS masters to be running and will skip if not available
func TestSMQIntegration(t *testing.T) {
	// This test requires SMQ to be available
	gateway := testutil.NewGatewayTestServerWithSMQ(t, testutil.SMQRequired)
	defer gateway.CleanupAndClose()

	addr := gateway.StartAndWait()
	
	t.Logf("Running SMQ integration test with SeaweedFS backend")

	t.Run("ProduceConsumeWithPersistence", func(t *testing.T) {
		testProduceConsumeWithPersistence(t, addr)
	})

	t.Run("ConsumerGroupOffsetPersistence", func(t *testing.T) {
		testConsumerGroupOffsetPersistence(t, addr)
	})

	t.Run("TopicPersistence", func(t *testing.T) {
		testTopicPersistence(t, addr)
	})
}

func testProduceConsumeWithPersistence(t *testing.T, addr string) {
	topicName := testutil.GenerateUniqueTopicName("smq-integration-produce-consume")
	
	client := testutil.NewSaramaClient(t, addr)
	msgGen := testutil.NewMessageGenerator()

	// Create topic
	err := client.CreateTopic(topicName, 1, 1)
	testutil.AssertNoError(t, err, "Failed to create topic")

	// Produce messages
	messages := msgGen.GenerateStringMessages(5)
	err = client.ProduceMessages(topicName, messages)
	testutil.AssertNoError(t, err, "Failed to produce messages")

	t.Logf("Produced %d messages to topic %s", len(messages), topicName)

	// Consume messages
	consumed, err := client.ConsumeMessages(topicName, 0, len(messages))
	testutil.AssertNoError(t, err, "Failed to consume messages")

	// Verify all messages were consumed
	testutil.AssertEqual(t, len(messages), len(consumed), "Message count mismatch")

	t.Logf("Successfully consumed %d messages from SMQ backend", len(consumed))
}

func testConsumerGroupOffsetPersistence(t *testing.T, addr string) {
	topicName := testutil.GenerateUniqueTopicName("smq-integration-offset-persistence")
	groupID := testutil.GenerateUniqueGroupID("smq-offset-group")
	
	client := testutil.NewSaramaClient(t, addr)
	msgGen := testutil.NewMessageGenerator()

	// Create topic and produce messages
	err := client.CreateTopic(topicName, 1, 1)
	testutil.AssertNoError(t, err, "Failed to create topic")

	messages := msgGen.GenerateStringMessages(10)
	err = client.ProduceMessages(topicName, messages)
	testutil.AssertNoError(t, err, "Failed to produce messages")

	// Phase 1: Consume first 5 messages with consumer group and commit offsets
	t.Logf("Phase 1: Consuming first 5 messages and committing offsets")
	
	config := client.GetConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	
	consumerGroup1, err := sarama.NewConsumerGroup([]string{addr}, groupID, config)
	testutil.AssertNoError(t, err, "Failed to create first consumer group")

	handler := &SMQOffsetTestHandler{
		messages:  make(chan *sarama.ConsumerMessage, len(messages)),
		ready:     make(chan bool),
		stopAfter: 5,
		t:         t,
	}

	ctx1, cancel1 := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel1()

	go func() {
		err := consumerGroup1.Consume(ctx1, []string{topicName}, handler)
		if err != nil && err != context.DeadlineExceeded {
			t.Logf("First consumer error: %v", err)
		}
	}()

	// Wait for consumer to be ready and consume messages
	<-handler.ready
	consumedCount := 0
	for consumedCount < 5 {
		select {
		case <-handler.messages:
			consumedCount++
		case <-time.After(10 * time.Second):
			t.Fatalf("Timeout waiting for first batch of messages")
		}
	}

	consumerGroup1.Close()
	cancel1()
	time.Sleep(2 * time.Second) // Allow offset commits to be processed by SMQ

	t.Logf("Consumed %d messages in first phase", consumedCount)

	// Phase 2: Start new consumer group with same ID - should resume from committed offset
	t.Logf("Phase 2: Starting new consumer group to test offset persistence")
	
	consumerGroup2, err := sarama.NewConsumerGroup([]string{addr}, groupID, config)
	testutil.AssertNoError(t, err, "Failed to create second consumer group")
	defer consumerGroup2.Close()

	handler2 := &SMQOffsetTestHandler{
		messages:  make(chan *sarama.ConsumerMessage, len(messages)),
		ready:     make(chan bool),
		stopAfter: 5, // Should consume remaining 5 messages
		t:         t,
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 15*time.Second)
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
	for consumedCount < 5 {
		select {
		case msg := <-handler2.messages:
			consumedCount++
			secondConsumerMessages = append(secondConsumerMessages, msg)
		case <-time.After(10 * time.Second):
			t.Fatalf("Timeout waiting for second batch of messages. Got %d/5", consumedCount)
		}
	}

	// Verify second consumer started from correct offset (should be >= 5)
	if len(secondConsumerMessages) > 0 {
		firstMessageOffset := secondConsumerMessages[0].Offset
		if firstMessageOffset < 5 {
			t.Fatalf("Second consumer should start from offset >= 5: got %d", firstMessageOffset)
		}
		t.Logf("Second consumer correctly resumed from offset %d", firstMessageOffset)
	}

	t.Logf("Successfully verified SMQ offset persistence")
}

func testTopicPersistence(t *testing.T, addr string) {
	topicName := testutil.GenerateUniqueTopicName("smq-integration-topic-persistence")
	
	client := testutil.NewSaramaClient(t, addr)

	// Create topic
	err := client.CreateTopic(topicName, 2, 1) // 2 partitions
	testutil.AssertNoError(t, err, "Failed to create topic")

	// Verify topic exists by listing topics using admin client
	admin, err := sarama.NewClusterAdmin([]string{addr}, client.GetConfig())
	testutil.AssertNoError(t, err, "Failed to create admin client")
	defer admin.Close()

	topics, err := admin.ListTopics()
	testutil.AssertNoError(t, err, "Failed to list topics")

	topicDetails, exists := topics[topicName]
	if !exists {
		t.Fatalf("Topic %s not found in topic list", topicName)
	}

	if topicDetails.NumPartitions != 2 {
		t.Errorf("Expected 2 partitions, got %d", topicDetails.NumPartitions)
	}

	t.Logf("Successfully verified topic persistence with %d partitions", topicDetails.NumPartitions)
}

// SMQOffsetTestHandler implements sarama.ConsumerGroupHandler for SMQ offset testing
type SMQOffsetTestHandler struct {
	messages  chan *sarama.ConsumerMessage
	ready     chan bool
	readyOnce bool
	stopAfter int
	consumed  int
	t         *testing.T
}

func (h *SMQOffsetTestHandler) Setup(sarama.ConsumerGroupSession) error {
	h.t.Logf("SMQ offset test consumer setup")
	if !h.readyOnce {
		close(h.ready)
		h.readyOnce = true
	}
	return nil
}

func (h *SMQOffsetTestHandler) Cleanup(sarama.ConsumerGroupSession) error {
	h.t.Logf("SMQ offset test consumer cleanup")
	return nil
}

func (h *SMQOffsetTestHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
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
				h.t.Logf("Stopping SMQ consumer after %d messages", h.consumed)
				// Ensure commits are flushed before exiting the claim
				session.Commit()
				return nil
			}
		case <-session.Context().Done():
			return nil
		}
	}
}
