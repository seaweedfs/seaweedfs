package kafka

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
)

func TestConsumerGroup_BasicFunctionality(t *testing.T) {
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
	topicName := "consumer-group-test"
	
	// Add topic for testing
	gatewayServer.GetHandler().AddTopicForTesting(topicName, 1)
	groupID := "test-consumer-group"
	numConsumers := 3
	numMessages := 9 // 3 messages per consumer

	// Create Sarama config for consumer group
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true
	config.Producer.Return.Successes = true

	// Produce test messages first
	t.Logf("=== Producing %d test messages ===", numMessages)
	producer, err := sarama.NewSyncProducer([]string{brokerAddr}, config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	for i := 0; i < numMessages; i++ {
		message := &sarama.ProducerMessage{
			Topic: topicName,
			Key:   sarama.StringEncoder(fmt.Sprintf("key-%d", i)),
			Value: sarama.StringEncoder(fmt.Sprintf("Consumer Group Message %d", i+1)),
		}

		partition, offset, err := producer.SendMessage(message)
		if err != nil {
			t.Fatalf("Failed to produce message %d: %v", i, err)
		}
		t.Logf("✅ Produced message %d: partition=%d, offset=%d", i, partition, offset)
	}

	// Create consumer group handler
	handler := &ConsumerGroupHandler{
		messages: make(chan *sarama.ConsumerMessage, numMessages),
		ready:    make(chan bool),
		t:        t,
	}

	// Start multiple consumers in the same group
	t.Logf("=== Starting %d consumers in group '%s' ===", numConsumers, groupID)
	
	var wg sync.WaitGroup
	consumerErrors := make(chan error, numConsumers)
	
	for i := 0; i < numConsumers; i++ {
		wg.Add(1)
		go func(consumerID int) {
			defer wg.Done()
			
			consumerGroup, err := sarama.NewConsumerGroup([]string{brokerAddr}, groupID, config)
			if err != nil {
				consumerErrors <- fmt.Errorf("consumer %d: failed to create consumer group: %v", consumerID, err)
				return
			}
			defer consumerGroup.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			t.Logf("Consumer %d: Starting consumption", consumerID)
			
			// Start consuming
			err = consumerGroup.Consume(ctx, []string{topicName}, handler)
			if err != nil && err != context.DeadlineExceeded {
				consumerErrors <- fmt.Errorf("consumer %d: consumption error: %v", consumerID, err)
				return
			}
			
			t.Logf("Consumer %d: Finished consumption", consumerID)
		}(i)
	}

	// Wait for consumers to be ready
	t.Logf("Waiting for consumers to be ready...")
	readyCount := 0
	for readyCount < numConsumers {
		select {
		case <-handler.ready:
			readyCount++
			t.Logf("Consumer ready: %d/%d", readyCount, numConsumers)
		case <-time.After(5 * time.Second):
			t.Fatalf("Timeout waiting for consumers to be ready")
		}
	}

	// Collect consumed messages
	t.Logf("=== Collecting consumed messages ===")
	consumedMessages := make([]*sarama.ConsumerMessage, 0, numMessages)
	messageTimeout := time.After(10 * time.Second)

	for len(consumedMessages) < numMessages {
		select {
		case msg := <-handler.messages:
			consumedMessages = append(consumedMessages, msg)
			t.Logf("✅ Consumed message %d: key=%s, value=%s, partition=%d, offset=%d",
				len(consumedMessages), string(msg.Key), string(msg.Value), msg.Partition, msg.Offset)
		case err := <-consumerErrors:
			t.Fatalf("Consumer error: %v", err)
		case <-messageTimeout:
			t.Fatalf("Timeout waiting for messages. Got %d/%d messages", len(consumedMessages), numMessages)
		}
	}

	// Wait for all consumers to finish
	wg.Wait()

	// Verify all messages were consumed exactly once
	if len(consumedMessages) != numMessages {
		t.Errorf("Expected %d messages, got %d", numMessages, len(consumedMessages))
	}

	// Verify message uniqueness (no duplicates)
	messageKeys := make(map[string]bool)
	for _, msg := range consumedMessages {
		key := string(msg.Key)
		if messageKeys[key] {
			t.Errorf("Duplicate message key: %s", key)
		}
		messageKeys[key] = true
	}

	// Verify all expected keys were received
	for i := 0; i < numMessages; i++ {
		expectedKey := fmt.Sprintf("key-%d", i)
		if !messageKeys[expectedKey] {
			t.Errorf("Missing message key: %s", expectedKey)
		}
	}

	t.Logf("🎉 SUCCESS: Consumer group test completed with %d messages consumed by %d consumers", 
		len(consumedMessages), numConsumers)
}

// ConsumerGroupHandler implements sarama.ConsumerGroupHandler
type ConsumerGroupHandler struct {
	messages chan *sarama.ConsumerMessage
	ready    chan bool
	t        *testing.T
}

func (h *ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	h.t.Logf("Consumer group session setup")
	close(h.ready)
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

func TestConsumerGroup_OffsetCommitAndFetch(t *testing.T) {
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
	topicName := "offset-commit-test"
	groupID := "offset-test-group"
	numMessages := 5
	
	// Add topic for testing
	gatewayServer.GetHandler().AddTopicForTesting(topicName, 1)

	// Create Sarama config
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true
	config.Producer.Return.Successes = true

	// Produce test messages
	t.Logf("=== Producing %d test messages ===", numMessages)
	producer, err := sarama.NewSyncProducer([]string{brokerAddr}, config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	for i := 0; i < numMessages; i++ {
		message := &sarama.ProducerMessage{
			Topic: topicName,
			Key:   sarama.StringEncoder(fmt.Sprintf("offset-key-%d", i)),
			Value: sarama.StringEncoder(fmt.Sprintf("Offset Test Message %d", i+1)),
		}

		_, _, err := producer.SendMessage(message)
		if err != nil {
			t.Fatalf("Failed to produce message %d: %v", i, err)
		}
	}

	// First consumer: consume first 3 messages and commit offsets
	t.Logf("=== First consumer: consuming first 3 messages ===")
	handler1 := &OffsetTestHandler{
		messages:    make(chan *sarama.ConsumerMessage, numMessages),
		ready:       make(chan bool),
		stopAfter:   3,
		t:           t,
	}

	consumerGroup1, err := sarama.NewConsumerGroup([]string{brokerAddr}, groupID, config)
	if err != nil {
		t.Fatalf("Failed to create first consumer group: %v", err)
	}

	ctx1, cancel1 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel1()

	// Start first consumer
	go func() {
		err := consumerGroup1.Consume(ctx1, []string{topicName}, handler1)
		if err != nil && err != context.DeadlineExceeded {
			t.Logf("First consumer error: %v", err)
		}
	}()

	// Wait for first consumer to be ready
	<-handler1.ready

	// Collect messages from first consumer
	consumedCount := 0
	for consumedCount < 3 {
		select {
		case msg := <-handler1.messages:
			consumedCount++
			t.Logf("✅ First consumer message %d: key=%s, offset=%d", 
				consumedCount, string(msg.Key), msg.Offset)
		case <-time.After(5 * time.Second):
			t.Fatalf("Timeout waiting for first consumer messages")
		}
	}

	// Close first consumer (this should commit offsets)
	consumerGroup1.Close()
	cancel1()

	// Wait a bit for cleanup
	time.Sleep(500 * time.Millisecond)

	// Second consumer: should start from offset 3 (after committed offset)
	t.Logf("=== Second consumer: should resume from offset 3 ===")
	handler2 := &OffsetTestHandler{
		messages:    make(chan *sarama.ConsumerMessage, numMessages),
		ready:       make(chan bool),
		stopAfter:   2, // Should get remaining 2 messages
		t:           t,
	}

	consumerGroup2, err := sarama.NewConsumerGroup([]string{brokerAddr}, groupID, config)
	if err != nil {
		t.Fatalf("Failed to create second consumer group: %v", err)
	}
	defer consumerGroup2.Close()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()

	// Start second consumer
	go func() {
		err := consumerGroup2.Consume(ctx2, []string{topicName}, handler2)
		if err != nil && err != context.DeadlineExceeded {
			t.Logf("Second consumer error: %v", err)
		}
	}()

	// Wait for second consumer to be ready
	<-handler2.ready

	// Collect messages from second consumer
	secondConsumerMessages := make([]*sarama.ConsumerMessage, 0)
	consumedCount = 0
	for consumedCount < 2 {
		select {
		case msg := <-handler2.messages:
			consumedCount++
			secondConsumerMessages = append(secondConsumerMessages, msg)
			t.Logf("✅ Second consumer message %d: key=%s, offset=%d", 
				consumedCount, string(msg.Key), msg.Offset)
		case <-time.After(5 * time.Second):
			t.Fatalf("Timeout waiting for second consumer messages. Got %d/2", consumedCount)
		}
	}

	// Verify second consumer started from correct offset
	if len(secondConsumerMessages) > 0 {
		firstMessageOffset := secondConsumerMessages[0].Offset
		if firstMessageOffset < 3 {
			t.Errorf("Expected second consumer to start from offset >= 3, got %d", firstMessageOffset)
		} else {
			t.Logf("✅ Second consumer correctly resumed from offset %d", firstMessageOffset)
		}
	}

	t.Logf("🎉 SUCCESS: Offset commit/fetch test completed successfully")
}

// OffsetTestHandler implements sarama.ConsumerGroupHandler for offset testing
type OffsetTestHandler struct {
	messages  chan *sarama.ConsumerMessage
	ready     chan bool
	stopAfter int
	consumed  int
	t         *testing.T
}

func (h *OffsetTestHandler) Setup(sarama.ConsumerGroupSession) error {
	h.t.Logf("Offset test consumer setup")
	close(h.ready)
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
				return nil
			}
		case <-session.Context().Done():
			return nil
		}
	}
}

func TestConsumerGroup_Rebalancing(t *testing.T) {
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
	topicName := "rebalance-test"
	groupID := "rebalance-test-group"
	numMessages := 12
	
	// Add topic for testing
	gatewayServer.GetHandler().AddTopicForTesting(topicName, 1)

	// Create Sarama config
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true
	config.Consumer.Group.Session.Timeout = 10 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 3 * time.Second
	config.Producer.Return.Successes = true

	// Produce test messages
	t.Logf("=== Producing %d test messages ===", numMessages)
	producer, err := sarama.NewSyncProducer([]string{brokerAddr}, config)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	for i := 0; i < numMessages; i++ {
		message := &sarama.ProducerMessage{
			Topic: topicName,
			Key:   sarama.StringEncoder(fmt.Sprintf("rebalance-key-%d", i)),
			Value: sarama.StringEncoder(fmt.Sprintf("Rebalance Test Message %d", i+1)),
		}

		_, _, err := producer.SendMessage(message)
		if err != nil {
			t.Fatalf("Failed to produce message %d: %v", i, err)
		}
	}

	// Start with 2 consumers
	t.Logf("=== Starting 2 initial consumers ===")
	
	handler1 := &RebalanceTestHandler{
		messages:   make(chan *sarama.ConsumerMessage, numMessages),
		ready:      make(chan bool),
		rebalanced: make(chan bool, 5),
		consumerID: "consumer-1",
		t:          t,
	}

	handler2 := &RebalanceTestHandler{
		messages:   make(chan *sarama.ConsumerMessage, numMessages),
		ready:      make(chan bool),
		rebalanced: make(chan bool, 5),
		consumerID: "consumer-2",
		t:          t,
	}

	// Start first consumer
	consumerGroup1, err := sarama.NewConsumerGroup([]string{brokerAddr}, groupID, config)
	if err != nil {
		t.Fatalf("Failed to create consumer group 1: %v", err)
	}
	defer consumerGroup1.Close()

	ctx1, cancel1 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel1()

	go func() {
		err := consumerGroup1.Consume(ctx1, []string{topicName}, handler1)
		if err != nil && err != context.DeadlineExceeded {
			t.Logf("Consumer 1 error: %v", err)
		}
	}()

	// Wait for first consumer to be ready
	<-handler1.ready
	t.Logf("Consumer 1 ready")

	// Start second consumer
	consumerGroup2, err := sarama.NewConsumerGroup([]string{brokerAddr}, groupID, config)
	if err != nil {
		t.Fatalf("Failed to create consumer group 2: %v", err)
	}
	defer consumerGroup2.Close()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel2()

	go func() {
		err := consumerGroup2.Consume(ctx2, []string{topicName}, handler2)
		if err != nil && err != context.DeadlineExceeded {
			t.Logf("Consumer 2 error: %v", err)
		}
	}()

	// Wait for second consumer to be ready
	<-handler2.ready
	t.Logf("Consumer 2 ready")

	// Wait for initial rebalancing
	t.Logf("Waiting for initial rebalancing...")
	rebalanceCount := 0
	for rebalanceCount < 2 {
		select {
		case <-handler1.rebalanced:
			rebalanceCount++
			t.Logf("Consumer 1 rebalanced (%d/2)", rebalanceCount)
		case <-handler2.rebalanced:
			rebalanceCount++
			t.Logf("Consumer 2 rebalanced (%d/2)", rebalanceCount)
		case <-time.After(10 * time.Second):
			t.Logf("Warning: Timeout waiting for initial rebalancing")
			break
		}
	}

	// Collect some messages
	t.Logf("=== Collecting messages from 2 consumers ===")
	allMessages := make([]*sarama.ConsumerMessage, 0)
	messageTimeout := time.After(10 * time.Second)
	
	// Collect at least half the messages
	for len(allMessages) < numMessages/2 {
		select {
		case msg := <-handler1.messages:
			allMessages = append(allMessages, msg)
			t.Logf("Consumer 1 message: key=%s, offset=%d", string(msg.Key), msg.Offset)
		case msg := <-handler2.messages:
			allMessages = append(allMessages, msg)
			t.Logf("Consumer 2 message: key=%s, offset=%d", string(msg.Key), msg.Offset)
		case <-messageTimeout:
			break
		}
	}

	t.Logf("Collected %d messages from 2 consumers", len(allMessages))

	// Add a third consumer to trigger rebalancing
	t.Logf("=== Adding third consumer to trigger rebalancing ===")
	
	handler3 := &RebalanceTestHandler{
		messages:   make(chan *sarama.ConsumerMessage, numMessages),
		ready:      make(chan bool),
		rebalanced: make(chan bool, 5),
		consumerID: "consumer-3",
		t:          t,
	}

	consumerGroup3, err := sarama.NewConsumerGroup([]string{brokerAddr}, groupID, config)
	if err != nil {
		t.Fatalf("Failed to create consumer group 3: %v", err)
	}
	defer consumerGroup3.Close()

	ctx3, cancel3 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel3()

	go func() {
		err := consumerGroup3.Consume(ctx3, []string{topicName}, handler3)
		if err != nil && err != context.DeadlineExceeded {
			t.Logf("Consumer 3 error: %v", err)
		}
	}()

	// Wait for third consumer to be ready
	<-handler3.ready
	t.Logf("Consumer 3 ready")

	// Wait for rebalancing after adding third consumer
	t.Logf("Waiting for rebalancing after adding third consumer...")
	rebalanceCount = 0
	rebalanceTimeout := time.After(15 * time.Second)
	
	for rebalanceCount < 3 {
		select {
		case <-handler1.rebalanced:
			rebalanceCount++
			t.Logf("Consumer 1 rebalanced after adding consumer 3 (%d/3)", rebalanceCount)
		case <-handler2.rebalanced:
			rebalanceCount++
			t.Logf("Consumer 2 rebalanced after adding consumer 3 (%d/3)", rebalanceCount)
		case <-handler3.rebalanced:
			rebalanceCount++
			t.Logf("Consumer 3 rebalanced (%d/3)", rebalanceCount)
		case <-rebalanceTimeout:
			t.Logf("Warning: Timeout waiting for rebalancing after adding consumer 3")
			break
		}
	}

	// Collect remaining messages from all 3 consumers
	t.Logf("=== Collecting remaining messages from 3 consumers ===")
	finalTimeout := time.After(10 * time.Second)
	
	for len(allMessages) < numMessages {
		select {
		case msg := <-handler1.messages:
			allMessages = append(allMessages, msg)
			t.Logf("Consumer 1 message: key=%s, offset=%d", string(msg.Key), msg.Offset)
		case msg := <-handler2.messages:
			allMessages = append(allMessages, msg)
			t.Logf("Consumer 2 message: key=%s, offset=%d", string(msg.Key), msg.Offset)
		case msg := <-handler3.messages:
			allMessages = append(allMessages, msg)
			t.Logf("Consumer 3 message: key=%s, offset=%d", string(msg.Key), msg.Offset)
		case <-finalTimeout:
			break
		}
	}

	t.Logf("Final message count: %d/%d", len(allMessages), numMessages)

	// Verify no duplicate messages
	messageKeys := make(map[string]bool)
	duplicates := 0
	for _, msg := range allMessages {
		key := string(msg.Key)
		if messageKeys[key] {
			duplicates++
			t.Logf("Duplicate message key: %s", key)
		}
		messageKeys[key] = true
	}

	if duplicates > 0 {
		t.Errorf("Found %d duplicate messages during rebalancing", duplicates)
	}

	t.Logf("🎉 SUCCESS: Rebalancing test completed. Consumed %d unique messages with %d consumers", 
		len(messageKeys), 3)
}

// RebalanceTestHandler implements sarama.ConsumerGroupHandler for rebalancing tests
type RebalanceTestHandler struct {
	messages   chan *sarama.ConsumerMessage
	ready      chan bool
	rebalanced chan bool
	consumerID string
	t          *testing.T
}

func (h *RebalanceTestHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.t.Logf("%s: Setup - Generation: %d, Claims: %v", 
		h.consumerID, session.GenerationID(), session.Claims())
	
	select {
	case h.rebalanced <- true:
	default:
	}
	
	select {
	case h.ready <- true:
	default:
	}
	
	return nil
}

func (h *RebalanceTestHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	h.t.Logf("%s: Cleanup", h.consumerID)
	return nil
}

func (h *RebalanceTestHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	h.t.Logf("%s: Starting to consume partition %d", h.consumerID, claim.Partition())
	
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
