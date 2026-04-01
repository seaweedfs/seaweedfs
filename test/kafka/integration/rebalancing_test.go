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

func testSingleConsumerAllPartitions(t *testing.T, addr, topicName, groupID string) {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true

	client, err := sarama.NewClient([]string{addr}, config)
	testutil.AssertNoError(t, err, "Failed to create client")
	defer client.Close()

	consumerGroup, err := sarama.NewConsumerGroupFromClient(groupID, client)
	testutil.AssertNoError(t, err, "Failed to create consumer group")
	defer consumerGroup.Close()

	handler := &RebalanceTestHandler{
		messages:    make(chan *sarama.ConsumerMessage, 20),
		ready:       make(chan bool),
		assignments: make(chan []int32, 5),
		t:           t,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start consumer
	go func() {
		err := consumerGroup.Consume(ctx, []string{topicName}, handler)
		if err != nil && err != context.DeadlineExceeded {
			t.Logf("Consumer error: %v", err)
		}
	}()

	// Wait for consumer to be ready
	<-handler.ready

	// Wait for assignment
	select {
	case partitions := <-handler.assignments:
		t.Logf("Single consumer assigned partitions: %v", partitions)
		if len(partitions) != 4 {
			t.Errorf("Expected single consumer to get all 4 partitions, got %d", len(partitions))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for partition assignment")
	}

	// Consume some messages to verify functionality
	consumedCount := 0
	for consumedCount < 4 { // At least one from each partition
		select {
		case msg := <-handler.messages:
			t.Logf("Consumed message from partition %d: %s", msg.Partition, string(msg.Value))
			consumedCount++
		case <-time.After(5 * time.Second):
			t.Logf("Consumed %d messages so far", consumedCount)
			break
		}
	}

	if consumedCount == 0 {
		t.Error("No messages consumed by single consumer")
	}
}

func testTwoConsumersRebalance(t *testing.T, addr, topicName, groupID string) {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true

	// Start first consumer
	client1, err := sarama.NewClient([]string{addr}, config)
	testutil.AssertNoError(t, err, "Failed to create client1")
	defer client1.Close()

	consumerGroup1, err := sarama.NewConsumerGroupFromClient(groupID, client1)
	testutil.AssertNoError(t, err, "Failed to create consumer group 1")
	defer consumerGroup1.Close()

	handler1 := &RebalanceTestHandler{
		messages:    make(chan *sarama.ConsumerMessage, 20),
		ready:       make(chan bool),
		assignments: make(chan []int32, 5),
		t:           t,
		name:        "Consumer1",
	}

	ctx1, cancel1 := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel1()

	go func() {
		err := consumerGroup1.Consume(ctx1, []string{topicName}, handler1)
		if err != nil && err != context.DeadlineExceeded {
			t.Logf("Consumer1 error: %v", err)
		}
	}()

	// Wait for first consumer to be ready and get initial assignment
	<-handler1.ready
	select {
	case partitions := <-handler1.assignments:
		t.Logf("Consumer1 initial assignment: %v", partitions)
		if len(partitions) != 4 {
			t.Errorf("Expected Consumer1 to initially get all 4 partitions, got %d", len(partitions))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for Consumer1 initial assignment")
	}

	// Start second consumer
	client2, err := sarama.NewClient([]string{addr}, config)
	testutil.AssertNoError(t, err, "Failed to create client2")
	defer client2.Close()

	consumerGroup2, err := sarama.NewConsumerGroupFromClient(groupID, client2)
	testutil.AssertNoError(t, err, "Failed to create consumer group 2")
	defer consumerGroup2.Close()

	handler2 := &RebalanceTestHandler{
		messages:    make(chan *sarama.ConsumerMessage, 20),
		ready:       make(chan bool),
		assignments: make(chan []int32, 5),
		t:           t,
		name:        "Consumer2",
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel2()

	go func() {
		err := consumerGroup2.Consume(ctx2, []string{topicName}, handler2)
		if err != nil && err != context.DeadlineExceeded {
			t.Logf("Consumer2 error: %v", err)
		}
	}()

	// Wait for second consumer to be ready
	<-handler2.ready

	// Wait for rebalancing to occur - both consumers should get new assignments
	var rebalancedAssignment1, rebalancedAssignment2 []int32

	// Consumer1 should get a rebalance assignment
	select {
	case partitions := <-handler1.assignments:
		rebalancedAssignment1 = partitions
		t.Logf("Consumer1 rebalanced assignment: %v", partitions)
	case <-time.After(15 * time.Second):
		t.Error("Timeout waiting for Consumer1 rebalance assignment")
	}

	// Consumer2 should get its assignment
	select {
	case partitions := <-handler2.assignments:
		rebalancedAssignment2 = partitions
		t.Logf("Consumer2 assignment: %v", partitions)
	case <-time.After(15 * time.Second):
		t.Error("Timeout waiting for Consumer2 assignment")
	}

	// Verify rebalancing occurred correctly
	totalPartitions := len(rebalancedAssignment1) + len(rebalancedAssignment2)
	if totalPartitions != 4 {
		t.Errorf("Expected total of 4 partitions assigned, got %d", totalPartitions)
	}

	// Each consumer should have at least 1 partition, and no more than 3
	if len(rebalancedAssignment1) == 0 || len(rebalancedAssignment1) > 3 {
		t.Errorf("Consumer1 should have 1-3 partitions, got %d", len(rebalancedAssignment1))
	}
	if len(rebalancedAssignment2) == 0 || len(rebalancedAssignment2) > 3 {
		t.Errorf("Consumer2 should have 1-3 partitions, got %d", len(rebalancedAssignment2))
	}

	// Verify no partition overlap
	partitionSet := make(map[int32]bool)
	for _, p := range rebalancedAssignment1 {
		if partitionSet[p] {
			t.Errorf("Partition %d assigned to multiple consumers", p)
		}
		partitionSet[p] = true
	}
	for _, p := range rebalancedAssignment2 {
		if partitionSet[p] {
			t.Errorf("Partition %d assigned to multiple consumers", p)
		}
		partitionSet[p] = true
	}

	t.Logf("Rebalancing test completed successfully")
}

func testConsumerLeaveRebalance(t *testing.T, addr, topicName, groupID string) {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true

	// Start two consumers
	client1, err := sarama.NewClient([]string{addr}, config)
	testutil.AssertNoError(t, err, "Failed to create client1")
	defer client1.Close()

	client2, err := sarama.NewClient([]string{addr}, config)
	testutil.AssertNoError(t, err, "Failed to create client2")
	defer client2.Close()

	consumerGroup1, err := sarama.NewConsumerGroupFromClient(groupID, client1)
	testutil.AssertNoError(t, err, "Failed to create consumer group 1")
	defer consumerGroup1.Close()

	consumerGroup2, err := sarama.NewConsumerGroupFromClient(groupID, client2)
	testutil.AssertNoError(t, err, "Failed to create consumer group 2")

	handler1 := &RebalanceTestHandler{
		messages:    make(chan *sarama.ConsumerMessage, 20),
		ready:       make(chan bool),
		assignments: make(chan []int32, 5),
		t:           t,
		name:        "Consumer1",
	}

	handler2 := &RebalanceTestHandler{
		messages:    make(chan *sarama.ConsumerMessage, 20),
		ready:       make(chan bool),
		assignments: make(chan []int32, 5),
		t:           t,
		name:        "Consumer2",
	}

	ctx1, cancel1 := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel1()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Second)

	// Start both consumers
	go func() {
		err := consumerGroup1.Consume(ctx1, []string{topicName}, handler1)
		if err != nil && err != context.DeadlineExceeded {
			t.Logf("Consumer1 error: %v", err)
		}
	}()

	go func() {
		err := consumerGroup2.Consume(ctx2, []string{topicName}, handler2)
		if err != nil && err != context.DeadlineExceeded {
			t.Logf("Consumer2 error: %v", err)
		}
	}()

	// Wait for both consumers to be ready
	<-handler1.ready
	<-handler2.ready

	// Wait for initial assignments
	<-handler1.assignments
	<-handler2.assignments

	t.Logf("Both consumers started, now stopping Consumer2")

	// Stop second consumer (simulate leave)
	cancel2()
	consumerGroup2.Close()

	// Wait for Consumer1 to get rebalanced assignment (should get all partitions)
	select {
	case partitions := <-handler1.assignments:
		t.Logf("Consumer1 rebalanced assignment after Consumer2 left: %v", partitions)
		if len(partitions) != 4 {
			t.Errorf("Expected Consumer1 to get all 4 partitions after Consumer2 left, got %d", len(partitions))
		}
	case <-time.After(20 * time.Second):
		t.Error("Timeout waiting for Consumer1 rebalance after Consumer2 left")
	}

	t.Logf("Consumer leave rebalancing test completed successfully")
}

func testMultipleConsumersJoin(t *testing.T, addr, topicName, groupID string) {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true

	numConsumers := 4
	consumers := make([]sarama.ConsumerGroup, numConsumers)
	clients := make([]sarama.Client, numConsumers)
	handlers := make([]*RebalanceTestHandler, numConsumers)
	contexts := make([]context.Context, numConsumers)
	cancels := make([]context.CancelFunc, numConsumers)

	// Start all consumers simultaneously
	for i := 0; i < numConsumers; i++ {
		client, err := sarama.NewClient([]string{addr}, config)
		testutil.AssertNoError(t, err, fmt.Sprintf("Failed to create client%d", i))
		clients[i] = client

		consumerGroup, err := sarama.NewConsumerGroupFromClient(groupID, client)
		testutil.AssertNoError(t, err, fmt.Sprintf("Failed to create consumer group %d", i))
		consumers[i] = consumerGroup

		handlers[i] = &RebalanceTestHandler{
			messages:    make(chan *sarama.ConsumerMessage, 20),
			ready:       make(chan bool),
			assignments: make(chan []int32, 5),
			t:           t,
			name:        fmt.Sprintf("Consumer%d", i),
		}

		contexts[i], cancels[i] = context.WithTimeout(context.Background(), 45*time.Second)

		go func(idx int) {
			err := consumers[idx].Consume(contexts[idx], []string{topicName}, handlers[idx])
			if err != nil && err != context.DeadlineExceeded {
				t.Logf("Consumer%d error: %v", idx, err)
			}
		}(i)
	}

	// Cleanup
	defer func() {
		for i := 0; i < numConsumers; i++ {
			cancels[i]()
			consumers[i].Close()
			clients[i].Close()
		}
	}()

	// Wait for all consumers to be ready
	for i := 0; i < numConsumers; i++ {
		select {
		case <-handlers[i].ready:
			t.Logf("Consumer%d ready", i)
		case <-time.After(15 * time.Second):
			t.Fatalf("Timeout waiting for Consumer%d to be ready", i)
		}
	}

	// Collect final assignments from all consumers
	assignments := make([][]int32, numConsumers)
	for i := 0; i < numConsumers; i++ {
		select {
		case partitions := <-handlers[i].assignments:
			assignments[i] = partitions
			t.Logf("Consumer%d final assignment: %v", i, partitions)
		case <-time.After(20 * time.Second):
			t.Errorf("Timeout waiting for Consumer%d assignment", i)
		}
	}

	// Verify all partitions are assigned exactly once
	assignedPartitions := make(map[int32]int)
	totalAssigned := 0
	for i, assignment := range assignments {
		totalAssigned += len(assignment)
		for _, partition := range assignment {
			assignedPartitions[partition]++
			if assignedPartitions[partition] > 1 {
				t.Errorf("Partition %d assigned to multiple consumers", partition)
			}
		}

		// Each consumer should get exactly 1 partition (4 partitions / 4 consumers)
		if len(assignment) != 1 {
			t.Errorf("Consumer%d should get exactly 1 partition, got %d", i, len(assignment))
		}
	}

	if totalAssigned != 4 {
		t.Errorf("Expected 4 total partitions assigned, got %d", totalAssigned)
	}

	// Verify all partitions 0-3 are assigned
	for i := int32(0); i < 4; i++ {
		if assignedPartitions[i] != 1 {
			t.Errorf("Partition %d assigned %d times, expected 1", i, assignedPartitions[i])
		}
	}

	t.Logf("Multiple consumers join test completed successfully")
}

// RebalanceTestHandler implements sarama.ConsumerGroupHandler with rebalancing awareness
type RebalanceTestHandler struct {
	messages    chan *sarama.ConsumerMessage
	ready       chan bool
	assignments chan []int32
	readyOnce   sync.Once
	t           *testing.T
	name        string
}

func (h *RebalanceTestHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.t.Logf("%s: Consumer group session setup", h.name)
	h.readyOnce.Do(func() {
		close(h.ready)
	})

	// Send partition assignment
	partitions := make([]int32, 0)
	for topic, partitionList := range session.Claims() {
		h.t.Logf("%s: Assigned topic %s with partitions %v", h.name, topic, partitionList)
		for _, partition := range partitionList {
			partitions = append(partitions, partition)
		}
	}

	select {
	case h.assignments <- partitions:
	default:
		// Channel might be full, that's ok
	}

	return nil
}

func (h *RebalanceTestHandler) Cleanup(sarama.ConsumerGroupSession) error {
	h.t.Logf("%s: Consumer group session cleanup", h.name)
	return nil
}

func (h *RebalanceTestHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}
			h.t.Logf("%s: Received message from partition %d: %s", h.name, message.Partition, string(message.Value))
			select {
			case h.messages <- message:
			default:
				// Channel full, drop message for test
			}
			session.MarkMessage(message, "")
		case <-session.Context().Done():
			return nil
		}
	}
}
