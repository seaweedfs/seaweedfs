package integration

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// PublishRecord publishes a single record to SeaweedMQ broker
// ctx controls the publish timeout - if client cancels, publish operation is cancelled
func (bc *BrokerClient) PublishRecord(ctx context.Context, topic string, partition int32, key []byte, value []byte, timestamp int64) (int64, error) {
	// Check context before starting
	if err := ctx.Err(); err != nil {
		return 0, fmt.Errorf("context cancelled before publish: %w", err)
	}

	session, err := bc.getOrCreatePublisher(topic, partition)
	if err != nil {
		return 0, err
	}

	if session.Stream == nil {
		return 0, fmt.Errorf("publisher session stream cannot be nil")
	}

	// CRITICAL: Lock to prevent concurrent Send/Recv causing response mix-ups
	// Without this, two concurrent publishes can steal each other's offsets
	session.mu.Lock()
	defer session.mu.Unlock()

	// Check context after acquiring lock
	if err := ctx.Err(); err != nil {
		return 0, fmt.Errorf("context cancelled after lock: %w", err)
	}

	// Send data message using broker API format
	dataMsg := &mq_pb.DataMessage{
		Key:   key,
		Value: value,
		TsNs:  timestamp,
	}

	// DEBUG: Log message being published for GitHub Actions debugging
	valuePreview := ""
	if len(dataMsg.Value) > 0 {
		if len(dataMsg.Value) <= 50 {
			valuePreview = string(dataMsg.Value)
		} else {
			valuePreview = fmt.Sprintf("%s...(total %d bytes)", string(dataMsg.Value[:50]), len(dataMsg.Value))
		}
	} else {
		valuePreview = "<empty>"
	}
	glog.V(1).Infof("[PUBLISH] topic=%s partition=%d key=%s valueLen=%d valuePreview=%q timestamp=%d",
		topic, partition, string(key), len(value), valuePreview, timestamp)

	// CRITICAL: Use a goroutine with context checking to enforce timeout
	// gRPC streams may not respect context deadlines automatically
	// We need to monitor the context and timeout the operation if needed
	sendErrChan := make(chan error, 1)
	go func() {
		sendErrChan <- session.Stream.Send(&mq_pb.PublishMessageRequest{
			Message: &mq_pb.PublishMessageRequest_Data{
				Data: dataMsg,
			},
		})
	}()

	select {
	case err := <-sendErrChan:
		if err != nil {
			return 0, fmt.Errorf("failed to send data: %v", err)
		}
	case <-ctx.Done():
		return 0, fmt.Errorf("context cancelled while sending: %w", ctx.Err())
	}

	// Read acknowledgment with context timeout enforcement
	recvErrChan := make(chan interface{}, 1)
	go func() {
		resp, err := session.Stream.Recv()
		if err != nil {
			recvErrChan <- err
		} else {
			recvErrChan <- resp
		}
	}()

	var resp *mq_pb.PublishMessageResponse
	select {
	case result := <-recvErrChan:
		if err, isErr := result.(error); isErr {
			return 0, fmt.Errorf("failed to receive ack: %v", err)
		}
		resp = result.(*mq_pb.PublishMessageResponse)
	case <-ctx.Done():
		return 0, fmt.Errorf("context cancelled while receiving: %w", ctx.Err())
	}

	// Handle structured broker errors
	if kafkaErrorCode, errorMsg, handleErr := HandleBrokerResponse(resp); handleErr != nil {
		return 0, handleErr
	} else if kafkaErrorCode != 0 {
		// Return error with Kafka error code information for better debugging
		return 0, fmt.Errorf("broker error (Kafka code %d): %s", kafkaErrorCode, errorMsg)
	}

	// Use the assigned offset from SMQ, not the timestamp
	glog.V(1).Infof("[PUBLISH_ACK] topic=%s partition=%d assignedOffset=%d", topic, partition, resp.AssignedOffset)
	return resp.AssignedOffset, nil
}

// PublishRecordValue publishes a RecordValue message to SeaweedMQ via broker
// ctx controls the publish timeout - if client cancels, publish operation is cancelled
func (bc *BrokerClient) PublishRecordValue(ctx context.Context, topic string, partition int32, key []byte, recordValueBytes []byte, timestamp int64) (int64, error) {
	// Check context before starting
	if err := ctx.Err(); err != nil {
		return 0, fmt.Errorf("context cancelled before publish: %w", err)
	}

	session, err := bc.getOrCreatePublisher(topic, partition)
	if err != nil {
		return 0, err
	}

	if session.Stream == nil {
		return 0, fmt.Errorf("publisher session stream cannot be nil")
	}

	// CRITICAL: Lock to prevent concurrent Send/Recv causing response mix-ups
	session.mu.Lock()
	defer session.mu.Unlock()

	// Check context after acquiring lock
	if err := ctx.Err(); err != nil {
		return 0, fmt.Errorf("context cancelled after lock: %w", err)
	}

	// Send data message with RecordValue in the Value field
	dataMsg := &mq_pb.DataMessage{
		Key:   key,
		Value: recordValueBytes, // This contains the marshaled RecordValue
		TsNs:  timestamp,
	}

	if err := session.Stream.Send(&mq_pb.PublishMessageRequest{
		Message: &mq_pb.PublishMessageRequest_Data{
			Data: dataMsg,
		},
	}); err != nil {
		return 0, fmt.Errorf("failed to send RecordValue data: %v", err)
	}

	// Read acknowledgment
	resp, err := session.Stream.Recv()
	if err != nil {
		return 0, fmt.Errorf("failed to receive RecordValue ack: %v", err)
	}

	// Handle structured broker errors
	if kafkaErrorCode, errorMsg, handleErr := HandleBrokerResponse(resp); handleErr != nil {
		return 0, handleErr
	} else if kafkaErrorCode != 0 {
		// Return error with Kafka error code information for better debugging
		return 0, fmt.Errorf("RecordValue broker error (Kafka code %d): %s", kafkaErrorCode, errorMsg)
	}

	// Use the assigned offset from SMQ, not the timestamp
	return resp.AssignedOffset, nil
}

// getOrCreatePublisher gets or creates a publisher stream for a topic-partition
func (bc *BrokerClient) getOrCreatePublisher(topic string, partition int32) (*BrokerPublisherSession, error) {
	key := fmt.Sprintf("%s-%d", topic, partition)

	// Try to get existing publisher
	bc.publishersLock.RLock()
	if session, exists := bc.publishers[key]; exists {
		bc.publishersLock.RUnlock()
		return session, nil
	}
	bc.publishersLock.RUnlock()

	// CRITICAL FIX: Prevent multiple concurrent attempts to create the same publisher
	// Use a creation lock that is specific to each topic-partition pair
	// This ensures only ONE goroutine tries to create/initialize for each publisher
	if bc.publisherCreationLocks == nil {
		bc.publishersLock.Lock()
		if bc.publisherCreationLocks == nil {
			bc.publisherCreationLocks = make(map[string]*sync.Mutex)
		}
		bc.publishersLock.Unlock()
	}

	bc.publishersLock.RLock()
	creationLock, exists := bc.publisherCreationLocks[key]
	if !exists {
		// Need to create a creation lock for this topic-partition
		bc.publishersLock.RUnlock()
		bc.publishersLock.Lock()
		// Double-check if someone else created it
		if lock, exists := bc.publisherCreationLocks[key]; exists {
			creationLock = lock
		} else {
			creationLock = &sync.Mutex{}
			bc.publisherCreationLocks[key] = creationLock
		}
		bc.publishersLock.Unlock()
	} else {
		bc.publishersLock.RUnlock()
	}

	// Acquire the creation lock - only ONE goroutine will proceed
	creationLock.Lock()
	defer creationLock.Unlock()

	// Double-check if publisher was created while we were waiting for the lock
	bc.publishersLock.RLock()
	if session, exists := bc.publishers[key]; exists {
		bc.publishersLock.RUnlock()
		return session, nil
	}
	bc.publishersLock.RUnlock()

	// Create the stream
	stream, err := bc.client.PublishMessage(bc.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create publish stream: %v", err)
	}

	// Get the actual partition assignment from the broker
	actualPartition, err := bc.getActualPartitionAssignment(topic, partition)
	if err != nil {
		return nil, fmt.Errorf("failed to get actual partition assignment: %v", err)
	}

	// Send init message
	if err := stream.Send(&mq_pb.PublishMessageRequest{
		Message: &mq_pb.PublishMessageRequest_Init{
			Init: &mq_pb.PublishMessageRequest_InitMessage{
				Topic: &schema_pb.Topic{
					Namespace: "kafka",
					Name:      topic,
				},
				Partition:     actualPartition,
				AckInterval:   1,
				PublisherName: "kafka-gateway",
			},
		},
	}); err != nil {
		return nil, fmt.Errorf("failed to send init message: %v", err)
	}

	// Consume the "hello" message sent by broker after init
	helloResp, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("failed to receive hello message: %v", err)
	}
	if helloResp.ErrorCode != 0 {
		return nil, fmt.Errorf("broker init error (code %d): %s", helloResp.ErrorCode, helloResp.Error)
	}

	session := &BrokerPublisherSession{
		Topic:     topic,
		Partition: partition,
		Stream:    stream,
	}

	// Store in the map under the publishersLock
	bc.publishersLock.Lock()
	bc.publishers[key] = session
	bc.publishersLock.Unlock()

	return session, nil
}

// ClosePublisher closes a specific publisher session
func (bc *BrokerClient) ClosePublisher(topic string, partition int32) error {
	key := fmt.Sprintf("%s-%d", topic, partition)

	bc.publishersLock.Lock()
	defer bc.publishersLock.Unlock()

	session, exists := bc.publishers[key]
	if !exists {
		return nil // Already closed or never existed
	}

	if session.Stream != nil {
		session.Stream.CloseSend()
	}
	delete(bc.publishers, key)
	return nil
}

// getActualPartitionAssignment looks up the actual partition assignment from the broker configuration
// Uses cache to avoid expensive LookupTopicBrokers calls on every fetch (13.5% CPU overhead!)
func (bc *BrokerClient) getActualPartitionAssignment(topic string, kafkaPartition int32) (*schema_pb.Partition, error) {
	// Check cache first
	bc.partitionAssignmentCacheMu.RLock()
	if entry, found := bc.partitionAssignmentCache[topic]; found {
		if time.Now().Before(entry.expiresAt) {
			assignments := entry.assignments
			bc.partitionAssignmentCacheMu.RUnlock()
			glog.V(4).Infof("Partition assignment cache HIT for topic %s", topic)
			// Use cached assignments to find partition
			return bc.findPartitionInAssignments(topic, kafkaPartition, assignments)
		}
	}
	bc.partitionAssignmentCacheMu.RUnlock()

	// Cache miss or expired - lookup from broker
	glog.V(4).Infof("Partition assignment cache MISS for topic %s, calling LookupTopicBrokers", topic)
	lookupResp, err := bc.client.LookupTopicBrokers(bc.ctx, &mq_pb.LookupTopicBrokersRequest{
		Topic: &schema_pb.Topic{
			Namespace: "kafka",
			Name:      topic,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to lookup topic brokers: %v", err)
	}

	if len(lookupResp.BrokerPartitionAssignments) == 0 {
		return nil, fmt.Errorf("no partition assignments found for topic %s", topic)
	}

	// Cache the assignments
	bc.partitionAssignmentCacheMu.Lock()
	bc.partitionAssignmentCache[topic] = &partitionAssignmentCacheEntry{
		assignments: lookupResp.BrokerPartitionAssignments,
		expiresAt:   time.Now().Add(bc.partitionAssignmentCacheTTL),
	}
	bc.partitionAssignmentCacheMu.Unlock()
	glog.V(4).Infof("Cached partition assignments for topic %s", topic)

	// Use freshly fetched assignments to find partition
	return bc.findPartitionInAssignments(topic, kafkaPartition, lookupResp.BrokerPartitionAssignments)
}

// findPartitionInAssignments finds the SeaweedFS partition for a given Kafka partition ID
func (bc *BrokerClient) findPartitionInAssignments(topic string, kafkaPartition int32, assignments []*mq_pb.BrokerPartitionAssignment) (*schema_pb.Partition, error) {
	totalPartitions := int32(len(assignments))
	if kafkaPartition >= totalPartitions {
		return nil, fmt.Errorf("kafka partition %d out of range, topic %s has %d partitions",
			kafkaPartition, topic, totalPartitions)
	}

	// Calculate expected range for this Kafka partition based on actual partition count
	// Ring is divided equally among partitions, with last partition getting any remainder
	rangeSize := int32(pub_balancer.MaxPartitionCount) / totalPartitions
	expectedRangeStart := kafkaPartition * rangeSize
	var expectedRangeStop int32

	if kafkaPartition == totalPartitions-1 {
		// Last partition gets the remainder to fill the entire ring
		expectedRangeStop = int32(pub_balancer.MaxPartitionCount)
	} else {
		expectedRangeStop = (kafkaPartition + 1) * rangeSize
	}

	glog.V(2).Infof("Looking for Kafka partition %d in topic %s: expected range [%d, %d] out of %d partitions",
		kafkaPartition, topic, expectedRangeStart, expectedRangeStop, totalPartitions)

	// Find the broker assignment that matches this range
	for _, assignment := range assignments {
		if assignment.Partition == nil {
			continue
		}

		// Check if this assignment's range matches our expected range
		if assignment.Partition.RangeStart == expectedRangeStart && assignment.Partition.RangeStop == expectedRangeStop {
			glog.V(1).Infof("found matching partition assignment for %s[%d]: {RingSize: %d, RangeStart: %d, RangeStop: %d, UnixTimeNs: %d}",
				topic, kafkaPartition, assignment.Partition.RingSize, assignment.Partition.RangeStart,
				assignment.Partition.RangeStop, assignment.Partition.UnixTimeNs)
			return assignment.Partition, nil
		}
	}

	// If no exact match found, log all available assignments for debugging
	glog.Warningf("no partition assignment found for Kafka partition %d in topic %s with expected range [%d, %d]",
		kafkaPartition, topic, expectedRangeStart, expectedRangeStop)
	glog.Warningf("Available assignments:")
	for i, assignment := range assignments {
		if assignment.Partition != nil {
			glog.Warningf("  Assignment[%d]: {RangeStart: %d, RangeStop: %d, RingSize: %d}",
				i, assignment.Partition.RangeStart, assignment.Partition.RangeStop, assignment.Partition.RingSize)
		}
	}

	return nil, fmt.Errorf("no broker assignment found for Kafka partition %d with expected range [%d, %d]",
		kafkaPartition, expectedRangeStart, expectedRangeStop)
}
