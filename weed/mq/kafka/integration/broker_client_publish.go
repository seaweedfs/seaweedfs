package integration

import (
	"context"
	"fmt"

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

	if len(dataMsg.Value) > 0 {
	} else {
	}
	if err := session.Stream.Send(&mq_pb.PublishMessageRequest{
		Message: &mq_pb.PublishMessageRequest_Data{
			Data: dataMsg,
		},
	}); err != nil {
		return 0, fmt.Errorf("failed to send data: %v", err)
	}

	// Read acknowledgment
	resp, err := session.Stream.Recv()
	if err != nil {
		return 0, fmt.Errorf("failed to receive ack: %v", err)
	}

	if topic == "_schemas" {
		glog.V(3).Infof("[GATEWAY RECV] topic=%s partition=%d resp.AssignedOffset=%d resp.AckTsNs=%d", 
			topic, partition, resp.AssignedOffset, resp.AckTsNs)
	}

	// Handle structured broker errors
	if kafkaErrorCode, errorMsg, handleErr := HandleBrokerResponse(resp); handleErr != nil {
		return 0, handleErr
	} else if kafkaErrorCode != 0 {
		// Return error with Kafka error code information for better debugging
		return 0, fmt.Errorf("broker error (Kafka code %d): %s", kafkaErrorCode, errorMsg)
	}

	// Use the assigned offset from SMQ, not the timestamp
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

	// Create new publisher stream
	bc.publishersLock.Lock()
	defer bc.publishersLock.Unlock()

	// Double-check after acquiring write lock
	if session, exists := bc.publishers[key]; exists {
		return session, nil
	}

	// Create the stream
	stream, err := bc.client.PublishMessage(bc.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create publish stream: %v", err)
	}

	// Get the actual partition assignment from the broker instead of using Kafka partition mapping
	actualPartition, err := bc.getActualPartitionAssignment(topic, partition)
	if err != nil {
		return nil, fmt.Errorf("failed to get actual partition assignment: %v", err)
	}

	// Send init message using the actual partition structure that the broker allocated
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

	// CRITICAL: Consume the "hello" message sent by broker after init
	// Broker sends empty PublishMessageResponse{} on line 137 of broker_grpc_pub.go
	// Without this, first Recv() in PublishRecord gets hello instead of data ack
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

	bc.publishers[key] = session
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
func (bc *BrokerClient) getActualPartitionAssignment(topic string, kafkaPartition int32) (*schema_pb.Partition, error) {
	// Look up the topic configuration from the broker to get the actual partition assignments
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

	totalPartitions := int32(len(lookupResp.BrokerPartitionAssignments))
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
	for _, assignment := range lookupResp.BrokerPartitionAssignments {
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
	for i, assignment := range lookupResp.BrokerPartitionAssignments {
		if assignment.Partition != nil {
			glog.Warningf("  Assignment[%d]: {RangeStart: %d, RangeStop: %d, RingSize: %d}",
				i, assignment.Partition.RangeStart, assignment.Partition.RangeStop, assignment.Partition.RingSize)
		}
	}

	return nil, fmt.Errorf("no broker assignment found for Kafka partition %d with expected range [%d, %d]",
		kafkaPartition, expectedRangeStart, expectedRangeStop)
}
