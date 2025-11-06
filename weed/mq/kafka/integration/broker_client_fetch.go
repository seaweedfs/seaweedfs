package integration

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// FetchMessagesStateless fetches messages using the Kafka-style stateless FetchMessage RPC
// This is the long-term solution that eliminates all Subscribe loop complexity
//
// Benefits over SubscribeMessage:
// 1. No broker-side session state
// 2. No shared Subscribe loops
// 3. No stream corruption from concurrent seeks
// 4. Simple request/response pattern
// 5. Natural support for concurrent reads
//
// This is how Kafka works - completely stateless per-fetch
func (bc *BrokerClient) FetchMessagesStateless(ctx context.Context, topic string, partition int32, startOffset int64, maxRecords int, consumerGroup string, consumerID string) ([]*SeaweedRecord, error) {
	glog.V(4).Infof("[FETCH-STATELESS] Fetching from %s-%d at offset %d, maxRecords=%d",
		topic, partition, startOffset, maxRecords)

	// Get actual partition assignment from broker
	actualPartition, err := bc.getActualPartitionAssignment(topic, partition)
	if err != nil {
		return nil, fmt.Errorf("failed to get partition assignment: %v", err)
	}

	// Create FetchMessage request
	req := &mq_pb.FetchMessageRequest{
		Topic: &schema_pb.Topic{
			Namespace: "kafka", // Kafka gateway always uses "kafka" namespace
			Name:      topic,
		},
		Partition:     actualPartition,
		StartOffset:   startOffset,
		MaxMessages:   int32(maxRecords),
		MaxBytes:      4 * 1024 * 1024, // 4MB default
		MaxWaitMs:     100,             // 100ms wait for data (long poll)
		MinBytes:      0,               // Return immediately if any data available
		ConsumerGroup: consumerGroup,
		ConsumerId:    consumerID,
	}

	// Get timeout from context (set by Kafka fetch request)
	// This respects the client's MaxWaitTime
	// Note: We use a default of 100ms above, but if context has shorter timeout, use that

	// Call FetchMessage RPC (simple request/response)
	resp, err := bc.client.FetchMessage(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("FetchMessage RPC failed: %v", err)
	}

	// Check for errors in response
	if resp.Error != "" {
		// Check if this is an "offset out of range" error
		if resp.ErrorCode == 2 && resp.LogStartOffset > 0 && startOffset < resp.LogStartOffset {
			// Offset too old - broker suggests starting from LogStartOffset
			glog.V(3).Infof("[FETCH-STATELESS-CLIENT] Requested offset %d too old, adjusting to log start %d",
				startOffset, resp.LogStartOffset)

			// Retry with adjusted offset
			req.StartOffset = resp.LogStartOffset
			resp, err = bc.client.FetchMessage(ctx, req)
			if err != nil {
				return nil, fmt.Errorf("FetchMessage RPC failed on retry: %v", err)
			}
			if resp.Error != "" {
				return nil, fmt.Errorf("broker error on retry: %s (code=%d)", resp.Error, resp.ErrorCode)
			}
			// Continue with adjusted offset response
			startOffset = resp.LogStartOffset
		} else {
			return nil, fmt.Errorf("broker error: %s (code=%d)", resp.Error, resp.ErrorCode)
		}
	}

	// CRITICAL: If broker returns 0 messages but hwm > startOffset, something is wrong
	if len(resp.Messages) == 0 && resp.HighWaterMark > startOffset {
		glog.Errorf("[FETCH-STATELESS-CLIENT] CRITICAL BUG: Broker returned 0 messages for %s[%d] offset %d, but HWM=%d (should have %d messages available)",
			topic, partition, startOffset, resp.HighWaterMark, resp.HighWaterMark-startOffset)
		glog.Errorf("[FETCH-STATELESS-CLIENT] This suggests broker's FetchMessage RPC is not returning data that exists!")
		glog.Errorf("[FETCH-STATELESS-CLIENT] Broker metadata: logStart=%d, nextOffset=%d, endOfPartition=%v",
			resp.LogStartOffset, resp.NextOffset, resp.EndOfPartition)
	}

	// Convert protobuf messages to SeaweedRecord
	records := make([]*SeaweedRecord, 0, len(resp.Messages))
	for i, msg := range resp.Messages {
		record := &SeaweedRecord{
			Key:       msg.Key,
			Value:     msg.Value,
			Timestamp: msg.TsNs,
			Offset:    startOffset + int64(i), // Sequential offset assignment
		}
		records = append(records, record)

		// Log each message for debugging
		glog.V(4).Infof("[FETCH-STATELESS-CLIENT] Message %d: offset=%d, keyLen=%d, valueLen=%d",
			i, record.Offset, len(msg.Key), len(msg.Value))
	}

	if len(records) > 0 {
		glog.V(3).Infof("[FETCH-STATELESS-CLIENT] Converted to %d SeaweedRecords, first offset=%d, last offset=%d",
			len(records), records[0].Offset, records[len(records)-1].Offset)
	} else {
		glog.V(3).Infof("[FETCH-STATELESS-CLIENT] Converted to 0 SeaweedRecords")
	}

	glog.V(4).Infof("[FETCH-STATELESS] Fetched %d records, nextOffset=%d, highWaterMark=%d, endOfPartition=%v",
		len(records), resp.NextOffset, resp.HighWaterMark, resp.EndOfPartition)

	return records, nil
}

// GetPartitionHighWaterMark returns the highest offset available in a partition
// This is useful for Kafka clients to track consumer lag
func (bc *BrokerClient) GetPartitionHighWaterMark(ctx context.Context, topic string, partition int32) (int64, error) {
	// Use FetchMessage with 0 maxRecords to just get metadata
	actualPartition, err := bc.getActualPartitionAssignment(topic, partition)
	if err != nil {
		return 0, fmt.Errorf("failed to get partition assignment: %v", err)
	}

	req := &mq_pb.FetchMessageRequest{
		Topic: &schema_pb.Topic{
			Namespace: "kafka",
			Name:      topic,
		},
		Partition:     actualPartition,
		StartOffset:   0,
		MaxMessages:   0, // Just get metadata
		MaxBytes:      0,
		MaxWaitMs:     0, // Return immediately
		ConsumerGroup: "kafka-metadata",
		ConsumerId:    "hwm-check",
	}

	resp, err := bc.client.FetchMessage(ctx, req)
	if err != nil {
		return 0, fmt.Errorf("FetchMessage RPC failed: %v", err)
	}

	if resp.Error != "" {
		return 0, fmt.Errorf("broker error: %s", resp.Error)
	}

	return resp.HighWaterMark, nil
}

// GetPartitionLogStartOffset returns the earliest offset available in a partition
// This is useful for Kafka clients to know the valid offset range
func (bc *BrokerClient) GetPartitionLogStartOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	actualPartition, err := bc.getActualPartitionAssignment(topic, partition)
	if err != nil {
		return 0, fmt.Errorf("failed to get partition assignment: %v", err)
	}

	req := &mq_pb.FetchMessageRequest{
		Topic: &schema_pb.Topic{
			Namespace: "kafka",
			Name:      topic,
		},
		Partition:     actualPartition,
		StartOffset:   0,
		MaxMessages:   0,
		MaxBytes:      0,
		MaxWaitMs:     0,
		ConsumerGroup: "kafka-metadata",
		ConsumerId:    "lso-check",
	}

	resp, err := bc.client.FetchMessage(ctx, req)
	if err != nil {
		return 0, fmt.Errorf("FetchMessage RPC failed: %v", err)
	}

	if resp.Error != "" {
		return 0, fmt.Errorf("broker error: %s", resp.Error)
	}

	return resp.LogStartOffset, nil
}
