package broker

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

// FetchMessage implements Kafka-style stateless message fetching
// This is the recommended API for Kafka gateway and other stateless clients
//
// Key differences from SubscribeMessage:
// 1. Request/Response pattern (not streaming)
// 2. No session state maintained on broker
// 3. Each request is completely independent
// 4. Safe for concurrent calls at different offsets
// 5. No Subscribe loop cancellation/restart complexity
//
// Design inspired by Kafka's Fetch API:
// - Client manages offset tracking
// - Each fetch is independent
// - No shared state between requests
// - Natural support for concurrent reads
func (b *MessageQueueBroker) FetchMessage(ctx context.Context, req *mq_pb.FetchMessageRequest) (*mq_pb.FetchMessageResponse, error) {
	glog.V(3).Infof("[FetchMessage] CALLED!") // DEBUG: ensure this shows up

	// Validate request
	if req.Topic == nil {
		return nil, fmt.Errorf("missing topic")
	}
	if req.Partition == nil {
		return nil, fmt.Errorf("missing partition")
	}

	t := topic.FromPbTopic(req.Topic)
	partition := topic.FromPbPartition(req.Partition)

	glog.V(3).Infof("[FetchMessage] %s/%s partition=%v offset=%d maxMessages=%d maxBytes=%d consumer=%s/%s",
		t.Namespace, t.Name, partition, req.StartOffset, req.MaxMessages, req.MaxBytes,
		req.ConsumerGroup, req.ConsumerId)

	// Get local partition
	localPartition, err := b.GetOrGenerateLocalPartition(t, partition)
	if err != nil {
		glog.Errorf("[FetchMessage] Failed to get partition: %v", err)
		return &mq_pb.FetchMessageResponse{
			Error:     fmt.Sprintf("partition not found: %v", err),
			ErrorCode: 1,
		}, nil
	}
	if localPartition == nil {
		return &mq_pb.FetchMessageResponse{
			Error:     "partition not found",
			ErrorCode: 1,
		}, nil
	}

	// Set defaults for limits
	maxMessages := int(req.MaxMessages)
	if maxMessages <= 0 {
		maxMessages = 100 // Reasonable default
	}
	if maxMessages > 10000 {
		maxMessages = 10000 // Safety limit
	}

	maxBytes := int(req.MaxBytes)
	if maxBytes <= 0 {
		maxBytes = 4 * 1024 * 1024 // 4MB default
	}
	if maxBytes > 100*1024*1024 {
		maxBytes = 100 * 1024 * 1024 // 100MB safety limit
	}

	// TODO: Long poll support disabled for now (causing timeouts)
	// Check if we should wait for data (long poll support)
	// shouldWait := req.MaxWaitMs > 0
	// if shouldWait {
	// 	// Wait for data to be available (with timeout)
	// 	dataAvailable := localPartition.LogBuffer.WaitForDataWithTimeout(req.StartOffset, int(req.MaxWaitMs))
	// 	if !dataAvailable {
	// 		// Timeout - return empty response
	// 		glog.V(3).Infof("[FetchMessage] Timeout waiting for data at offset %d", req.StartOffset)
	// 		return &mq_pb.FetchMessageResponse{
	// 			Messages:       []*mq_pb.DataMessage{},
	// 			HighWaterMark:  localPartition.LogBuffer.GetHighWaterMark(),
	// 			LogStartOffset: localPartition.LogBuffer.GetLogStartOffset(),
	// 			EndOfPartition: false,
	// 			NextOffset:     req.StartOffset,
	// 		}, nil
	// 	}
	// }

	// Check if disk read function is configured
	if localPartition.LogBuffer.ReadFromDiskFn == nil {
		glog.Errorf("[FetchMessage] LogBuffer.ReadFromDiskFn is nil! This should not happen.")
	} else {
		glog.V(3).Infof("[FetchMessage] LogBuffer.ReadFromDiskFn is configured")
	}

	// Use requested offset directly - let ReadMessagesAtOffset handle disk reads
	requestedOffset := req.StartOffset

	// Read messages from LogBuffer (stateless read)
	logEntries, nextOffset, highWaterMark, endOfPartition, err := localPartition.LogBuffer.ReadMessagesAtOffset(
		requestedOffset,
		maxMessages,
		maxBytes,
	)

	// CRITICAL: Log the result with full details
	if len(logEntries) == 0 && highWaterMark > requestedOffset && err == nil {
		glog.Errorf("[FetchMessage] CRITICAL: ReadMessagesAtOffset returned 0 entries but HWM=%d > requestedOffset=%d (should return data!)",
			highWaterMark, requestedOffset)
		glog.Errorf("[FetchMessage] Details: nextOffset=%d, endOfPartition=%v, bufferStartOffset=%d",
			nextOffset, endOfPartition, localPartition.LogBuffer.GetLogStartOffset())
	}

	if err != nil {
		// Check if this is an "offset out of range" error
		errMsg := err.Error()
		if len(errMsg) > 0 && (len(errMsg) < 20 || errMsg[:20] != "offset") {
			glog.Errorf("[FetchMessage] Read error: %v", err)
		} else {
			// Offset out of range - this is expected when consumer requests old data
			glog.V(3).Infof("[FetchMessage] Offset out of range: %v", err)
		}

		// Return empty response with metadata - let client adjust offset
		return &mq_pb.FetchMessageResponse{
			Messages:       []*mq_pb.DataMessage{},
			HighWaterMark:  highWaterMark,
			LogStartOffset: localPartition.LogBuffer.GetLogStartOffset(),
			EndOfPartition: false,
			NextOffset:     localPartition.LogBuffer.GetLogStartOffset(), // Suggest starting from earliest available
			Error:          errMsg,
			ErrorCode:      2,
		}, nil
	}

	// Convert to protobuf messages
	messages := make([]*mq_pb.DataMessage, 0, len(logEntries))
	for _, entry := range logEntries {
		messages = append(messages, &mq_pb.DataMessage{
			Key:   entry.Key,
			Value: entry.Data,
			TsNs:  entry.TsNs,
		})
	}

	glog.V(4).Infof("[FetchMessage] Returning %d messages, nextOffset=%d, highWaterMark=%d, endOfPartition=%v",
		len(messages), nextOffset, highWaterMark, endOfPartition)

	return &mq_pb.FetchMessageResponse{
		Messages:       messages,
		HighWaterMark:  highWaterMark,
		LogStartOffset: localPartition.LogBuffer.GetLogStartOffset(),
		EndOfPartition: endOfPartition,
		NextOffset:     nextOffset,
	}, nil
}
