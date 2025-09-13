package broker

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
	"google.golang.org/protobuf/proto"
)

// OffsetAssignmentFunc is a function type for assigning offsets to messages
type OffsetAssignmentFunc func() (int64, error)

// AddToBufferWithOffset adds a message to the log buffer with offset assignment
// TODO: This is a temporary solution until LogBuffer can be modified to accept offset assignment
// ASSUMPTION: This function will be integrated into LogBuffer.AddToBuffer in the future
func (b *MessageQueueBroker) AddToBufferWithOffset(
	logBuffer *log_buffer.LogBuffer,
	message *mq_pb.DataMessage,
	t topic.Topic,
	p topic.Partition,
) error {
	// Assign offset for this message
	offset, err := b.offsetManager.AssignOffset(t, p)
	if err != nil {
		return err
	}

	// PERFORMANCE OPTIMIZATION: Pre-process expensive operations OUTSIDE the lock
	var ts time.Time
	processingTsNs := message.TsNs
	if processingTsNs == 0 {
		ts = time.Now()
		processingTsNs = ts.UnixNano()
	} else {
		ts = time.Unix(0, processingTsNs)
	}

	// Create LogEntry with assigned offset
	logEntry := &filer_pb.LogEntry{
		TsNs:             processingTsNs,
		PartitionKeyHash: util.HashToInt32(message.Key),
		Data:             message.Value,
		Key:              message.Key,
		Offset:           offset, // Add the assigned offset
	}

	logEntryData, err := proto.Marshal(logEntry)
	if err != nil {
		return err
	}

	// Use the existing LogBuffer infrastructure for the rest
	// TODO: This is a workaround - ideally LogBuffer should handle offset assignment
	// For now, we'll add the message with the pre-assigned offset
	return b.addLogEntryToBuffer(logBuffer, logEntry, logEntryData, ts)
}

// addLogEntryToBuffer adds a pre-constructed LogEntry to the buffer
// This is a helper function that mimics LogBuffer.AddDataToBuffer but with a pre-built LogEntry
func (b *MessageQueueBroker) addLogEntryToBuffer(
	logBuffer *log_buffer.LogBuffer,
	logEntry *filer_pb.LogEntry,
	logEntryData []byte,
	ts time.Time,
) error {
	// TODO: This is a simplified version of LogBuffer.AddDataToBuffer
	// ASSUMPTION: We're bypassing some of the LogBuffer's internal logic
	// This should be properly integrated when LogBuffer is modified

	// Use the new AddLogEntryToBuffer method to preserve offset information
	// This ensures the offset is maintained throughout the entire data flow
	logBuffer.AddLogEntryToBuffer(logEntry)
	return nil
}

// GetPartitionOffsetInfo returns offset information for a partition
func (b *MessageQueueBroker) GetPartitionOffsetInfo(t topic.Topic, p topic.Partition) (*PartitionOffsetInfo, error) {
	info, err := b.offsetManager.GetPartitionOffsetInfo(t, p)
	if err != nil {
		return nil, err
	}

	// Convert to broker-specific format if needed
	return &PartitionOffsetInfo{
		Topic:               t,
		Partition:           p,
		EarliestOffset:      info.EarliestOffset,
		LatestOffset:        info.LatestOffset,
		HighWaterMark:       info.HighWaterMark,
		RecordCount:         info.RecordCount,
		ActiveSubscriptions: info.ActiveSubscriptions,
	}, nil
}

// PartitionOffsetInfo provides offset information for a partition (broker-specific)
type PartitionOffsetInfo struct {
	Topic               topic.Topic
	Partition           topic.Partition
	EarliestOffset      int64
	LatestOffset        int64
	HighWaterMark       int64
	RecordCount         int64
	ActiveSubscriptions int64
}

// CreateOffsetSubscription creates an offset-based subscription through the broker
func (b *MessageQueueBroker) CreateOffsetSubscription(
	subscriptionID string,
	t topic.Topic,
	p topic.Partition,
	offsetType string, // Will be converted to schema_pb.OffsetType
	startOffset int64,
) error {
	// TODO: Convert string offsetType to schema_pb.OffsetType
	// ASSUMPTION: For now using RESET_TO_EARLIEST as default
	// This should be properly mapped based on the offsetType parameter

	_, err := b.offsetManager.CreateSubscription(
		subscriptionID,
		t,
		p,
		0, // schema_pb.OffsetType_RESET_TO_EARLIEST
		startOffset,
	)

	return err
}

// GetOffsetMetrics returns offset metrics for monitoring
func (b *MessageQueueBroker) GetOffsetMetrics() map[string]interface{} {
	metrics := b.offsetManager.GetOffsetMetrics()

	return map[string]interface{}{
		"partition_count":      metrics.PartitionCount,
		"total_offsets":        metrics.TotalOffsets,
		"active_subscriptions": metrics.ActiveSubscriptions,
		"average_latency":      metrics.AverageLatency,
	}
}
