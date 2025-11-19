package topic

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// OffsetAssignmentFunc is a function type for assigning offsets to messages
type OffsetAssignmentFunc func() (int64, error)

// PublishWithOffset publishes a message with offset assignment
// This method is used by the Kafka gateway integration for sequential offset assignment
func (p *LocalPartition) PublishWithOffset(message *mq_pb.DataMessage, assignOffsetFn OffsetAssignmentFunc) (int64, error) {
	// Assign offset for this message
	offset, err := assignOffsetFn()
	if err != nil {
		return 0, fmt.Errorf("failed to assign offset: %w", err)
	}

	// Add message to buffer with offset
	err = p.addToBufferWithOffset(message, offset)
	if err != nil {
		return 0, fmt.Errorf("failed to add message to buffer: %w", err)
	}

	// Send to follower if needed (same logic as original Publish)
	if p.publishFolloweMeStream != nil {
		if followErr := p.publishFolloweMeStream.Send(&mq_pb.PublishFollowMeRequest{
			Message: &mq_pb.PublishFollowMeRequest_Data{
				Data: message,
			},
		}); followErr != nil {
			return 0, fmt.Errorf("send to follower %s: %v", p.Follower, followErr)
		}
	} else {
		atomic.StoreInt64(&p.AckTsNs, message.TsNs)
	}

	return offset, nil
}

// addToBufferWithOffset adds a message to the log buffer with a pre-assigned offset
func (p *LocalPartition) addToBufferWithOffset(message *mq_pb.DataMessage, offset int64) error {
	// Ensure we have a timestamp
	processingTsNs := message.TsNs
	if processingTsNs == 0 {
		processingTsNs = time.Now().UnixNano()
	}

	// Build a LogEntry that preserves the assigned sequential offset
	logEntry := &filer_pb.LogEntry{
		TsNs:             processingTsNs,
		PartitionKeyHash: util.HashToInt32(message.Key),
		Data:             message.Value,
		Key:              message.Key,
		Offset:           offset,
	}

	// Add the entry to the buffer in a way that preserves offset on disk and in-memory
	if err := p.LogBuffer.AddLogEntryToBuffer(logEntry); err != nil {
		return fmt.Errorf("failed to add log entry to buffer: %w", err)
	}

	return nil
}

// GetOffsetInfo returns offset information for this partition
// Used for debugging and monitoring partition offset state
func (p *LocalPartition) GetOffsetInfo() map[string]interface{} {
	return map[string]interface{}{
		"partition_ring_size":   p.RingSize,
		"partition_range_start": p.RangeStart,
		"partition_range_stop":  p.RangeStop,
		"partition_unix_time":   p.UnixTimeNs,
		"buffer_name":           p.LogBuffer.GetName(),
		"buffer_offset":         p.LogBuffer.GetOffset(),
	}
}

// OffsetAwarePublisher wraps a LocalPartition with offset assignment capability
type OffsetAwarePublisher struct {
	partition      *LocalPartition
	assignOffsetFn OffsetAssignmentFunc
}

// NewOffsetAwarePublisher creates a new offset-aware publisher
func NewOffsetAwarePublisher(partition *LocalPartition, assignOffsetFn OffsetAssignmentFunc) *OffsetAwarePublisher {
	return &OffsetAwarePublisher{
		partition:      partition,
		assignOffsetFn: assignOffsetFn,
	}
}

// Publish publishes a message with automatic offset assignment
func (oap *OffsetAwarePublisher) Publish(message *mq_pb.DataMessage) error {
	_, err := oap.partition.PublishWithOffset(message, oap.assignOffsetFn)
	return err
}

// GetPartition returns the underlying partition
func (oap *OffsetAwarePublisher) GetPartition() *LocalPartition {
	return oap.partition
}
