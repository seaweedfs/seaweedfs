package broker

import (
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/offset"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// BrokerOffsetManager manages offset assignment for all partitions in a broker
type BrokerOffsetManager struct {
	mu                   sync.RWMutex
	offsetIntegration    *offset.SMQOffsetIntegration
	storage              offset.OffsetStorage
	consumerGroupStorage offset.ConsumerGroupOffsetStorage
}

// NewBrokerOffsetManagerWithFilerAccessor creates a new broker offset manager using existing filer client accessor
func NewBrokerOffsetManagerWithFilerAccessor(filerAccessor *filer_client.FilerClientAccessor) *BrokerOffsetManager {
	// Create filer storage using the accessor directly - no duplicate connection management
	filerStorage := offset.NewFilerOffsetStorageWithAccessor(filerAccessor)

	// Create consumer group storage using the accessor directly
	consumerGroupStorage := offset.NewFilerConsumerGroupOffsetStorageWithAccessor(filerAccessor)

	return &BrokerOffsetManager{
		offsetIntegration:    offset.NewSMQOffsetIntegration(filerStorage),
		storage:              filerStorage,
		consumerGroupStorage: consumerGroupStorage,
	}
}

// AssignOffset assigns the next offset for a partition
func (bom *BrokerOffsetManager) AssignOffset(t topic.Topic, p topic.Partition) (int64, error) {
	partition := topicPartitionToSchemaPartition(t, p)

	// Use the integration layer's offset assigner to ensure consistency with subscriptions
	result := bom.offsetIntegration.AssignSingleOffset(t.Namespace, t.Name, partition)
	if result.Error != nil {
		return 0, result.Error
	}

	return result.Assignment.Offset, nil
}

// AssignBatchOffsets assigns a batch of offsets for a partition
func (bom *BrokerOffsetManager) AssignBatchOffsets(t topic.Topic, p topic.Partition, count int64) (baseOffset, lastOffset int64, err error) {
	partition := topicPartitionToSchemaPartition(t, p)

	// Use the integration layer's offset assigner to ensure consistency with subscriptions
	result := bom.offsetIntegration.AssignBatchOffsets(t.Namespace, t.Name, partition, count)
	if result.Error != nil {
		return 0, 0, result.Error
	}

	return result.Batch.BaseOffset, result.Batch.LastOffset, nil
}

// GetHighWaterMark returns the high water mark for a partition
func (bom *BrokerOffsetManager) GetHighWaterMark(t topic.Topic, p topic.Partition) (int64, error) {
	partition := topicPartitionToSchemaPartition(t, p)

	// Use the integration layer's offset assigner to ensure consistency with subscriptions
	return bom.offsetIntegration.GetHighWaterMark(t.Namespace, t.Name, partition)
}

// CreateSubscription creates an offset-based subscription
func (bom *BrokerOffsetManager) CreateSubscription(
	subscriptionID string,
	t topic.Topic,
	p topic.Partition,
	offsetType schema_pb.OffsetType,
	startOffset int64,
) (*offset.OffsetSubscription, error) {
	partition := topicPartitionToSchemaPartition(t, p)
	return bom.offsetIntegration.CreateSubscription(subscriptionID, t.Namespace, t.Name, partition, offsetType, startOffset)
}

// GetSubscription retrieves an existing subscription
func (bom *BrokerOffsetManager) GetSubscription(subscriptionID string) (*offset.OffsetSubscription, error) {
	return bom.offsetIntegration.GetSubscription(subscriptionID)
}

// CloseSubscription closes a subscription
func (bom *BrokerOffsetManager) CloseSubscription(subscriptionID string) error {
	return bom.offsetIntegration.CloseSubscription(subscriptionID)
}

// ListActiveSubscriptions returns all active subscriptions
func (bom *BrokerOffsetManager) ListActiveSubscriptions() ([]*offset.OffsetSubscription, error) {
	return bom.offsetIntegration.ListActiveSubscriptions()
}

// GetPartitionOffsetInfo returns comprehensive offset information for a partition
func (bom *BrokerOffsetManager) GetPartitionOffsetInfo(t topic.Topic, p topic.Partition) (*offset.PartitionOffsetInfo, error) {
	partition := topicPartitionToSchemaPartition(t, p)

	// Use the integration layer to ensure consistency with subscriptions
	return bom.offsetIntegration.GetPartitionOffsetInfo(t.Namespace, t.Name, partition)
}

// topicPartitionToSchemaPartition converts topic.Topic and topic.Partition to schema_pb.Partition
func topicPartitionToSchemaPartition(t topic.Topic, p topic.Partition) *schema_pb.Partition {
	return &schema_pb.Partition{
		RingSize:   int32(p.RingSize),
		RangeStart: int32(p.RangeStart),
		RangeStop:  int32(p.RangeStop),
		UnixTimeNs: p.UnixTimeNs,
	}
}

// OffsetAssignmentResult contains the result of offset assignment for logging/metrics
type OffsetAssignmentResult struct {
	Topic      topic.Topic
	Partition  topic.Partition
	BaseOffset int64
	LastOffset int64
	Count      int64
	Timestamp  int64
	Error      error
}

// AssignOffsetsWithResult assigns offsets and returns detailed result for logging/metrics
func (bom *BrokerOffsetManager) AssignOffsetsWithResult(t topic.Topic, p topic.Partition, count int64) *OffsetAssignmentResult {
	baseOffset, lastOffset, err := bom.AssignBatchOffsets(t, p, count)

	result := &OffsetAssignmentResult{
		Topic:     t,
		Partition: p,
		Count:     count,
		Error:     err,
	}

	if err == nil {
		result.BaseOffset = baseOffset
		result.LastOffset = lastOffset
		result.Timestamp = time.Now().UnixNano()
	}

	return result
}

// GetOffsetMetrics returns metrics about offset usage across all partitions
func (bom *BrokerOffsetManager) GetOffsetMetrics() *offset.OffsetMetrics {
	// Use the integration layer to ensure consistency with subscriptions
	return bom.offsetIntegration.GetOffsetMetrics()
}

// Shutdown gracefully shuts down the offset manager
func (bom *BrokerOffsetManager) Shutdown() {
	bom.mu.Lock()
	defer bom.mu.Unlock()

	// Reset the underlying storage to ensure clean restart behavior
	// This is important for testing where we want offsets to start from 0 after shutdown
	if bom.storage != nil {
		if resettable, ok := bom.storage.(interface{ Reset() error }); ok {
			resettable.Reset()
		}
	}

	// Reset the integration layer to ensure clean restart behavior
	bom.offsetIntegration.Reset()
}

// Consumer Group Offset Management

// SaveConsumerGroupOffset saves the committed offset for a consumer group
func (bom *BrokerOffsetManager) SaveConsumerGroupOffset(t topic.Topic, p topic.Partition, consumerGroup string, offset int64) error {
	if bom.consumerGroupStorage == nil {
		return fmt.Errorf("consumer group storage not configured")
	}
	return bom.consumerGroupStorage.SaveConsumerGroupOffset(t, p, consumerGroup, offset)
}

// LoadConsumerGroupOffset loads the committed offset for a consumer group
func (bom *BrokerOffsetManager) LoadConsumerGroupOffset(t topic.Topic, p topic.Partition, consumerGroup string) (int64, error) {
	if bom.consumerGroupStorage == nil {
		return -1, fmt.Errorf("consumer group storage not configured")
	}
	return bom.consumerGroupStorage.LoadConsumerGroupOffset(t, p, consumerGroup)
}

// ListConsumerGroups returns all consumer groups for a topic partition
func (bom *BrokerOffsetManager) ListConsumerGroups(t topic.Topic, p topic.Partition) ([]string, error) {
	if bom.consumerGroupStorage == nil {
		return nil, fmt.Errorf("consumer group storage not configured")
	}
	return bom.consumerGroupStorage.ListConsumerGroups(t, p)
}

// DeleteConsumerGroupOffset removes the offset file for a consumer group
func (bom *BrokerOffsetManager) DeleteConsumerGroupOffset(t topic.Topic, p topic.Partition, consumerGroup string) error {
	if bom.consumerGroupStorage == nil {
		return fmt.Errorf("consumer group storage not configured")
	}
	return bom.consumerGroupStorage.DeleteConsumerGroupOffset(t, p, consumerGroup)
}
