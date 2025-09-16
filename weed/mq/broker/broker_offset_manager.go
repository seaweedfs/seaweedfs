package broker

import (
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/offset"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// BrokerOffsetManager manages offset assignment for all partitions in a broker
type BrokerOffsetManager struct {
	mu                sync.RWMutex
	offsetIntegration *offset.SMQOffsetIntegration
	partitionManagers map[string]*offset.PartitionOffsetManager
	storage           offset.OffsetStorage
}

// NewBrokerOffsetManager creates a new broker offset manager
func NewBrokerOffsetManager() *BrokerOffsetManager {
	return NewBrokerOffsetManagerWithStorage(nil)
}

// NewBrokerOffsetManagerWithStorage creates a new broker offset manager with custom storage
func NewBrokerOffsetManagerWithStorage(storage offset.OffsetStorage) *BrokerOffsetManager {
	// TODO: Add configuration for database path and type
	// ASSUMPTION: Using in-memory storage as fallback, SQL storage preferred when available
	if storage == nil {
		storage = offset.NewInMemoryOffsetStorage()
	}

	return &BrokerOffsetManager{
		offsetIntegration: offset.NewSMQOffsetIntegration(storage),
		partitionManagers: make(map[string]*offset.PartitionOffsetManager),
		storage:           storage,
	}
}

// NewBrokerOffsetManagerWithSQL creates a new broker offset manager with SQL storage
func NewBrokerOffsetManagerWithSQL(dbPath string) (*BrokerOffsetManager, error) {
	// Create or open SQL database
	db, err := offset.CreateDatabase(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create database: %w", err)
	}

	// Create SQL storage
	sqlStorage, err := offset.NewSQLOffsetStorage(db)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create SQL storage: %w", err)
	}

	return &BrokerOffsetManager{
		offsetIntegration: offset.NewSMQOffsetIntegration(sqlStorage),
		partitionManagers: make(map[string]*offset.PartitionOffsetManager),
		storage:           sqlStorage,
	}, nil
}

// AssignOffset assigns the next offset for a partition
func (bom *BrokerOffsetManager) AssignOffset(t topic.Topic, p topic.Partition) (int64, error) {
	partition := topicPartitionToSchemaPartition(t, p)

	// Use the integration layer's offset assigner to ensure consistency with subscriptions
	result := bom.offsetIntegration.AssignSingleOffset(partition)
	if result.Error != nil {
		return 0, result.Error
	}

	return result.Assignment.Offset, nil
}

// AssignBatchOffsets assigns a batch of offsets for a partition
func (bom *BrokerOffsetManager) AssignBatchOffsets(t topic.Topic, p topic.Partition, count int64) (baseOffset, lastOffset int64, err error) {
	partition := topicPartitionToSchemaPartition(t, p)

	// Use the integration layer's offset assigner to ensure consistency with subscriptions
	result := bom.offsetIntegration.AssignBatchOffsets(partition, count)
	if result.Error != nil {
		return 0, 0, result.Error
	}

	return result.Batch.BaseOffset, result.Batch.LastOffset, nil
}

// GetHighWaterMark returns the high water mark for a partition
func (bom *BrokerOffsetManager) GetHighWaterMark(t topic.Topic, p topic.Partition) (int64, error) {
	partition := topicPartitionToSchemaPartition(t, p)

	// Use the integration layer's offset assigner to ensure consistency with subscriptions
	return bom.offsetIntegration.GetHighWaterMark(partition)
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
	return bom.offsetIntegration.CreateSubscription(subscriptionID, partition, offsetType, startOffset)
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
	return bom.offsetIntegration.GetPartitionOffsetInfo(partition)
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

// partitionKey generates a unique key for a partition (same as offset package)
func partitionKey(partition *schema_pb.Partition) string {
	return fmt.Sprintf("ring:%d:range:%d-%d:time:%d",
		partition.RingSize, partition.RangeStart, partition.RangeStop, partition.UnixTimeNs)
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

	// Clear the partition managers map (now unused but kept for compatibility)
	bom.partitionManagers = make(map[string]*offset.PartitionOffsetManager)

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
