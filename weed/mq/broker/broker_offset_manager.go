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
	mu                 sync.RWMutex
	offsetIntegration  *offset.SMQOffsetIntegration
	partitionManagers  map[string]*offset.PartitionOffsetManager
	storage           offset.OffsetStorage
}

// NewBrokerOffsetManager creates a new broker offset manager
func NewBrokerOffsetManager() *BrokerOffsetManager {
	// TODO: Replace with SQL-based storage in Phase 5
	// ASSUMPTION: For now using in-memory storage, will be replaced with persistent storage
	storage := offset.NewInMemoryOffsetStorage()
	
	return &BrokerOffsetManager{
		offsetIntegration: offset.NewSMQOffsetIntegration(storage),
		partitionManagers: make(map[string]*offset.PartitionOffsetManager),
		storage:          storage,
	}
}

// AssignOffset assigns the next offset for a partition
func (bom *BrokerOffsetManager) AssignOffset(t topic.Topic, p topic.Partition) (int64, error) {
	partition := topicPartitionToSchemaPartition(t, p)
	
	bom.mu.RLock()
	manager, exists := bom.partitionManagers[partitionKey(partition)]
	bom.mu.RUnlock()
	
	if !exists {
		bom.mu.Lock()
		// Double-check after acquiring write lock
		if manager, exists = bom.partitionManagers[partitionKey(partition)]; !exists {
			var err error
			manager, err = offset.NewPartitionOffsetManager(partition, bom.storage)
			if err != nil {
				bom.mu.Unlock()
				return 0, fmt.Errorf("failed to create partition offset manager: %w", err)
			}
			bom.partitionManagers[partitionKey(partition)] = manager
		}
		bom.mu.Unlock()
	}
	
	return manager.AssignOffset(), nil
}

// AssignBatchOffsets assigns a batch of offsets for a partition
func (bom *BrokerOffsetManager) AssignBatchOffsets(t topic.Topic, p topic.Partition, count int64) (baseOffset, lastOffset int64, err error) {
	partition := topicPartitionToSchemaPartition(t, p)
	
	bom.mu.RLock()
	manager, exists := bom.partitionManagers[partitionKey(partition)]
	bom.mu.RUnlock()
	
	if !exists {
		bom.mu.Lock()
		// Double-check after acquiring write lock
		if manager, exists = bom.partitionManagers[partitionKey(partition)]; !exists {
			manager, err = offset.NewPartitionOffsetManager(partition, bom.storage)
			if err != nil {
				bom.mu.Unlock()
				return 0, 0, fmt.Errorf("failed to create partition offset manager: %w", err)
			}
			bom.partitionManagers[partitionKey(partition)] = manager
		}
		bom.mu.Unlock()
	}
	
	baseOffset, lastOffset = manager.AssignOffsets(count)
	return baseOffset, lastOffset, nil
}

// GetHighWaterMark returns the high water mark for a partition
func (bom *BrokerOffsetManager) GetHighWaterMark(t topic.Topic, p topic.Partition) (int64, error) {
	partition := topicPartitionToSchemaPartition(t, p)
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
	// TODO: Access offsetSubscriber through public method
	// ASSUMPTION: This should be exposed through the integration layer
	return nil, fmt.Errorf("GetSubscription not implemented - needs public accessor")
}

// CloseSubscription closes a subscription
func (bom *BrokerOffsetManager) CloseSubscription(subscriptionID string) error {
	return bom.offsetIntegration.CloseSubscription(subscriptionID)
}

// GetPartitionOffsetInfo returns comprehensive offset information for a partition
func (bom *BrokerOffsetManager) GetPartitionOffsetInfo(t topic.Topic, p topic.Partition) (*offset.PartitionOffsetInfo, error) {
	partition := topicPartitionToSchemaPartition(t, p)
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
	Topic       topic.Topic
	Partition   topic.Partition
	BaseOffset  int64
	LastOffset  int64
	Count       int64
	Timestamp   int64
	Error       error
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
	return bom.offsetIntegration.GetOffsetMetrics()
}

// Shutdown gracefully shuts down the offset manager
func (bom *BrokerOffsetManager) Shutdown() {
	bom.mu.Lock()
	defer bom.mu.Unlock()
	
	// Close all partition managers
	for key := range bom.partitionManagers {
		// Partition managers don't have explicit shutdown, but we clear the map
		delete(bom.partitionManagers, key)
	}
	bom.partitionManagers = make(map[string]*offset.PartitionOffsetManager)
	
	// TODO: Close storage connections when SQL storage is implemented
}
