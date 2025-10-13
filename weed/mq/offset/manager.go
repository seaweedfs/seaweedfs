package offset

import (
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// PartitionOffsetManager manages sequential offset assignment for a single partition
type PartitionOffsetManager struct {
	mu         sync.RWMutex
	namespace  string
	topicName  string
	partition  *schema_pb.Partition
	nextOffset int64

	// Checkpointing for recovery
	lastCheckpoint     int64
	checkpointInterval int64
	storage            OffsetStorage
}

// OffsetStorage interface for persisting offset state
type OffsetStorage interface {
	// SaveCheckpoint persists the current offset state for recovery
	// Takes topic information along with partition to determine the correct storage location
	SaveCheckpoint(namespace, topicName string, partition *schema_pb.Partition, offset int64) error

	// LoadCheckpoint retrieves the last saved offset state
	LoadCheckpoint(namespace, topicName string, partition *schema_pb.Partition) (int64, error)

	// GetHighestOffset scans storage to find the highest assigned offset
	GetHighestOffset(namespace, topicName string, partition *schema_pb.Partition) (int64, error)
}

// NewPartitionOffsetManager creates a new offset manager for a partition
func NewPartitionOffsetManager(namespace, topicName string, partition *schema_pb.Partition, storage OffsetStorage) (*PartitionOffsetManager, error) {
	manager := &PartitionOffsetManager{
		namespace:          namespace,
		topicName:          topicName,
		partition:          partition,
		checkpointInterval: 1, // Checkpoint every offset for immediate persistence
		storage:            storage,
	}

	// Recover offset state
	if err := manager.recover(); err != nil {
		return nil, fmt.Errorf("failed to recover offset state: %w", err)
	}

	return manager, nil
}

// AssignOffset assigns the next sequential offset
func (m *PartitionOffsetManager) AssignOffset() int64 {
	var shouldCheckpoint bool
	var checkpointOffset int64

	m.mu.Lock()
	offset := m.nextOffset
	m.nextOffset++

	// Check if we should checkpoint (but don't do it inside the lock)
	if offset-m.lastCheckpoint >= m.checkpointInterval {
		shouldCheckpoint = true
		checkpointOffset = offset
	}
	m.mu.Unlock()

	// Checkpoint outside the lock to avoid deadlock
	if shouldCheckpoint {
		m.checkpoint(checkpointOffset)
	}

	return offset
}

// AssignOffsets assigns a batch of sequential offsets
func (m *PartitionOffsetManager) AssignOffsets(count int64) (baseOffset int64, lastOffset int64) {
	var shouldCheckpoint bool
	var checkpointOffset int64

	m.mu.Lock()
	baseOffset = m.nextOffset
	lastOffset = m.nextOffset + count - 1
	m.nextOffset += count

	// Check if we should checkpoint (but don't do it inside the lock)
	if lastOffset-m.lastCheckpoint >= m.checkpointInterval {
		shouldCheckpoint = true
		checkpointOffset = lastOffset
	}
	m.mu.Unlock()

	// Checkpoint outside the lock to avoid deadlock
	if shouldCheckpoint {
		m.checkpoint(checkpointOffset)
	}

	return baseOffset, lastOffset
}

// GetNextOffset returns the next offset that will be assigned
func (m *PartitionOffsetManager) GetNextOffset() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.nextOffset
}

// GetHighWaterMark returns the high water mark (next offset)
func (m *PartitionOffsetManager) GetHighWaterMark() int64 {
	return m.GetNextOffset()
}

// recover restores offset state from storage
func (m *PartitionOffsetManager) recover() error {
	var checkpointOffset int64 = -1
	var highestOffset int64 = -1

	// Try to load checkpoint
	if offset, err := m.storage.LoadCheckpoint(m.namespace, m.topicName, m.partition); err == nil && offset >= 0 {
		checkpointOffset = offset
	}

	// Try to scan storage for highest offset
	if offset, err := m.storage.GetHighestOffset(m.namespace, m.topicName, m.partition); err == nil && offset >= 0 {
		highestOffset = offset
	}

	// Use the higher of checkpoint or storage scan
	if checkpointOffset >= 0 && highestOffset >= 0 {
		if highestOffset > checkpointOffset {
			m.nextOffset = highestOffset + 1
			m.lastCheckpoint = highestOffset
		} else {
			m.nextOffset = checkpointOffset + 1
			m.lastCheckpoint = checkpointOffset
		}
	} else if checkpointOffset >= 0 {
		m.nextOffset = checkpointOffset + 1
		m.lastCheckpoint = checkpointOffset
	} else if highestOffset >= 0 {
		m.nextOffset = highestOffset + 1
		m.lastCheckpoint = highestOffset
	} else {
		// No data exists, start from 0
		m.nextOffset = 0
		m.lastCheckpoint = -1
	}

	return nil
}

// checkpoint saves the current offset state
func (m *PartitionOffsetManager) checkpoint(offset int64) {
	if err := m.storage.SaveCheckpoint(m.namespace, m.topicName, m.partition, offset); err != nil {
		// Log error but don't fail - checkpointing is for optimization
		fmt.Printf("Failed to checkpoint offset %d: %v\n", offset, err)
		return
	}

	m.mu.Lock()
	m.lastCheckpoint = offset
	m.mu.Unlock()
}

// PartitionOffsetRegistry manages offset managers for multiple partitions
type PartitionOffsetRegistry struct {
	mu       sync.RWMutex
	managers map[string]*PartitionOffsetManager
	storage  OffsetStorage
}

// NewPartitionOffsetRegistry creates a new registry
func NewPartitionOffsetRegistry(storage OffsetStorage) *PartitionOffsetRegistry {
	return &PartitionOffsetRegistry{
		managers: make(map[string]*PartitionOffsetManager),
		storage:  storage,
	}
}

// GetManager returns the offset manager for a partition, creating it if needed
func (r *PartitionOffsetRegistry) GetManager(namespace, topicName string, partition *schema_pb.Partition) (*PartitionOffsetManager, error) {
	// CRITICAL FIX: Use TopicPartitionKey to ensure each topic has its own offset manager
	key := TopicPartitionKey(namespace, topicName, partition)

	r.mu.RLock()
	manager, exists := r.managers[key]
	r.mu.RUnlock()

	if exists {
		return manager, nil
	}

	// Create new manager
	r.mu.Lock()
	defer r.mu.Unlock()

	// Double-check after acquiring write lock
	if manager, exists := r.managers[key]; exists {
		return manager, nil
	}

	manager, err := NewPartitionOffsetManager(namespace, topicName, partition, r.storage)
	if err != nil {
		return nil, err
	}

	r.managers[key] = manager
	return manager, nil
}

// AssignOffset assigns an offset for the given partition
func (r *PartitionOffsetRegistry) AssignOffset(namespace, topicName string, partition *schema_pb.Partition) (int64, error) {
	isSchemasTopic := topicName == "_schemas"
	partKey := PartitionKey(partition)

	manager, err := r.GetManager(namespace, topicName, partition)
	if err != nil {
		return 0, err
	}

	assignedOffset := manager.AssignOffset()

	if isSchemasTopic {
		glog.Infof("[OFFSET DEBUG] AssignOffset for _schemas: partitionKey=%s assignedOffset=%d nextOffset=%d",
			partKey, assignedOffset, manager.GetNextOffset())
	}

	return assignedOffset, nil
}

// AssignOffsets assigns a batch of offsets for the given partition
func (r *PartitionOffsetRegistry) AssignOffsets(namespace, topicName string, partition *schema_pb.Partition, count int64) (baseOffset, lastOffset int64, err error) {
	manager, err := r.GetManager(namespace, topicName, partition)
	if err != nil {
		return 0, 0, err
	}

	baseOffset, lastOffset = manager.AssignOffsets(count)
	return baseOffset, lastOffset, nil
}

// GetHighWaterMark returns the high water mark for a partition
func (r *PartitionOffsetRegistry) GetHighWaterMark(namespace, topicName string, partition *schema_pb.Partition) (int64, error) {
	manager, err := r.GetManager(namespace, topicName, partition)
	if err != nil {
		return 0, err
	}

	return manager.GetHighWaterMark(), nil
}

// TopicPartitionKey generates a unique key for a topic-partition combination
// This is the canonical key format used across the offset management system
func TopicPartitionKey(namespace, topicName string, partition *schema_pb.Partition) string {
	return fmt.Sprintf("%s/%s/ring:%d:range:%d-%d",
		namespace, topicName,
		partition.RingSize, partition.RangeStart, partition.RangeStop)
}

// PartitionKey generates a unique key for a partition (without topic context)
// Note: UnixTimeNs is intentionally excluded from the key because it represents
// partition creation time, not partition identity. Using it would cause offset
// tracking to reset whenever a partition is recreated or looked up again.
// DEPRECATED: Use TopicPartitionKey for production code to avoid key collisions
func PartitionKey(partition *schema_pb.Partition) string {
	return fmt.Sprintf("ring:%d:range:%d-%d",
		partition.RingSize, partition.RangeStart, partition.RangeStop)
}

// partitionKey is the internal lowercase version for backward compatibility within this package
func partitionKey(partition *schema_pb.Partition) string {
	return PartitionKey(partition)
}

// OffsetAssignment represents an assigned offset with metadata
type OffsetAssignment struct {
	Offset    int64
	Timestamp int64
	Partition *schema_pb.Partition
}

// BatchOffsetAssignment represents a batch of assigned offsets
type BatchOffsetAssignment struct {
	BaseOffset int64
	LastOffset int64
	Count      int64
	Timestamp  int64
	Partition  *schema_pb.Partition
}

// AssignmentResult contains the result of offset assignment
type AssignmentResult struct {
	Assignment *OffsetAssignment
	Batch      *BatchOffsetAssignment
	Error      error
}

// OffsetAssigner provides high-level offset assignment operations
type OffsetAssigner struct {
	registry *PartitionOffsetRegistry
}

// NewOffsetAssigner creates a new offset assigner
func NewOffsetAssigner(storage OffsetStorage) *OffsetAssigner {
	return &OffsetAssigner{
		registry: NewPartitionOffsetRegistry(storage),
	}
}

// AssignSingleOffset assigns a single offset with timestamp
func (a *OffsetAssigner) AssignSingleOffset(namespace, topicName string, partition *schema_pb.Partition) *AssignmentResult {
	offset, err := a.registry.AssignOffset(namespace, topicName, partition)
	if err != nil {
		return &AssignmentResult{Error: err}
	}

	return &AssignmentResult{
		Assignment: &OffsetAssignment{
			Offset:    offset,
			Timestamp: time.Now().UnixNano(),
			Partition: partition,
		},
	}
}

// AssignBatchOffsets assigns a batch of offsets with timestamp
func (a *OffsetAssigner) AssignBatchOffsets(namespace, topicName string, partition *schema_pb.Partition, count int64) *AssignmentResult {
	baseOffset, lastOffset, err := a.registry.AssignOffsets(namespace, topicName, partition, count)
	if err != nil {
		return &AssignmentResult{Error: err}
	}

	return &AssignmentResult{
		Batch: &BatchOffsetAssignment{
			BaseOffset: baseOffset,
			LastOffset: lastOffset,
			Count:      count,
			Timestamp:  time.Now().UnixNano(),
			Partition:  partition,
		},
	}
}

// GetHighWaterMark returns the high water mark for a partition
func (a *OffsetAssigner) GetHighWaterMark(namespace, topicName string, partition *schema_pb.Partition) (int64, error) {
	return a.registry.GetHighWaterMark(namespace, topicName, partition)
}
