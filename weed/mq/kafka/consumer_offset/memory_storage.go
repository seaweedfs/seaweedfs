package consumer_offset

import (
	"sync"
)

// MemoryStorage implements OffsetStorage using in-memory maps
// This is suitable for testing and single-node deployments
// Data is lost on restart
type MemoryStorage struct {
	mu     sync.RWMutex
	groups map[string]map[TopicPartition]OffsetMetadata
	closed bool
}

// NewMemoryStorage creates a new in-memory offset storage
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		groups: make(map[string]map[TopicPartition]OffsetMetadata),
		closed: false,
	}
}

// CommitOffset commits an offset for a consumer group
func (m *MemoryStorage) CommitOffset(group, topic string, partition int32, offset int64, metadata string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrStorageClosed
	}

	// Validate inputs
	if offset < -1 {
		return ErrInvalidOffset
	}
	if partition < 0 {
		return ErrInvalidPartition
	}

	// Create group if it doesn't exist
	if m.groups[group] == nil {
		m.groups[group] = make(map[TopicPartition]OffsetMetadata)
	}

	// Store offset
	tp := TopicPartition{Topic: topic, Partition: partition}
	m.groups[group][tp] = OffsetMetadata{
		Offset:   offset,
		Metadata: metadata,
	}

	return nil
}

// FetchOffset fetches the committed offset for a consumer group
func (m *MemoryStorage) FetchOffset(group, topic string, partition int32) (int64, string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return -1, "", ErrStorageClosed
	}

	groupOffsets, exists := m.groups[group]
	if !exists {
		// Group doesn't exist, return -1 (no committed offset)
		return -1, "", nil
	}

	tp := TopicPartition{Topic: topic, Partition: partition}
	offsetMeta, exists := groupOffsets[tp]
	if !exists {
		// No offset committed for this partition
		return -1, "", nil
	}

	return offsetMeta.Offset, offsetMeta.Metadata, nil
}

// FetchAllOffsets fetches all committed offsets for a consumer group
func (m *MemoryStorage) FetchAllOffsets(group string) (map[TopicPartition]OffsetMetadata, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, ErrStorageClosed
	}

	groupOffsets, exists := m.groups[group]
	if !exists {
		// Return empty map for non-existent group
		return make(map[TopicPartition]OffsetMetadata), nil
	}

	// Return a copy to prevent external modification
	result := make(map[TopicPartition]OffsetMetadata, len(groupOffsets))
	for tp, offset := range groupOffsets {
		result[tp] = offset
	}

	return result, nil
}

// DeleteGroup deletes all offset data for a consumer group
func (m *MemoryStorage) DeleteGroup(group string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrStorageClosed
	}

	delete(m.groups, group)
	return nil
}

// ListGroups returns all consumer group IDs
func (m *MemoryStorage) ListGroups() ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, ErrStorageClosed
	}

	groups := make([]string, 0, len(m.groups))
	for group := range m.groups {
		groups = append(groups, group)
	}

	return groups, nil
}

// Close releases resources (no-op for memory storage)
func (m *MemoryStorage) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true
	m.groups = nil

	return nil
}
