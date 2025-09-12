package offset

import (
	"fmt"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// InMemoryOffsetStorage provides an in-memory implementation of OffsetStorage for testing
type InMemoryOffsetStorage struct {
	mu          sync.RWMutex
	checkpoints map[string]int64           // partition key -> offset
	records     map[string]map[int64]bool  // partition key -> offset -> exists
}

// NewInMemoryOffsetStorage creates a new in-memory storage
func NewInMemoryOffsetStorage() *InMemoryOffsetStorage {
	return &InMemoryOffsetStorage{
		checkpoints: make(map[string]int64),
		records:     make(map[string]map[int64]bool),
	}
}

// SaveCheckpoint saves the checkpoint for a partition
func (s *InMemoryOffsetStorage) SaveCheckpoint(partition *schema_pb.Partition, offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	key := partitionKey(partition)
	s.checkpoints[key] = offset
	return nil
}

// LoadCheckpoint loads the checkpoint for a partition
func (s *InMemoryOffsetStorage) LoadCheckpoint(partition *schema_pb.Partition) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	key := partitionKey(partition)
	offset, exists := s.checkpoints[key]
	if !exists {
		return -1, fmt.Errorf("no checkpoint found")
	}
	
	return offset, nil
}

// GetHighestOffset finds the highest offset in storage for a partition
func (s *InMemoryOffsetStorage) GetHighestOffset(partition *schema_pb.Partition) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	key := partitionKey(partition)
	offsets, exists := s.records[key]
	if !exists || len(offsets) == 0 {
		return -1, fmt.Errorf("no records found")
	}
	
	var highest int64 = -1
	for offset := range offsets {
		if offset > highest {
			highest = offset
		}
	}
	
	return highest, nil
}

// AddRecord simulates storing a record with an offset (for testing)
func (s *InMemoryOffsetStorage) AddRecord(partition *schema_pb.Partition, offset int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	key := partitionKey(partition)
	if s.records[key] == nil {
		s.records[key] = make(map[int64]bool)
	}
	s.records[key][offset] = true
}

// GetRecordCount returns the number of records for a partition (for testing)
func (s *InMemoryOffsetStorage) GetRecordCount(partition *schema_pb.Partition) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	key := partitionKey(partition)
	if offsets, exists := s.records[key]; exists {
		return len(offsets)
	}
	return 0
}

// Clear removes all data (for testing)
func (s *InMemoryOffsetStorage) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	s.checkpoints = make(map[string]int64)
	s.records = make(map[string]map[int64]bool)
}

// Note: SQLOffsetStorage is now implemented in sql_storage.go
