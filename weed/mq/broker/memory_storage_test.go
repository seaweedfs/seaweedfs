package broker

import (
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/offset"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// recordEntry holds a record with timestamp for TTL cleanup
type recordEntry struct {
	exists    bool
	timestamp time.Time
}

// InMemoryOffsetStorage provides an in-memory implementation of OffsetStorage for testing ONLY
// This is a copy of the implementation in weed/mq/offset/memory_storage_test.go
type InMemoryOffsetStorage struct {
	mu          sync.RWMutex
	checkpoints map[string]int64                  // partition key -> offset
	records     map[string]map[int64]*recordEntry // partition key -> offset -> entry with timestamp

	// Memory leak protection
	maxRecordsPerPartition int           // Maximum records to keep per partition
	recordTTL              time.Duration // TTL for record entries
	lastCleanup            time.Time     // Last cleanup time
	cleanupInterval        time.Duration // How often to run cleanup
}

// NewInMemoryOffsetStorage creates a new in-memory storage with memory leak protection
// FOR TESTING ONLY - do not use in production
func NewInMemoryOffsetStorage() *InMemoryOffsetStorage {
	return &InMemoryOffsetStorage{
		checkpoints:            make(map[string]int64),
		records:                make(map[string]map[int64]*recordEntry),
		maxRecordsPerPartition: 10000,           // Limit to 10K records per partition
		recordTTL:              1 * time.Hour,   // Records expire after 1 hour
		cleanupInterval:        5 * time.Minute, // Cleanup every 5 minutes
		lastCleanup:            time.Now(),
	}
}

// SaveCheckpoint saves the checkpoint for a partition
func (s *InMemoryOffsetStorage) SaveCheckpoint(namespace, topicName string, partition *schema_pb.Partition, off int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := offset.PartitionKey(partition)
	s.checkpoints[key] = off
	return nil
}

// LoadCheckpoint loads the checkpoint for a partition
func (s *InMemoryOffsetStorage) LoadCheckpoint(namespace, topicName string, partition *schema_pb.Partition) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := offset.PartitionKey(partition)
	off, exists := s.checkpoints[key]
	if !exists {
		return -1, fmt.Errorf("no checkpoint found")
	}

	return off, nil
}

// GetHighestOffset finds the highest offset in storage for a partition
func (s *InMemoryOffsetStorage) GetHighestOffset(namespace, topicName string, partition *schema_pb.Partition) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := offset.PartitionKey(partition)
	offsets, exists := s.records[key]
	if !exists || len(offsets) == 0 {
		return -1, fmt.Errorf("no records found")
	}

	var highest int64 = -1
	for off, entry := range offsets {
		if entry.exists && off > highest {
			highest = off
		}
	}

	return highest, nil
}

// AddRecord simulates storing a record with an offset (for testing)
func (s *InMemoryOffsetStorage) AddRecord(partition *schema_pb.Partition, off int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := offset.PartitionKey(partition)
	if s.records[key] == nil {
		s.records[key] = make(map[int64]*recordEntry)
	}

	// Add record with current timestamp
	s.records[key][off] = &recordEntry{
		exists:    true,
		timestamp: time.Now(),
	}

	// Trigger cleanup if needed (memory leak protection)
	s.cleanupIfNeeded()
}

// Reset removes all data (implements resettable interface for shutdown)
func (s *InMemoryOffsetStorage) Reset() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.checkpoints = make(map[string]int64)
	s.records = make(map[string]map[int64]*recordEntry)
	s.lastCleanup = time.Now()
	return nil
}

// cleanupIfNeeded performs memory leak protection cleanup
// This method assumes the caller already holds the write lock
func (s *InMemoryOffsetStorage) cleanupIfNeeded() {
	now := time.Now()

	// Only cleanup if enough time has passed
	if now.Sub(s.lastCleanup) < s.cleanupInterval {
		return
	}

	s.lastCleanup = now
	cutoff := now.Add(-s.recordTTL)

	// Clean up expired records and enforce size limits
	for partitionKey, offsets := range s.records {
		// Remove expired records
		for offset, entry := range offsets {
			if entry.timestamp.Before(cutoff) {
				delete(offsets, offset)
			}
		}

		// Enforce size limit per partition
		if len(offsets) > s.maxRecordsPerPartition {
			// Keep only the most recent records
			type offsetTime struct {
				offset int64
				time   time.Time
			}

			var entries []offsetTime
			for offset, entry := range offsets {
				entries = append(entries, offsetTime{offset: offset, time: entry.timestamp})
			}

			// Sort by timestamp (newest first)
			for i := 0; i < len(entries)-1; i++ {
				for j := i + 1; j < len(entries); j++ {
					if entries[i].time.Before(entries[j].time) {
						entries[i], entries[j] = entries[j], entries[i]
					}
				}
			}

			// Keep only the newest maxRecordsPerPartition entries
			newOffsets := make(map[int64]*recordEntry)
			for i := 0; i < s.maxRecordsPerPartition && i < len(entries); i++ {
				offset := entries[i].offset
				newOffsets[offset] = offsets[offset]
			}

			s.records[partitionKey] = newOffsets
		}

		// Remove empty partition maps
		if len(offsets) == 0 {
			delete(s.records, partitionKey)
		}
	}
}

// NewInMemoryOffsetStorageForTesting creates an InMemoryOffsetStorage for testing purposes
func NewInMemoryOffsetStorageForTesting() offset.OffsetStorage {
	return NewInMemoryOffsetStorage()
}

// NewBrokerOffsetManagerWithStorage creates a new broker offset manager with custom storage
// FOR TESTING ONLY - moved from production code since it's only used in tests
func NewBrokerOffsetManagerWithStorage(storage offset.OffsetStorage) *BrokerOffsetManager {
	if storage == nil {
		panic("BrokerOffsetManager requires a storage implementation. Use NewBrokerOffsetManagerWithFiler() or provide FilerOffsetStorage/SQLOffsetStorage. InMemoryOffsetStorage is only for testing.")
	}

	return &BrokerOffsetManager{
		offsetIntegration:    offset.NewSMQOffsetIntegration(storage),
		storage:              storage,
		consumerGroupStorage: nil, // Will be set separately if needed
	}
}
