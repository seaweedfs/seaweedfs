package offset

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// OffsetEntry represents a single offset mapping
type OffsetEntry struct {
	KafkaOffset int64 // Kafka offset (sequential integer)
	Timestamp   int64 // SeaweedMQ timestamp (nanoseconds)
	Size        int32 // Message size in bytes
}

// Ledger maintains the mapping between Kafka offsets and SeaweedMQ timestamps
// for a single topic partition
type Ledger struct {
	mu           sync.RWMutex
	entries      []OffsetEntry // sorted by KafkaOffset
	nextOffset   int64         // next offset to assign
	earliestTime int64         // timestamp of earliest message
	latestTime   int64         // timestamp of latest message
}

// NewLedger creates a new offset ledger starting from offset 0
func NewLedger() *Ledger {
	return &Ledger{
		entries:    make([]OffsetEntry, 0, 1000), // pre-allocate for performance
		nextOffset: 0,
	}
}

// AssignOffsets reserves a range of consecutive Kafka offsets
// Returns the base offset of the reserved range
func (l *Ledger) AssignOffsets(count int64) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	
	baseOffset := l.nextOffset
	l.nextOffset += count
	return baseOffset
}

// AppendRecord adds a new offset entry to the ledger
// The kafkaOffset should be from a previous AssignOffsets call
func (l *Ledger) AppendRecord(kafkaOffset, timestamp int64, size int32) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	
	// Validate offset is in expected range
	if kafkaOffset < 0 || kafkaOffset >= l.nextOffset {
		return fmt.Errorf("invalid offset %d, expected 0 <= offset < %d", kafkaOffset, l.nextOffset)
	}
	
	// Check for duplicate offset (shouldn't happen in normal operation)
	if len(l.entries) > 0 && l.entries[len(l.entries)-1].KafkaOffset >= kafkaOffset {
		return fmt.Errorf("offset %d already exists or is out of order", kafkaOffset)
	}
	
	entry := OffsetEntry{
		KafkaOffset: kafkaOffset,
		Timestamp:   timestamp,
		Size:        size,
	}
	
	l.entries = append(l.entries, entry)
	
	// Update earliest/latest timestamps
	if l.earliestTime == 0 || timestamp < l.earliestTime {
		l.earliestTime = timestamp
	}
	if timestamp > l.latestTime {
		l.latestTime = timestamp
	}
	
	return nil
}

// GetRecord retrieves the record information for a given Kafka offset
func (l *Ledger) GetRecord(kafkaOffset int64) (timestamp int64, size int32, err error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	
	// Binary search for the offset
	idx := sort.Search(len(l.entries), func(i int) bool {
		return l.entries[i].KafkaOffset >= kafkaOffset
	})
	
	if idx >= len(l.entries) || l.entries[idx].KafkaOffset != kafkaOffset {
		return 0, 0, fmt.Errorf("offset %d not found", kafkaOffset)
	}
	
	entry := l.entries[idx]
	return entry.Timestamp, entry.Size, nil
}

// GetEarliestOffset returns the smallest Kafka offset in the ledger
func (l *Ledger) GetEarliestOffset() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	
	if len(l.entries) == 0 {
		return 0 // no messages yet, earliest is 0
	}
	return l.entries[0].KafkaOffset
}

// GetLatestOffset returns the largest Kafka offset in the ledger
func (l *Ledger) GetLatestOffset() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	
	if len(l.entries) == 0 {
		return 0 // no messages yet, latest is 0
	}
	return l.entries[len(l.entries)-1].KafkaOffset
}

// GetHighWaterMark returns the next offset that will be assigned
// (i.e., one past the latest offset)
func (l *Ledger) GetHighWaterMark() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.nextOffset
}

// FindOffsetByTimestamp returns the first offset with a timestamp >= target
// Used for timestamp-based offset lookup
func (l *Ledger) FindOffsetByTimestamp(targetTimestamp int64) int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	
	if len(l.entries) == 0 {
		return 0
	}
	
	// Binary search for first entry with timestamp >= targetTimestamp
	idx := sort.Search(len(l.entries), func(i int) bool {
		return l.entries[i].Timestamp >= targetTimestamp
	})
	
	if idx >= len(l.entries) {
		// Target timestamp is after all entries, return high water mark
		return l.nextOffset
	}
	
	return l.entries[idx].KafkaOffset
}

// GetStats returns basic statistics about the ledger
func (l *Ledger) GetStats() (entryCount int, earliestTime, latestTime, nextOffset int64) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	
	return len(l.entries), l.earliestTime, l.latestTime, l.nextOffset
}

// GetTimestampRange returns the time range covered by this ledger
func (l *Ledger) GetTimestampRange() (earliest, latest int64) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	
	if len(l.entries) == 0 {
		now := time.Now().UnixNano()
		return now, now // stub values when no data
	}
	
	return l.earliestTime, l.latestTime
}
