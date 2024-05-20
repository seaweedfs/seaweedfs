package sub_coordinator

import (
	"sort"
	"sync"
)

type InflightMessageTracker struct {
	messages   map[string]int64
	mu         sync.Mutex
	timestamps *RingBuffer
}

func NewInflightMessageTracker(capacity int) *InflightMessageTracker {
	return &InflightMessageTracker{
		messages:   make(map[string]int64),
		timestamps: NewRingBuffer(capacity),
	}
}

// InflightMessage tracks the message with the key and timestamp.
// These messages are sent to the consumer group instances and waiting for ack.
func (imt *InflightMessageTracker) InflightMessage(key []byte, tsNs int64) {
	imt.mu.Lock()
	defer imt.mu.Unlock()
	imt.messages[string(key)] = tsNs
	imt.timestamps.Add(tsNs)
}

// IsMessageAcknowledged returns true if the message has been acknowledged.
// If the message is older than the oldest inflight messages, returns false.
// returns false if the message is inflight.
// Otherwise, returns false if the message is old and can be ignored.
func (imt *InflightMessageTracker) IsMessageAcknowledged(key []byte, tsNs int64) bool {
	imt.mu.Lock()
	defer imt.mu.Unlock()

	if tsNs < imt.timestamps.Oldest() {
		return true
	}
	if tsNs > imt.timestamps.Latest() {
		return false
	}

	if _, found := imt.messages[string(key)]; found {
		return false
	}

	return true
}

// AcknowledgeMessage acknowledges the message with the key and timestamp.
func (imt *InflightMessageTracker) AcknowledgeMessage(key []byte, tsNs int64) bool {
	imt.mu.Lock()
	defer imt.mu.Unlock()
	timestamp, exists := imt.messages[string(key)]
	if !exists || timestamp != tsNs {
		return false
	}
	delete(imt.messages, string(key))
	// Remove the specific timestamp from the ring buffer.
	imt.timestamps.Remove(tsNs)
	return true
}

func (imt *InflightMessageTracker) GetOldest() int64 {
	return imt.timestamps.Oldest()
}

// RingBuffer represents a circular buffer to hold timestamps.
type RingBuffer struct {
	buffer []int64
	head   int
	size   int
}

// NewRingBuffer creates a new RingBuffer of the given capacity.
func NewRingBuffer(capacity int) *RingBuffer {
	return &RingBuffer{
		buffer: make([]int64, capacity),
	}
}

// Add adds a new timestamp to the ring buffer.
func (rb *RingBuffer) Add(timestamp int64) {
	rb.buffer[rb.head] = timestamp
	rb.head = (rb.head + 1) % len(rb.buffer)
	if rb.size < len(rb.buffer) {
		rb.size++
	}
}

// Remove removes the specified timestamp from the ring buffer.
func (rb *RingBuffer) Remove(timestamp int64) {
	// Perform binary search
	index := sort.Search(rb.size, func(i int) bool {
		return rb.buffer[(rb.head+len(rb.buffer)-rb.size+i)%len(rb.buffer)] >= timestamp
	})
	actualIndex := (rb.head + len(rb.buffer) - rb.size + index) % len(rb.buffer)

	if index < rb.size && rb.buffer[actualIndex] == timestamp {
		// Shift elements to maintain the buffer order
		for i := index; i < rb.size-1; i++ {
			fromIndex := (rb.head + len(rb.buffer) - rb.size + i + 1) % len(rb.buffer)
			toIndex := (rb.head + len(rb.buffer) - rb.size + i) % len(rb.buffer)
			rb.buffer[toIndex] = rb.buffer[fromIndex]
		}
		rb.size--
		rb.buffer[(rb.head+len(rb.buffer)-1)%len(rb.buffer)] = 0 // Clear the last element
	}
}

// Oldest returns the oldest timestamp in the ring buffer.
func (rb *RingBuffer) Oldest() int64 {
	if rb.size == 0 {
		return 0
	}
	oldestIndex := (rb.head + len(rb.buffer) - rb.size) % len(rb.buffer)
	return rb.buffer[oldestIndex]
}

// Latest returns the most recently added timestamp in the ring buffer.
func (rb *RingBuffer) Latest() int64 {
	if rb.size == 0 {
		return 0
	}
	latestIndex := (rb.head + len(rb.buffer) - 1) % len(rb.buffer)
	return rb.buffer[latestIndex]
}
