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

// EnflightMessage tracks the message with the key and timestamp.
// These messages are sent to the consumer group instances and waiting for ack.
func (imt *InflightMessageTracker) EnflightMessage(key []byte, tsNs int64) {
	// fmt.Printf("EnflightMessage(%s,%d)\n", string(key), tsNs)
	imt.mu.Lock()
	defer imt.mu.Unlock()
	imt.messages[string(key)] = tsNs
	imt.timestamps.EnflightTimestamp(tsNs)
}

// IsMessageAcknowledged returns true if the message has been acknowledged.
// If the message is older than the oldest inflight messages, returns false.
// returns false if the message is inflight.
// Otherwise, returns false if the message is old and can be ignored.
func (imt *InflightMessageTracker) IsMessageAcknowledged(key []byte, tsNs int64) bool {
	imt.mu.Lock()
	defer imt.mu.Unlock()

	if tsNs <= imt.timestamps.OldestAckedTimestamp() {
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
	// fmt.Printf("AcknowledgeMessage(%s,%d)\n", string(key), tsNs)
	imt.mu.Lock()
	defer imt.mu.Unlock()
	timestamp, exists := imt.messages[string(key)]
	if !exists || timestamp != tsNs {
		return false
	}
	delete(imt.messages, string(key))
	// Remove the specific timestamp from the ring buffer.
	imt.timestamps.AckTimestamp(tsNs)
	return true
}

func (imt *InflightMessageTracker) GetOldestAckedTimestamp() int64 {
	return imt.timestamps.OldestAckedTimestamp()
}

// IsInflight returns true if the message with the key is inflight.
func (imt *InflightMessageTracker) IsInflight(key []byte) bool {
	imt.mu.Lock()
	defer imt.mu.Unlock()
	_, found := imt.messages[string(key)]
	return found
}

type TimestampStatus struct {
	Timestamp int64
	Acked     bool
}

// RingBuffer represents a circular buffer to hold timestamps.
type RingBuffer struct {
	buffer        []*TimestampStatus
	head          int
	size          int
	maxTimestamp  int64
	maxAllAckedTs int64
}

// NewRingBuffer creates a new RingBuffer of the given capacity.
func NewRingBuffer(capacity int) *RingBuffer {
	return &RingBuffer{
		buffer: newBuffer(capacity),
	}
}

func newBuffer(capacity int) []*TimestampStatus {
	buffer := make([]*TimestampStatus, capacity)
	for i := range buffer {
		buffer[i] = &TimestampStatus{}
	}
	return buffer
}

// EnflightTimestamp adds a new timestamp to the ring buffer.
func (rb *RingBuffer) EnflightTimestamp(timestamp int64) {
	if rb.size < len(rb.buffer) {
		rb.size++
	} else {
		newBuf := newBuffer(2 * len(rb.buffer))
		for i := 0; i < rb.size; i++ {
			newBuf[i] = rb.buffer[(rb.head+len(rb.buffer)-rb.size+i)%len(rb.buffer)]
		}
		rb.buffer = newBuf
		rb.head = rb.size
		rb.size++
	}
	head := rb.buffer[rb.head]
	head.Timestamp = timestamp
	head.Acked = false
	rb.head = (rb.head + 1) % len(rb.buffer)
	if timestamp > rb.maxTimestamp {
		rb.maxTimestamp = timestamp
	}
}

// AckTimestamp removes the specified timestamp from the ring buffer.
func (rb *RingBuffer) AckTimestamp(timestamp int64) {
	// Perform binary search
	index := sort.Search(rb.size, func(i int) bool {
		return rb.buffer[(rb.head+len(rb.buffer)-rb.size+i)%len(rb.buffer)].Timestamp >= timestamp
	})
	actualIndex := (rb.head + len(rb.buffer) - rb.size + index) % len(rb.buffer)

	rb.buffer[actualIndex].Acked = true

	// Remove all the continuously acknowledged timestamps from the buffer
	startPos := (rb.head + len(rb.buffer) - rb.size) % len(rb.buffer)
	for i := 0; i < len(rb.buffer) && rb.buffer[(startPos+i)%len(rb.buffer)].Acked; i++ {
		t := rb.buffer[(startPos+i)%len(rb.buffer)]
		if rb.maxAllAckedTs < t.Timestamp {
			rb.size--
			rb.maxAllAckedTs = t.Timestamp
		}
	}
}

// OldestAckedTimestamp returns the oldest that is already acked timestamp in the ring buffer.
func (rb *RingBuffer) OldestAckedTimestamp() int64 {
	return rb.maxAllAckedTs
}

// Latest returns the most recently known timestamp in the ring buffer.
func (rb *RingBuffer) Latest() int64 {
	return rb.maxTimestamp
}
