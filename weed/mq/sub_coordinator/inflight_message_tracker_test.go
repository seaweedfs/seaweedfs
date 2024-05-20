package sub_coordinator

import (
	"sort"
	"testing"
	"time"
)

func TestRingBuffer(t *testing.T) {
	// Initialize a RingBuffer with capacity 5
	rb := NewRingBuffer(5)

	// Add timestamps to the buffer
	timestamps := []int64{100, 200, 300, 400, 500}
	for _, ts := range timestamps {
		rb.Add(ts)
	}

	// Test Add method and buffer size
	expectedSize := 5
	if rb.size != expectedSize {
		t.Errorf("Expected buffer size %d, got %d", expectedSize, rb.size)
	}

	// Test Oldest and Latest methods
	expectedOldest := int64(100)
	if oldest := rb.Oldest(); oldest != expectedOldest {
		t.Errorf("Expected oldest timestamp %d, got %d", expectedOldest, oldest)
	}
	expectedLatest := int64(500)
	if latest := rb.Latest(); latest != expectedLatest {
		t.Errorf("Expected latest timestamp %d, got %d", expectedLatest, latest)
	}

	// Test Remove method
	rb.Remove(200)
	expectedSize--
	if rb.size != expectedSize {
		t.Errorf("Expected buffer size %d after removal, got %d", expectedSize, rb.size)
	}

	// Test removal of non-existent element
	rb.Remove(600)
	if rb.size != expectedSize {
		t.Errorf("Expected buffer size %d after attempting removal of non-existent element, got %d", expectedSize, rb.size)
	}

	// Test binary search correctness
	target := int64(300)
	index := sort.Search(rb.size, func(i int) bool {
		return rb.buffer[(rb.head+len(rb.buffer)-rb.size+i)%len(rb.buffer)] >= target
	})
	actualIndex := (rb.head + len(rb.buffer) - rb.size + index) % len(rb.buffer)
	if rb.buffer[actualIndex] != target {
		t.Errorf("Binary search failed to find the correct index for timestamp %d", target)
	}
}

func TestInflightMessageTracker(t *testing.T) {
	// Initialize an InflightMessageTracker with capacity 5
	tracker := NewInflightMessageTracker(5)

	// Add inflight messages
	key := []byte("exampleKey")
	timestamp := time.Now().UnixNano()
	tracker.InflightMessage(key, timestamp)

	// Test IsMessageAcknowledged method
	isOld := tracker.IsMessageAcknowledged(key, timestamp-10)
	if !isOld {
		t.Error("Expected message to be old")
	}

	// Test AcknowledgeMessage method
	acked := tracker.AcknowledgeMessage(key, timestamp)
	if !acked {
		t.Error("Expected message to be acked")
	}
	if _, exists := tracker.messages[string(key)]; exists {
		t.Error("Expected message to be deleted after ack")
	}
	if tracker.timestamps.size != 0 {
		t.Error("Expected buffer size to be 0 after ack")
	}
}
