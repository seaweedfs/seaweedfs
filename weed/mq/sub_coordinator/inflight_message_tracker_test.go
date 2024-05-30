package sub_coordinator

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRingBuffer(t *testing.T) {
	// Initialize a RingBuffer with capacity 5
	rb := NewRingBuffer(5)

	// Add timestamps to the buffer
	timestamps := []int64{100, 200, 300, 400, 500}
	for _, ts := range timestamps {
		rb.EnflightTimestamp(ts)
	}

	// Test Add method and buffer size
	expectedSize := 5
	if rb.size != expectedSize {
		t.Errorf("Expected buffer size %d, got %d", expectedSize, rb.size)
	}

	assert.Equal(t, int64(0), rb.OldestAckedTimestamp())
	assert.Equal(t, int64(500), rb.Latest())

	rb.AckTimestamp(200)
	assert.Equal(t, int64(0), rb.OldestAckedTimestamp())
	rb.AckTimestamp(100)
	assert.Equal(t, int64(200), rb.OldestAckedTimestamp())

	rb.EnflightTimestamp(int64(600))
	rb.EnflightTimestamp(int64(700))

	rb.AckTimestamp(500)
	assert.Equal(t, int64(200), rb.OldestAckedTimestamp())
	rb.AckTimestamp(400)
	assert.Equal(t, int64(200), rb.OldestAckedTimestamp())
	rb.AckTimestamp(300)
	assert.Equal(t, int64(500), rb.OldestAckedTimestamp())

	assert.Equal(t, int64(700), rb.Latest())
}

func TestInflightMessageTracker(t *testing.T) {
	// Initialize an InflightMessageTracker with capacity 5
	tracker := NewInflightMessageTracker(5)

	// Add inflight messages
	key := []byte("1")
	timestamp := int64(1)
	tracker.EnflightMessage(key, timestamp)

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
	assert.Equal(t, timestamp, tracker.GetOldestAckedTimestamp())
}

func TestInflightMessageTracker2(t *testing.T) {
	// Initialize an InflightMessageTracker with initial capacity 1
	tracker := NewInflightMessageTracker(1)

	tracker.EnflightMessage([]byte("1"), int64(1))
	tracker.EnflightMessage([]byte("2"), int64(2))
	tracker.EnflightMessage([]byte("3"), int64(3))
	tracker.EnflightMessage([]byte("4"), int64(4))
	tracker.EnflightMessage([]byte("5"), int64(5))
	assert.True(t, tracker.AcknowledgeMessage([]byte("1"), int64(1)))
	assert.Equal(t, int64(1), tracker.GetOldestAckedTimestamp())

	// Test IsMessageAcknowledged method
	isAcked := tracker.IsMessageAcknowledged([]byte("2"), int64(2))
	if isAcked {
		t.Error("Expected message to be not acked")
	}

	// Test AcknowledgeMessage method
	assert.True(t, tracker.AcknowledgeMessage([]byte("2"), int64(2)))
	assert.Equal(t, int64(2), tracker.GetOldestAckedTimestamp())

}

func TestInflightMessageTracker3(t *testing.T) {
	// Initialize an InflightMessageTracker with initial capacity 1
	tracker := NewInflightMessageTracker(1)

	tracker.EnflightMessage([]byte("1"), int64(1))
	tracker.EnflightMessage([]byte("2"), int64(2))
	tracker.EnflightMessage([]byte("3"), int64(3))
	assert.True(t, tracker.AcknowledgeMessage([]byte("1"), int64(1)))
	tracker.EnflightMessage([]byte("4"), int64(4))
	tracker.EnflightMessage([]byte("5"), int64(5))
	assert.True(t, tracker.AcknowledgeMessage([]byte("2"), int64(2)))
	assert.True(t, tracker.AcknowledgeMessage([]byte("3"), int64(3)))
	tracker.EnflightMessage([]byte("6"), int64(6))
	tracker.EnflightMessage([]byte("7"), int64(7))
	assert.True(t, tracker.AcknowledgeMessage([]byte("4"), int64(4)))
	assert.True(t, tracker.AcknowledgeMessage([]byte("5"), int64(5)))
	assert.True(t, tracker.AcknowledgeMessage([]byte("6"), int64(6)))
	assert.Equal(t, int64(6), tracker.GetOldestAckedTimestamp())
	assert.True(t, tracker.AcknowledgeMessage([]byte("7"), int64(7)))
	assert.Equal(t, int64(7), tracker.GetOldestAckedTimestamp())

}

func TestInflightMessageTracker4(t *testing.T) {
	// Initialize an InflightMessageTracker with initial capacity 1
	tracker := NewInflightMessageTracker(1)

	tracker.EnflightMessage([]byte("1"), int64(1))
	tracker.EnflightMessage([]byte("2"), int64(2))
	assert.True(t, tracker.AcknowledgeMessage([]byte("1"), int64(1)))
	assert.True(t, tracker.AcknowledgeMessage([]byte("2"), int64(2)))
	tracker.EnflightMessage([]byte("3"), int64(3))
	assert.True(t, tracker.AcknowledgeMessage([]byte("3"), int64(3)))
	assert.Equal(t, int64(3), tracker.GetOldestAckedTimestamp())

}
