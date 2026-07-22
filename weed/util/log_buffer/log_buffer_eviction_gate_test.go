package log_buffer

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// TestReadFromBufferEvictionGate pins the eviction-watermark gate: a time-based
// read positioned below the watermark must defer to disk (the window may hold
// evicted-but-unflushed events), while a position within the retained history
// reads inclusively from memory regardless of its batch offset.
func TestReadFromBufferEvictionGate(t *testing.T) {
	lb := NewLogBuffer("evict-gate", 10*time.Minute,
		func(logBuffer *LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {},
		func(startPosition MessagePosition, stopTsNs int64, eachLogEntryFn EachLogEntryFuncType) (MessagePosition, bool, error) {
			return startPosition, false, nil
		},
		func() {})

	base := time.Now().Add(-time.Second)
	entry := &filer_pb.LogEntry{TsNs: base.UnixNano(), Data: []byte("x"), Key: []byte("k"), Offset: 0}
	if err := lb.AddLogEntryToBuffer(entry); err != nil {
		t.Fatalf("add: %v", err)
	}

	// No eviction yet: a position below earliest still reads inclusively.
	if buf, _, _, err := lb.ReadFromBuffer(NewMessagePosition(base.Add(-time.Minute).UnixNano(), -2)); err != nil || buf == nil {
		t.Fatalf("pre-eviction inclusive read: buf=%v err=%v", buf != nil, err)
	}

	// Simulate a window evicted from the ring before it was flushed.
	evictedTs := base.Add(-time.Millisecond)
	lb.lastEvictedTsNs.Store(evictedTs.UnixNano())

	// Below the watermark: must defer to disk, never silently start at earliest.
	if _, _, _, err := lb.ReadFromBuffer(NewMessagePosition(base.Add(-time.Minute).UnixNano(), -2)); err != ResumeFromDiskError {
		t.Fatalf("below watermark: want ResumeFromDiskError, got %v", err)
	}

	// Zero/epoch start after eviction: memory no longer holds the complete
	// history → defer to disk.
	if _, _, _, err := lb.ReadFromBuffer(NewMessagePosition(0, -2)); err != ResumeFromDiskError {
		t.Fatalf("epoch start after eviction: want ResumeFromDiskError, got %v", err)
	}

	// At/after the watermark the retained history is complete → inclusive read,
	// regardless of the cursor's batch offset (adjacent-cursor case).
	if buf, _, _, err := lb.ReadFromBuffer(NewMessagePosition(evictedTs.UnixNano(), 7)); err != nil || buf == nil {
		t.Fatalf("within retained history: buf=%v err=%v", buf != nil, err)
	}
}
