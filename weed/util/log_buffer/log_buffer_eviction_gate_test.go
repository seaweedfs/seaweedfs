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
	defer lb.ShutdownLogBuffer()

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

	// At the watermark with an exclusive cursor (positive batch offset) the
	// retained history is complete → served from memory (adjacent-cursor case).
	if buf, _, _, err := lb.ReadFromBuffer(NewMessagePosition(evictedTs.UnixNano(), 7)); err != nil || buf == nil {
		t.Fatalf("within retained history: buf=%v err=%v", buf != nil, err)
	}

	// At the watermark with a sentinel cursor: sentinel reads include their own
	// timestamp, and the evicted window may end exactly there → defer to disk.
	if _, _, _, err := lb.ReadFromBuffer(NewMessagePosition(evictedTs.UnixNano(), -2)); err != ResumeFromDiskError {
		t.Fatalf("sentinel at watermark: want ResumeFromDiskError, got %v", err)
	}

	// Strictly above the watermark a sentinel cursor is safe again.
	if buf, _, _, err := lb.ReadFromBuffer(NewMessagePosition(evictedTs.Add(time.Nanosecond).UnixNano(), -2)); err != nil || buf == nil {
		t.Fatalf("sentinel above watermark: buf=%v err=%v", buf != nil, err)
	}
}

// TestEvictionWatermarkTracksSealedRing drives the watermark through the real
// eviction path instead of writing the field: only copyToFlushInternal sees the
// window about to be dropped, and it reads it one statement before SealBuffer
// shifts it out of slot 0. The buffer here never flushes, matching the
// aggregated meta ring, so the watermark is its only emptiness proof.
func TestEvictionWatermarkTracksSealedRing(t *testing.T) {
	lb := NewLogBuffer("evict-watermark", time.Minute, nil, nil, nil)
	defer lb.ShutdownLogBuffer()

	// Each timestamp is further past the previous window's start than the flush
	// interval, so every append seals the window before it.
	base := time.Now().Add(-time.Hour).Truncate(time.Second)
	step := 2 * time.Minute
	at := func(i int) time.Time { return base.Add(time.Duration(i) * step) }

	if got := lb.GetLastEvictedTsNs(); got != 0 {
		t.Fatalf("fresh buffer evicted through %v, want 0", time.Unix(0, got))
	}

	// PreviousBufferCount seals only fill the ring; the next one drops window 0.
	for i := 0; i <= PreviousBufferCount; i++ {
		if err := lb.AddLogEntryToBuffer(&filer_pb.LogEntry{TsNs: at(i).UnixNano(), Data: []byte("x"), Key: []byte("k")}); err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
		if got := lb.GetLastEvictedTsNs(); got != 0 {
			t.Fatalf("after %d appends evicted through %v, want nothing evicted yet", i+1, time.Unix(0, got))
		}
	}

	if err := lb.AddLogEntryToBuffer(&filer_pb.LogEntry{TsNs: at(PreviousBufferCount + 1).UnixNano(), Data: []byte("x"), Key: []byte("k")}); err != nil {
		t.Fatalf("add evicting entry: %v", err)
	}
	// Window 0 held only the first entry, so its stopTime is that entry's ts.
	if got, want := lb.GetLastEvictedTsNs(), at(0).UnixNano(); got != want {
		t.Fatalf("evicted through %v, want the dropped window's stop %v", time.Unix(0, got), time.Unix(0, want))
	}

	// The gate must now send a cursor inside the dropped window to disk: those
	// entries are gone from memory and this buffer never flushed them.
	if _, _, _, err := lb.ReadFromBuffer(NewMessagePosition(at(0).UnixNano()-1, -2)); err != ResumeFromDiskError {
		t.Fatalf("cursor below the watermark: want ResumeFromDiskError, got %v", err)
	}
	if _, _, _, err := lb.ReadFromBuffer(NewMessagePosition(0, -2)); err != ResumeFromDiskError {
		t.Fatalf("epoch cursor after eviction: want ResumeFromDiskError, got %v", err)
	}

	// One more eviction advances the watermark; it never regresses.
	if err := lb.AddLogEntryToBuffer(&filer_pb.LogEntry{TsNs: at(PreviousBufferCount + 2).UnixNano(), Data: []byte("x"), Key: []byte("k")}); err != nil {
		t.Fatalf("add second evicting entry: %v", err)
	}
	if got, want := lb.GetLastEvictedTsNs(), at(1).UnixNano(); got != want {
		t.Fatalf("evicted through %v, want %v", time.Unix(0, got), time.Unix(0, want))
	}
}
