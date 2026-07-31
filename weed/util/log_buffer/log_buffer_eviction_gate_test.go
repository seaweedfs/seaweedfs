package log_buffer

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

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

	// One more eviction advances the watermark; it never regresses.
	if err := lb.AddLogEntryToBuffer(&filer_pb.LogEntry{TsNs: at(PreviousBufferCount + 2).UnixNano(), Data: []byte("x"), Key: []byte("k")}); err != nil {
		t.Fatalf("add second evicting entry: %v", err)
	}
	if got, want := lb.GetLastEvictedTsNs(), at(1).UnixNano(); got != want {
		t.Fatalf("evicted through %v, want %v", time.Unix(0, got), time.Unix(0, want))
	}
}
