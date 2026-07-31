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

// TestGapResumeCursorReadsSingleEntrySealedWindow pins the resume cursor
// against the shape that actually breaks: a sealed window holding one entry,
// where startTime == stopTime. The sealed-buffer lookup only enters a window
// whose stopTime is strictly after the cursor, so a cursor sitting exactly on
// earliest walks straight past such a window and its sole event is never
// delivered. Low-volume metadata windows are routinely one entry.
func TestGapResumeCursorReadsSingleEntrySealedWindow(t *testing.T) {
	lb := NewLogBuffer("gap-resume-sealed", time.Minute, nil, nil, nil)
	defer lb.ShutdownLogBuffer()

	// Timestamps far enough apart that every append seals the window before it,
	// so each sealed window holds exactly one entry.
	base := time.Now().Add(-time.Hour).Truncate(time.Second)
	for i := 0; i < 3; i++ {
		if err := lb.AddLogEntryToBuffer(&filer_pb.LogEntry{
			TsNs: base.Add(time.Duration(i) * 2 * time.Minute).UnixNano(), Data: []byte("x"), Key: []byte("k"),
		}); err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
	}
	earliest := lb.GetEarliestTime()
	if earliest.IsZero() {
		t.Fatal("expected in-memory data")
	}

	// firstTsFrom reports the timestamp of the first entry a read hands back.
	firstTsFrom := func(cursorTsNs int64) int64 {
		buf, _, pooled, err := lb.ReadFromBuffer(NewMessagePosition(cursorTsNs, gapResumeTestOffset))
		if err != nil {
			t.Fatalf("read at %v: %v", time.Unix(0, cursorTsNs), err)
		}
		if buf == nil {
			t.Fatalf("read at %v returned no data", time.Unix(0, cursorTsNs))
		}
		if pooled {
			defer lb.ReleaseMemory(buf)
		}
		_, ts, readErr := readTs(buf.Bytes(), 0)
		if readErr != nil {
			t.Fatalf("decode first entry: %v", readErr)
		}
		return ts
	}

	// The resume the resolvers issue must deliver the earliest entry itself.
	if got := firstTsFrom(earliest.UnixNano() - 1); got != earliest.UnixNano() {
		t.Fatalf("resume just below earliest starts at %v, want the earliest entry %v",
			time.Unix(0, got), earliest)
	}
	// Resuming exactly on earliest is the shape that loses it.
	if got := firstTsFrom(earliest.UnixNano()); got == earliest.UnixNano() {
		t.Fatal("expected a cursor exactly on earliest to skip the single-entry window (precondition)")
	}
}

// gapResumeTestOffset mirrors the sentinel the filer's gap resume carries.
const gapResumeTestOffset = -2

// TestGapResumeCursorReadsFromMemory pins the contract the filer's gap resolver
// depends on: the cursor it resumes with must be one this read actually serves.
// ReadFromBuffer only falls through to memory for a cursor below the in-memory
// window when the offset is a sentinel, and answers ResumeFromDiskError for
// every positive one -- a resume carrying a positive offset would bounce
// straight back to the resolver, which sees no progress and parks a subscriber
// whose data is sitting in the ring.
func TestGapResumeCursorReadsFromMemory(t *testing.T) {
	lb := NewLogBuffer("gap-resume", time.Minute, nil, nil, nil)
	defer lb.ShutdownLogBuffer()

	base := time.Now().Add(-time.Hour).Truncate(time.Second)
	for i := 0; i < 3; i++ {
		if err := lb.AddLogEntryToBuffer(&filer_pb.LogEntry{
			TsNs: base.Add(time.Duration(i) * time.Second).UnixNano(), Data: []byte("x"), Key: []byte("k"),
		}); err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
	}
	earliest := lb.GetEarliestTime()
	if earliest.IsZero() {
		t.Fatal("expected in-memory data")
	}

	// The resolver resumes at earliest itself, read inclusively.
	if buf, _, _, err := lb.ReadFromBuffer(NewMessagePosition(earliest.UnixNano(), -2)); err != nil || buf == nil {
		t.Fatalf("resume at earliest: buf=%v err=%v", buf != nil, err)
	}
	// A sentinel just below it is served too, so the exact boundary is not load-bearing.
	if buf, _, _, err := lb.ReadFromBuffer(NewMessagePosition(earliest.UnixNano()-1, -2)); err != nil || buf == nil {
		t.Fatalf("resume just below earliest: buf=%v err=%v", buf != nil, err)
	}
	// A positive offset below the window is refused: this is the shape a gap
	// resume must never take.
	if _, _, _, err := lb.ReadFromBuffer(NewMessagePosition(earliest.UnixNano()-1, 1)); err != ResumeFromDiskError {
		t.Fatalf("positive-offset cursor below the window: want ResumeFromDiskError, got %v", err)
	}
}
