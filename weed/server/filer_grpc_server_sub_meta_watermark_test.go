package weed_server

import (
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
)

// TestResolveAggReadHoldTsNs pins the aggregated delivery hold point: a
// subscriber may not read past the peers' delivery low-watermark (a source
// that is still catching up may merge older events in late), except that a
// watermark stalled beyond the settled horizon stops holding delivery back.
func TestResolveAggReadHoldTsNs(t *testing.T) {
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC).UnixNano()
	horizon := metadataGapSettledHorizon
	ago := func(d time.Duration) int64 { return now - int64(d) }

	cases := []struct {
		name        string
		watermarkNs int64
		wantHoldNs  int64
	}{
		{
			// Healthy: all peers signalled recently → deliver up to the watermark.
			name:        "healthy watermark ahead of horizon",
			watermarkNs: ago(2 * time.Second),
			wantHoldNs:  ago(2 * time.Second),
		},
		{
			// A peer stalled long ago: liveness escape takes over at the horizon.
			name:        "stalled watermark falls back to horizon",
			watermarkNs: ago(30 * time.Minute),
			wantHoldNs:  ago(horizon),
		},
		{
			// No signal from some peer yet (fresh cluster join): completeness
			// unknown → horizon bounds the hold.
			name:        "unknown watermark falls back to horizon",
			watermarkNs: 0,
			wantHoldNs:  ago(horizon),
		},
		{
			// Watermark exactly at the horizon: either bound gives the same
			// answer; pin the equality behavior.
			name:        "watermark exactly at horizon",
			watermarkNs: ago(horizon),
			wantHoldNs:  ago(horizon),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveAggReadHoldTsNs(tc.watermarkNs, now, horizon)
			if got != tc.wantHoldNs {
				t.Fatalf("hold=%d want %d", got, tc.wantHoldNs)
			}
		})
	}
}

func TestPreviousMinuteEndTsNs(t *testing.T) {
	ts := time.Date(2026, 7, 29, 12, 34, 56, 789, time.UTC)
	want := time.Date(2026, 7, 29, 12, 33, 59, 999999999, time.UTC).UnixNano()
	if got := previousMinuteEndTsNs(ts.UnixNano()); got != want {
		t.Fatalf("got %d want %d", got, want)
	}
	// A timestamp exactly on a minute boundary bounds to the end of the
	// preceding minute.
	onBoundary := time.Date(2026, 7, 29, 12, 34, 0, 0, time.UTC)
	wantBoundary := time.Date(2026, 7, 29, 12, 33, 59, 999999999, time.UTC).UnixNano()
	if got := previousMinuteEndTsNs(onBoundary.UnixNano()); got != wantBoundary {
		t.Fatalf("boundary: got %d want %d", got, wantBoundary)
	}
}

// TestAggWatermarkHoldRewindRedelivers pins the rewind invariant SubscribeMetadata
// relies on: when a read is held at an entry (errHeldByPeerWatermark), resuming
// from heldAtTsNs-1 re-delivers exactly the held entry and nothing before it —
// positions are exclusive, so the -1 keeps the held entry ahead of the cursor.
func TestAggWatermarkHoldRewindRedelivers(t *testing.T) {
	lb := log_buffer.NewLogBuffer("agg-hold", 10*time.Minute,
		func(logBuffer *log_buffer.LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {
		},
		func(startPosition log_buffer.MessagePosition, stopTsNs int64, eachLogEntryFn log_buffer.EachLogEntryFuncType) (log_buffer.MessagePosition, bool, error) {
			return startPosition, false, nil
		},
		func() {})
	defer lb.ShutdownLogBuffer()

	base := time.Now().Add(-time.Second)
	ts1 := base.UnixNano()
	ts2 := base.Add(100 * time.Millisecond).UnixNano()
	for i, ts := range []int64{ts1, ts2} {
		if err := lb.AddLogEntryToBuffer(&filer_pb.LogEntry{TsNs: ts, Data: []byte{byte(i)}, Key: []byte("k"), Offset: int64(i)}); err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
	}

	holdTsNs := ts1 // ts2 is beyond the hold point
	var heldAtTsNs int64
	var delivered []int64
	guarded := func(logEntry *filer_pb.LogEntry) (bool, error) {
		if logEntry.TsNs > holdTsNs {
			heldAtTsNs = logEntry.TsNs
			return false, errHeldByPeerWatermark
		}
		delivered = append(delivered, logEntry.TsNs)
		return false, nil
	}

	start := log_buffer.NewMessagePosition(ts1-1, -2)
	_, _, err := lb.LoopProcessLogData("hold-test", start, 0, func() bool { return false }, guarded)
	if !errors.Is(err, errHeldByPeerWatermark) {
		t.Fatalf("want held error, got %v", err)
	}
	if len(delivered) != 1 || delivered[0] != ts1 {
		t.Fatalf("before hold: delivered %v, want [%d]", delivered, ts1)
	}
	if heldAtTsNs != ts2 {
		t.Fatalf("heldAt=%d want %d", heldAtTsNs, ts2)
	}

	// Release the hold and resume from just below the held entry: it must be
	// re-delivered exactly once, without re-delivering ts1.
	holdTsNs = ts2
	delivered = nil
	resume := log_buffer.NewMessagePosition(heldAtTsNs-1, -2)
	_, _, err = lb.LoopProcessLogData("hold-test-resume", resume, 0, func() bool { return false }, guarded)
	if err != nil && !errors.Is(err, log_buffer.ResumeFromDiskError) {
		t.Fatalf("resume: %v", err)
	}
	if len(delivered) != 1 || delivered[0] != ts2 {
		t.Fatalf("after release: delivered %v, want [%d]", delivered, ts2)
	}
}
