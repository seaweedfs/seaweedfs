package weed_server

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
)

// sentRecord snapshots what a Send delivered at call time - the sender clears
// the envelope's Events after a batched Send, so asserting on the retained
// pointer would miss the nesting.
type sentRecord struct {
	hasNotification bool
	tsNs            int64
	flushedTsNs     int64
	nested          int
	nestedControl   int
}

// gatedRecordingStream blocks its first Send until the gate opens, letting a
// test queue several messages into the pipelined sender deterministically.
type gatedRecordingStream struct {
	gate     chan struct{}
	gateOnce sync.Once
	records  []sentRecord
}

func (s *gatedRecordingStream) Send(msg *filer_pb.SubscribeMetadataResponse) error {
	rec := sentRecord{
		hasNotification: msg.EventNotification != nil,
		tsNs:            msg.TsNs,
		flushedTsNs:     msg.FlushedTsNs,
		nested:          len(msg.Events),
	}
	for _, nested := range msg.Events {
		if nested.EventNotification == nil || nested.FlushedTsNs != 0 {
			rec.nestedControl++
		}
	}
	s.records = append(s.records, rec)
	s.gateOnce.Do(func() { <-s.gate })
	return nil
}

// TestPipelinedSenderControlMessagesNeverNested pins the batching rule flush
// reports rely on: a control message (nil EventNotification - a flush report
// or an idle heartbeat) must never ride in a batch's Events tail, where the
// peer aggregator's nil-guard would drop its watermark state. A starved flush
// watermark looks stalled, and the settled-horizon escape would then allow
// reads past it.
func TestPipelinedSenderControlMessagesNeverNested(t *testing.T) {
	stream := &gatedRecordingStream{gate: make(chan struct{})}
	sender := newPipelinedSender(stream, 16, true)

	oldTs := time.Now().Add(-time.Hour).UnixNano()
	if err := sender.Send(makeEvent("/d", "e1", oldTs)); err != nil {
		t.Fatalf("send e1: %v", err)
	}
	// While the stream is blocked on e1, queue a batchable backlog event, a
	// flush report, and another event - the drain loop sees them together.
	if err := sender.Send(makeEvent("/d", "e2", oldTs+1)); err != nil {
		t.Fatalf("send e2: %v", err)
	}
	if err := sender.Send(&filer_pb.SubscribeMetadataResponse{FlushedTsNs: 123}); err != nil {
		t.Fatalf("send flush report: %v", err)
	}
	if err := sender.Send(makeEvent("/d", "e3", oldTs+2)); err != nil {
		t.Fatalf("send e3: %v", err)
	}
	close(stream.gate)
	if err := sender.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	events, flushReports := 0, 0
	for _, rec := range stream.records {
		if rec.nestedControl > 0 {
			t.Fatalf("control message nested in a batch: %+v", rec)
		}
		if rec.hasNotification {
			events += 1 + rec.nested
		}
		if rec.flushedTsNs != 0 {
			flushReports++
			if rec.hasNotification || rec.nested != 0 {
				t.Fatalf("flush report not sent solo: %+v", rec)
			}
			if rec.flushedTsNs != 123 {
				t.Fatalf("flush report watermark = %d, want 123", rec.flushedTsNs)
			}
		}
	}
	if events != 3 {
		t.Fatalf("delivered %d events, want 3", events)
	}
	if flushReports != 1 {
		t.Fatalf("delivered %d flush reports, want 1", flushReports)
	}
}

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
