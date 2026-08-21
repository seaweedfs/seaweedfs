package weed_server

// End-to-end tests for the aggregated (SubscribeMetadata) loop's peer
// watermark holds. Peers report their progress at their own pace, so on a
// cluster that keeps writing there is almost always an entry newer than the
// low-watermark: a hold is the normal state, and has to be quiet and paced
// rather than logged and retried per arriving event.

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc/peer"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
)

const (
	testSelfAddress = pb.ServerAddress("self:8888")
	testPeerAddress = pb.ServerAddress("peer:8888")
)

// startAggregator gives the harness a meta aggregator with one remote peer, so
// SubscribeMetadata takes the aggregated path instead of delegating to
// SubscribeLocalMetadata.
func (h *subscribeHarness) startAggregator() *filer.MetaAggregator {
	ma := filer.NewMetaAggregator(h.f, testSelfAddress, nil)
	ma.TrackPeerForTesting(testSelfAddress)
	ma.TrackPeerForTesting(testPeerAddress)
	h.f.MetaAggregator = ma
	h.t.Cleanup(ma.MetaLogBuffer.ShutdownLogBuffer)
	return ma
}

// reportPeers stands in for both peers' streams reporting through tsNs.
func reportPeers(ma *filer.MetaAggregator, tsNs int64) {
	ma.ReportPeerWatermarksForTesting(testSelfAddress, tsNs, tsNs)
	ma.ReportPeerWatermarksForTesting(testPeerAddress, tsNs, tsNs)
}

func (h *subscribeHarness) appendAggregated(tsNs int64) {
	if err := h.f.MetaAggregator.MetaLogBuffer.AddLogEntryToBuffer(testEvent(tsNs, fmt.Sprintf("a-%d", tsNs))); err != nil {
		h.t.Fatalf("append aggregated: %v", err)
	}
}

func (h *subscribeHarness) subscribeAggregated(sinceNs int64) *runningSubscribe {
	// A peer in the context keeps findClientAddress quiet, so an ERROR count
	// across the run sees only what the subscribe loop itself writes.
	ctx, cancel := context.WithCancel(peer.NewContext(context.Background(),
		&peer.Peer{Addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345}}))
	stream := &fakeSubscribeStream{ctx: ctx}
	req := &filer_pb.SubscribeMetadataRequest{
		ClientName:  "agg-loop-test",
		ClientId:    11,
		ClientEpoch: 1,
		SinceNs:     sinceNs,
	}
	r := &runningSubscribe{stream: stream, cancel: cancel, done: make(chan error, 1), finished: make(chan struct{})}
	go func() {
		r.done <- h.fs.SubscribeMetadata(req, stream)
		close(r.finished)
	}()
	h.t.Cleanup(func() {
		cancel()
		select {
		case <-r.finished:
		case <-time.After(5 * time.Second):
			h.t.Error("aggregated subscribe loop did not exit on cancel")
		}
	})
	return r
}

// heldSubscriber starts a subscriber whose peers reported a moment ago and
// then went quiet - the hold sits just behind live writes, exactly where a
// quiet peer's idle heartbeat leaves it - and returns it with the write
// timestamp the peers last reported.
func (h *subscribeHarness) heldSubscriber() (*filer.MetaAggregator, *runningSubscribe, int64) {
	ma := h.startAggregator()
	frozen := time.Now().Add(-500 * time.Millisecond).UnixNano()
	reportPeers(ma, frozen)
	return ma, h.subscribeAggregated(frozen), frozen
}

// writeFor appends an entry every 2ms for d, reporting the peers through the
// PREVIOUS entry each time when report is set: the live-cluster shape, where
// the low-watermark keeps moving but always trails the newest entry, so every
// read ends held.
func (h *subscribeHarness) writeFor(ma *filer.MetaAggregator, d time.Duration, report bool) (written []int64, elapsed time.Duration) {
	start := time.Now()
	for stop := time.After(d); ; {
		select {
		case <-stop:
			if report && len(written) > 0 {
				reportPeers(ma, written[len(written)-1])
			}
			return written, time.Since(start)
		default:
		}
		ts := time.Now().UnixNano()
		h.appendAggregated(ts)
		if report && len(written) > 0 {
			reportPeers(ma, written[len(written)-1])
		}
		written = append(written, ts)
		time.Sleep(2 * time.Millisecond)
	}
}

// heldReads reads back the hold counter the loop keeps in place of the log
// line, which is also how an operator sees a held subscriber now.
func heldReads() int {
	var total int
	for _, scope := range []string{"memory", "disk"} {
		total += int(testutil.ToFloat64(stats.FilerSubscribeWatermarkHolds.WithLabelValues(scope)))
	}
	return total
}

// waitForEventsAtLeastOnce asserts every want arrives, in order. The cursor a
// hold rewinds to is inclusive of the last delivered entry, so each cycle
// repeats it - at-least-once, which is the contract; a skip is not.
func waitForEventsAtLeastOnce(t *testing.T, r *runningSubscribe, want []int64, timeout time.Duration) {
	t.Helper()
	last := want[len(want)-1]
	deadline := time.Now().Add(timeout)
	var got []int64
	for time.Now().Before(deadline) {
		got = eventTimestamps(r.stream.snapshot())
		if len(got) > 0 && got[len(got)-1] >= last {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	seen := make(map[int64]bool, len(got))
	var prev int64
	for _, ts := range got {
		if ts < prev {
			t.Fatalf("delivered %v after %v: out of order", time.Unix(0, ts), time.Unix(0, prev))
		}
		prev, seen[ts] = ts, true
	}
	for i, ts := range want {
		if !seen[ts] {
			t.Fatalf("event %d of %d (%v) never delivered", i+1, len(want), time.Unix(0, ts))
		}
	}
}

// TestSubscribeLoop_AggregatedHoldIsQuiet: every held read used to log an
// ERROR, so a busy cluster wrote one line per arriving event.
func TestSubscribeLoop_AggregatedHoldIsQuiet(t *testing.T) {
	h := newSubscribeHarness(t)
	ma, r, _ := h.heldSubscriber()

	errorsBefore := glog.Stats.Error.Lines()
	written, elapsed := h.writeFor(ma, time.Second, false)
	errorLines := glog.Stats.Error.Lines() - errorsBefore

	t.Logf("%d writes over %v produced %d ERROR lines", len(written), elapsed, errorLines)
	if errorLines > 0 {
		t.Fatalf("a held read logged %d ERROR lines over %d writes; holds are control flow", errorLines, len(written))
	}
	if got := eventTimestamps(r.stream.snapshot()); len(got) > 0 {
		t.Fatalf("delivered %d events past the peers' watermark", len(got))
	}
}

// TestSubscribeLoop_AggregatedHoldIsPaced is the other half: a held read used
// to wait on the buffer's data channel, which the very next write signalled,
// so the loop re-ran a whole pass - a log file listing in it - per event.
// Arriving data cannot release a hold; only peer progress can.
func TestSubscribeLoop_AggregatedHoldIsPaced(t *testing.T) {
	h := newSubscribeHarness(t)
	// Long enough that a hold paced by the retry interval alone is far below
	// one hold per write, short enough to keep the test quick.
	prevRetry := unflushedGapRetryInterval
	unflushedGapRetryInterval = 200 * time.Millisecond
	t.Cleanup(func() { unflushedGapRetryInterval = prevRetry })
	ma, _, _ := h.heldSubscriber()

	holdsBefore := heldReads()
	written, elapsed := h.writeFor(ma, time.Second, false)
	holds := heldReads() - holdsBefore

	t.Logf("%d writes over %v produced %d holds", len(written), elapsed, holds)
	// The watermarks never moved, so the retry interval alone paces the holds.
	if maxHolds := int(elapsed/unflushedGapRetryInterval) + 2; holds > maxHolds {
		t.Fatalf("held %d times in %v, want at most %d: a hold must wait for peer progress, not for the next write",
			holds, elapsed, maxHolds)
	}
}

// TestSubscribeLoop_AggregatedHoldReleasesOnPeerProgress pins what pacing the
// hold may not cost: once the peers report through the held entry it must be
// delivered, without waiting out the retry interval.
func TestSubscribeLoop_AggregatedHoldReleasesOnPeerProgress(t *testing.T) {
	h := newSubscribeHarness(t)
	// Far longer than this test runs: only the watermark signal can deliver.
	prevRetry := unflushedGapRetryInterval
	unflushedGapRetryInterval = 30 * time.Second
	t.Cleanup(func() { unflushedGapRetryInterval = prevRetry })
	ma, r, _ := h.heldSubscriber()

	ts := time.Now().UnixNano()
	h.appendAggregated(ts)
	assertNoEventsFor(t, r, 200*time.Millisecond)

	reportPeers(ma, ts)
	waitForEvents(t, r, []int64{ts}, 3*time.Second)
}

// TestSubscribeLoop_AggregatedHoldCoalescesPeerProgress covers the rest of the
// cost: peers advance their watermark on every event they stream, so releasing
// a hold per advance is the same pass-per-event storm as releasing per write,
// just without the log lines.
func TestSubscribeLoop_AggregatedHoldCoalescesPeerProgress(t *testing.T) {
	h := newSubscribeHarness(t)
	ma, r, _ := h.heldSubscriber()

	holdsBefore := heldReads()
	written, elapsed := h.writeFor(ma, time.Second, true)
	holds := heldReads() - holdsBefore

	t.Logf("%d writes over %v produced %d holds", len(written), elapsed, holds)
	if maxHolds := int(elapsed/heldWakeFloor) + 2; holds > maxHolds {
		t.Fatalf("held %d times in %v, want at most %d: releases must coalesce", holds, elapsed, maxHolds)
	}
	// Coalescing may not lose anything the peers reported through.
	waitForEventsAtLeastOnce(t, r, written, 3*time.Second)
}

// TestPeerDeliveryClaimPacing pins what a filer with peers owes them: the idle
// heartbeat on its local stream is a delivery claim every aggregated
// subscriber in the cluster holds at, so it refreshes at the claim interval,
// not at the keepalive interval that used to park them ~5s behind live writes.
func TestPeerDeliveryClaimPacing(t *testing.T) {
	h := newSubscribeHarness(t)
	req := &filer_pb.SubscribeMetadataRequest{ClientSupportsIdleHeartbeat: true}
	lb := h.f.LocalMetaLogBuffer
	// Claimed a second ago: stale for a delivery claim, fresh for a keepalive.
	claimedAtNs := time.Now().Add(-time.Second).UnixNano()

	s := &collectingStream{}
	if got := h.fs.maybeSendIdleHeartbeat(req, s, lb, 0, 0, claimedAtNs); got != claimedAtNs || len(s.messages) != 0 {
		t.Fatalf("standalone filer sent %d heartbeats early", len(s.messages))
	}

	h.startAggregator()
	s = &collectingStream{}
	if got := h.fs.maybeSendIdleHeartbeat(req, s, lb, 0, 0, claimedAtNs); got == claimedAtNs || len(s.messages) != 1 {
		t.Fatalf("filer with peers sent %d delivery claims, want 1", len(s.messages))
	}
	if s.messages[0].TsNs <= claimedAtNs {
		t.Fatalf("claim ts %d did not advance past %d", s.messages[0].TsNs, claimedAtNs)
	}
}
