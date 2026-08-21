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

	"google.golang.org/grpc/peer"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
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
