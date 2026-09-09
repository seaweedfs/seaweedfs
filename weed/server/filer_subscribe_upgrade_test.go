package weed_server

// Regression tests for the aggregated-subscribe entrypoint's peer-discovery
// window. SubscribeMetadata delegates to the local loop when the aggregator
// knows no remote peers yet; these tests pin the upgrade: the local stream
// ends when a remote peer appears, so the client reconnects into the
// aggregated stream.

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
)

// startAggregatorWithoutPeers gives the harness an aggregator that tracks
// only self, the state of a filer whose gRPC server is up but whose master
// connection has not announced the other filers yet.
func (h *subscribeHarness) startAggregatorWithoutPeers() *filer.MetaAggregator {
	ma := filer.NewMetaAggregator(h.f, testSelfAddress, nil)
	ma.TrackPeerForTesting(testSelfAddress)
	h.f.MetaAggregator = ma
	h.t.Cleanup(ma.MetaLogBuffer.ShutdownLogBuffer)
	return ma
}

// TestSubscribeMetadataLocalStreamEndsWhenRemotePeerAppears pins the upgrade
// path: a stream that started local-only must end when the first remote peer
// appears, so the client reconnects into the aggregated stream.
func TestSubscribeMetadataLocalStreamEndsWhenRemotePeerAppears(t *testing.T) {
	h := newSubscribeHarness(t)

	ma := h.startAggregatorWithoutPeers()
	if ma.HasRemotePeers() {
		t.Fatal("test setup: aggregator must start without remote peers")
	}

	localTs := time.Now().UnixNano()
	h.append(localTs)

	// The subscriber connects inside the discovery window.
	r := h.subscribeAggregated(0)
	waitForEvents(t, r, []int64{localTs}, 3*time.Second)

	// The master announces a second filer, as OnPeerUpdate does in production.
	ma.TrackPeerForTesting(testPeerAddress)

	// The local stream must end so the client reconnects to the aggregated path.
	select {
	case err := <-r.done:
		if err == nil {
			t.Fatal("local stream ended cleanly; the upgrade must surface as an error so RetryUntil-driven followers reconnect")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("local-only stream stayed alive after a remote peer appeared; the subscriber is pinned to a partial cluster view")
	}
}

// TestSubscribeMetadataAggregatedPathAfterPeerArrival pins the other half:
// after the upgrade, a re-subscribe from the last delivered offset takes the
// aggregated path and receives the peer's events.
func TestSubscribeMetadataAggregatedPathAfterPeerArrival(t *testing.T) {
	h := newSubscribeHarness(t)

	ma := h.startAggregatorWithoutPeers()
	if ma.HasRemotePeers() {
		t.Fatal("test setup: aggregator must start without remote peers")
	}

	localTs := time.Now().UnixNano()
	h.append(localTs)
	r := h.subscribeAggregated(0)
	waitForEvents(t, r, []int64{localTs}, 3*time.Second)

	ma.TrackPeerForTesting(testPeerAddress)
	select {
	case <-r.done:
	case <-time.After(5 * time.Second):
		t.Fatal("local-only stream stayed alive after a remote peer appeared")
	}

	// The client reconnects from the last delivered offset; the aggregated
	// path now serves the peer's events.
	peerTs := time.Now().UnixNano()
	h.appendAggregated(peerTs)
	reportPeers(ma, peerTs)
	r2 := h.subscribeAggregated(localTs)
	waitForEvents(t, r2, []int64{peerTs}, 3*time.Second)
}

// TestSubscribeMetadataLocalStreamPersistsWithoutPeers pins the boundary: on
// a standalone filer the local stream keeps serving indefinitely.
func TestSubscribeMetadataLocalStreamPersistsWithoutPeers(t *testing.T) {
	h := newSubscribeHarness(t)

	ma := h.startAggregatorWithoutPeers()
	if ma.HasRemotePeers() {
		t.Fatal("test setup: aggregator must start without remote peers")
	}

	localTs := time.Now().UnixNano()
	h.append(localTs)
	r := h.subscribeAggregated(0)
	waitForEvents(t, r, []int64{localTs}, 3*time.Second)

	// No peer ever appears; the stream must still be alive and still deliver.
	moreTs := time.Now().UnixNano()
	h.append(moreTs)
	waitForEventsAtLeastOnce(t, r, []int64{localTs, moreTs}, 3*time.Second)

	select {
	case err := <-r.done:
		t.Fatalf("local stream ended (%v) on a standalone filer; it must keep serving", err)
	case <-time.After(200 * time.Millisecond):
	}
}
