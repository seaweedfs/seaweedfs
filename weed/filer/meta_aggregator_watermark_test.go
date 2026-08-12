package filer

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
)

// TestPeerWatermarkBookkeeping pins the per-peer delivery watermark semantics
// backing MetaAggregator.PeerLowWatermarkTsNs: the low-watermark is the minimum
// received-through timestamp across tracked peers, a peer that has not
// signalled yet holds it at zero (completeness unknown), advances are
// monotonic, and removed peers stop participating.
func TestPeerWatermarkBookkeeping(t *testing.T) {
	ma := &MetaAggregator{
		peerWatermarks:      make(map[pb.ServerAddress]int64),
		peerFlushWatermarks: make(map[pb.ServerAddress]int64),
	}
	a, b := pb.ServerAddress("filer-a:8888"), pb.ServerAddress("filer-b:8888")

	if got := ma.PeerLowWatermarkTsNs(); got != 0 {
		t.Fatalf("no peers: low=%d want 0", got)
	}

	// A tracked-but-silent peer pins the low-watermark at zero.
	ma.initPeerWatermark(a)
	ma.initPeerWatermark(b)
	ma.advancePeerWatermark(a, 100)
	if got := ma.PeerLowWatermarkTsNs(); got != 0 {
		t.Fatalf("silent peer: low=%d want 0", got)
	}

	// Both signalled: low is the minimum.
	ma.advancePeerWatermark(b, 50)
	if got := ma.PeerLowWatermarkTsNs(); got != 50 {
		t.Fatalf("low=%d want 50", got)
	}

	// Advances are monotonic: a stale (lower) signal cannot regress.
	ma.advancePeerWatermark(b, 40)
	if got := ma.PeerLowWatermarkTsNs(); got != 50 {
		t.Fatalf("after stale signal: low=%d want 50", got)
	}

	// Reconnect keeps the prior value (init does not reset).
	ma.initPeerWatermark(b)
	if got := ma.PeerLowWatermarkTsNs(); got != 50 {
		t.Fatalf("after re-init: low=%d want 50", got)
	}

	// A removed peer no longer holds the watermark back.
	ma.deletePeerWatermark(b)
	if got := ma.PeerLowWatermarkTsNs(); got != 100 {
		t.Fatalf("after delete: low=%d want 100", got)
	}
}

// TestPeerFlushWatermarkBookkeeping mirrors the delivery-watermark semantics
// for the flush watermark that bounds persisted-log reads: min across peers,
// zero until every peer has reported, monotonic advances.
func TestPeerFlushWatermarkBookkeeping(t *testing.T) {
	ma := &MetaAggregator{
		peerWatermarks:      make(map[pb.ServerAddress]int64),
		peerFlushWatermarks: make(map[pb.ServerAddress]int64),
	}
	a, b := pb.ServerAddress("filer-a:8888"), pb.ServerAddress("filer-b:8888")

	ma.initPeerWatermark(a)
	ma.initPeerWatermark(b)
	ma.advancePeerFlushWatermark(a, 200)
	if got := ma.PeerLowFlushWatermarkTsNs(); got != 0 {
		t.Fatalf("unreported peer: low=%d want 0", got)
	}
	ma.advancePeerFlushWatermark(b, 150)
	if got := ma.PeerLowFlushWatermarkTsNs(); got != 150 {
		t.Fatalf("low=%d want 150", got)
	}
	ma.advancePeerFlushWatermark(b, 120) // stale report cannot regress
	if got := ma.PeerLowFlushWatermarkTsNs(); got != 150 {
		t.Fatalf("after stale: low=%d want 150", got)
	}
	ma.deletePeerWatermark(b)
	if got := ma.PeerLowFlushWatermarkTsNs(); got != 200 {
		t.Fatalf("after delete: low=%d want 200", got)
	}
}
