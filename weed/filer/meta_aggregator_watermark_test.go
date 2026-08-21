package filer

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
)

func newTestAggregator() *MetaAggregator {
	return &MetaAggregator{
		peerWatermarks:      make(map[pb.ServerAddress]int64),
		peerFlushWatermarks: make(map[pb.ServerAddress]int64),
		peerRemovedAtNs:     make(map[pb.ServerAddress]int64),
	}
}

// TestPeerWatermarkBookkeeping pins the per-peer delivery watermark semantics
// backing MetaAggregator.PeerLowWatermarkTsNs: the low-watermark is the minimum
// received-through timestamp across tracked peers, a peer that has not
// signalled yet holds it at zero (completeness unknown), advances are
// monotonic, and removed peers stop participating only after the grace.
func TestPeerWatermarkBookkeeping(t *testing.T) {
	ma := newTestAggregator()
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
}

// TestPeerWatermarkRemovalGrace pins the removal semantics: a removed peer is
// usually a flap (frozen or partitioned filer), so its watermarks keep
// holding the low-watermarks for the grace period - de-accounting it at once
// would let subscribers advance past its still-unflushed events. A re-add
// within the grace continues the values; a peer gone past the grace is
// dropped, so a decommission cannot pin the low-watermark, and its straggling
// signals cannot resurrect the entry.
func TestPeerWatermarkRemovalGrace(t *testing.T) {
	ma := newTestAggregator()
	a, b := pb.ServerAddress("filer-a:8888"), pb.ServerAddress("filer-b:8888")
	ma.initPeerWatermark(a)
	ma.initPeerWatermark(b)
	ma.advancePeerWatermark(a, 100)
	ma.advancePeerWatermark(b, 50)
	ma.advancePeerFlushWatermark(a, 100)
	ma.advancePeerFlushWatermark(b, 50)

	// Freshly removed: still participates (the flap case that loses data if
	// dropped at once).
	ma.markPeerWatermarkRemoved(b)
	if got := ma.PeerLowWatermarkTsNs(); got != 50 {
		t.Fatalf("within grace: low=%d want 50", got)
	}
	if got := ma.PeerLowFlushWatermarkTsNs(); got != 50 {
		t.Fatalf("within grace: flush low=%d want 50", got)
	}
	// Its draining stream may still advance it while marked.
	ma.advancePeerWatermark(b, 60)
	if got := ma.PeerLowWatermarkTsNs(); got != 60 {
		t.Fatalf("marked peer advance: low=%d want 60", got)
	}

	// Re-add within the grace: mark cleared, values continue.
	ma.initPeerWatermark(b)
	if _, marked := ma.peerRemovedAtNs[b]; marked {
		t.Fatalf("re-added peer still marked removed")
	}
	if got := ma.PeerLowWatermarkTsNs(); got != 60 {
		t.Fatalf("after re-add: low=%d want 60", got)
	}

	// Removal past the grace: dropped from both watermark sets. A duplicate
	// removal notification must not refresh the deadline (first mark wins).
	ma.markPeerWatermarkRemoved(b)
	ma.peerWatermarksLock.Lock()
	ma.peerRemovedAtNs[b] = time.Now().UnixNano() - int64(peerWatermarkRemovalGrace) - int64(time.Second)
	ma.peerWatermarksLock.Unlock()
	ma.markPeerWatermarkRemoved(b) // duplicate removal: must not reset the clock
	if got := ma.PeerLowWatermarkTsNs(); got != 100 {
		t.Fatalf("past grace: low=%d want 100", got)
	}
	if got := ma.PeerLowFlushWatermarkTsNs(); got != 100 {
		t.Fatalf("past grace: flush low=%d want 100", got)
	}

	// A straggling signal after the drop must not resurrect the entry.
	ma.advancePeerWatermark(b, 999)
	ma.advancePeerFlushWatermark(b, 999)
	if got := ma.PeerLowWatermarkTsNs(); got != 100 {
		t.Fatalf("after straggler: low=%d want 100", got)
	}
	if _, found := ma.peerWatermarks[b]; found {
		t.Fatalf("dropped peer resurrected in delivery watermark set")
	}
	if _, found := ma.peerFlushWatermarks[b]; found {
		t.Fatalf("dropped peer resurrected in flush watermark set")
	}
}

// TestPeerFlushWatermarkBookkeeping mirrors the delivery-watermark semantics
// for the flush watermark that bounds persisted-log reads: min across peers,
// zero until every peer has reported, monotonic advances.
func TestPeerFlushWatermarkBookkeeping(t *testing.T) {
	ma := newTestAggregator()
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
	_ = a
}

// closed reports whether ch has been signalled, without blocking.
func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// TestWatermarkAdvancedChansAreScoped pins what each wake channel promises. A
// held read re-runs a whole pass - a persisted-log listing in it - so waking
// one whose bound did not move is pure waste, and peers advance their delivery
// watermark on every event they stream.
func TestWatermarkAdvancedChansAreScoped(t *testing.T) {
	ma := newTestAggregator()
	a, b := pb.ServerAddress("filer-a:8888"), pb.ServerAddress("filer-b:8888")
	ma.initPeerWatermark(a)
	ma.initPeerWatermark(b)

	delivery, flush := ma.DeliveryWatermarkAdvancedChan(), ma.FlushWatermarkAdvancedChan()

	// One peer alone does not move either minimum: the other still pins both.
	ma.advancePeerWatermark(a, 100)
	ma.advancePeerFlushWatermark(a, 100)
	if closed(delivery) || closed(flush) {
		t.Fatalf("a single peer moved a minimum: delivery=%v flush=%v", closed(delivery), closed(flush))
	}

	// Delivery progress on every peer wakes the in-memory holds only.
	ma.advancePeerWatermark(b, 50)
	if !closed(delivery) {
		t.Fatal("delivery low-watermark rose without waking the in-memory holds")
	}
	if closed(flush) {
		t.Fatal("delivery progress woke the persisted-log holds, which it cannot release")
	}

	// And flush progress the persisted-log holds only.
	delivery = ma.DeliveryWatermarkAdvancedChan()
	ma.advancePeerFlushWatermark(b, 50)
	if !closed(flush) {
		t.Fatal("flush low-watermark rose without waking the persisted-log holds")
	}
	if closed(delivery) {
		t.Fatal("flush progress woke the in-memory holds, which it cannot release")
	}

	// A fresh channel is handed out after each signal, so the next park is on
	// the next rise rather than on one already consumed.
	flush = ma.FlushWatermarkAdvancedChan()
	if closed(flush) {
		t.Fatal("re-handed a signalled channel")
	}
	ma.advancePeerFlushWatermark(a, 200)
	if closed(flush) {
		t.Fatal("one peer above the minimum signalled a rise")
	}
	ma.advancePeerFlushWatermark(b, 200)
	if !closed(flush) {
		t.Fatal("the trailing peer catching up did not signal a rise")
	}
}
