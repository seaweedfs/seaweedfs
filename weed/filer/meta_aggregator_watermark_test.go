package filer

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func newTestAggregator() *MetaAggregator {
	ma := &MetaAggregator{
		peerChans:           make(map[pb.ServerAddress]chan struct{}),
		peerWatermarks:      make(map[pb.ServerAddress]int64),
		peerFlushWatermarks: make(map[pb.ServerAddress]int64),
		peerRemovedAtNs:     make(map[pb.ServerAddress]int64),
		peerResumeTsNs:      make(map[pb.ServerAddress]int64),

		peerUnreachableSinceNs: make(map[pb.ServerAddress]int64),
		peerSharesStore:        make(map[pb.ServerAddress]bool),
	}
	ma.peerSubscribeLoop = ma.loopSubscribeToOneFiler
	return ma
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
	if got := ma.peerResumeTsNs[b]; got != 60 {
		t.Fatalf("after straggler: resume=%d want 60", got)
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

// TestUnreachablePeerStopsPinningAfterTheGrace pins the rule for a peer whose
// removal never reached this filer (it died across a master leader change):
// its zero watermark holds the low-watermark only until the connection
// failures have lasted peerUnreachableRemovalAfter; it is then marked removed,
// drops after the usual grace, and is tracked again the moment it answers.
func TestUnreachablePeerStopsPinningAfterTheGrace(t *testing.T) {
	ma := newTestAggregator()
	a, b := pb.ServerAddress("filer-a:8888"), pb.ServerAddress("filer-b:8888")
	ma.initPeerWatermark(a)
	ma.initPeerWatermark(b)
	ma.advancePeerWatermark(a, 100)
	stopB := make(chan struct{})
	ma.peerChans[b] = stopB

	t0 := time.Now().UnixNano()
	if ma.notePeerUnreachable(b, stopB, t0) || ma.notePeerUnreachable(b, stopB, t0+int64(peerUnreachableRemovalAfter)-1) {
		t.Fatal("a peer unreachable for less than the bound must not be marked")
	}
	if !ma.notePeerUnreachable(b, stopB, t0+int64(peerUnreachableRemovalAfter)) {
		t.Fatal("a peer unreachable for the bound must be marked removed")
	}
	if ma.notePeerUnreachable(b, stopB, t0+int64(peerUnreachableRemovalAfter)+1) {
		t.Fatal("the mark must be reported once")
	}
	if got := ma.PeerLowWatermarkTsNs(); got != 0 {
		t.Fatalf("within the removal grace: low=%d want 0", got)
	}
	ma.peerWatermarksLock.Lock()
	ma.peerRemovedAtNs[b] = time.Now().UnixNano() - int64(peerWatermarkRemovalGrace) - 1
	ma.peerWatermarksLock.Unlock()
	if got := ma.PeerLowWatermarkTsNs(); got != 100 {
		t.Fatalf("past the grace the unreachable peer must not pin: low=%d want 100", got)
	}

	// It answers again: tracked from zero until it signals.
	ma.notePeerReachable(b, stopB)
	if got := ma.PeerLowWatermarkTsNs(); got != 0 {
		t.Fatalf("re-tracked peer before its first claim: low=%d want 0", got)
	}
	ma.advancePeerWatermark(b, 70)
	if got := ma.PeerLowWatermarkTsNs(); got != 70 {
		t.Fatalf("after its claim: low=%d want 70", got)
	}
	if ma.notePeerUnreachable(b, stopB, t0+10*int64(peerUnreachableRemovalAfter)) {
		t.Fatal("a fresh outage must start a fresh clock")
	}
}

// A subscription whose signature read succeeded just before the master
// removed the peer must not clear the removal: its loop is about to exit, so
// nothing would advance or drop the watermark it re-tracked.
func TestStaleSubscriptionDoesNotUndoMasterRemoval(t *testing.T) {
	ma := newTestAggregator()
	ma.peerSubscribeLoop = func(_ *Filer, _, _ pb.ServerAddress, _ time.Time, _ chan struct{}) {}
	a, b := pb.ServerAddress("filer-a:8888"), pb.ServerAddress("filer-b:8888")
	ma.OnPeerUpdate(&master_pb.ClusterNodeUpdate{Address: string(a), IsAdd: true}, time.Now())
	ma.OnPeerUpdate(&master_pb.ClusterNodeUpdate{Address: string(b), IsAdd: true}, time.Now())
	ma.advancePeerWatermark(a, 100)
	ma.advancePeerWatermark(b, 50)
	stopB := ma.peerChans[b]

	ma.OnPeerUpdate(&master_pb.ClusterNodeUpdate{Address: string(b), IsAdd: false}, time.Now())
	ma.notePeerReachable(b, stopB)

	ma.peerWatermarksLock.Lock()
	_, marked := ma.peerRemovedAtNs[b]
	ma.peerRemovedAtNs[b] = time.Now().UnixNano() - int64(peerWatermarkRemovalGrace) - 1
	ma.peerWatermarksLock.Unlock()
	if !marked {
		t.Fatal("a stale subscription cleared the master's removal mark")
	}
	if got := ma.PeerLowWatermarkTsNs(); got != 100 {
		t.Fatalf("the removed peer still pins past the grace: low=%d want 100", got)
	}
}

// A master removal forgets the peer's unreachable clock, and the removed
// peer's exiting loop cannot start a new one: departed addresses do not
// accumulate, and a re-added peer starts a fresh outage clock.
func TestMasterRemovalForgetsUnreachableClock(t *testing.T) {
	ma := newTestAggregator()
	ma.peerSubscribeLoop = func(_ *Filer, _, _ pb.ServerAddress, _ time.Time, _ chan struct{}) {}
	b := pb.ServerAddress("filer-b:8888")
	ma.OnPeerUpdate(&master_pb.ClusterNodeUpdate{Address: string(b), IsAdd: true}, time.Now())
	stopB := ma.peerChans[b]
	t0 := time.Now().UnixNano()
	ma.notePeerUnreachable(b, stopB, t0)

	ma.OnPeerUpdate(&master_pb.ClusterNodeUpdate{Address: string(b), IsAdd: false}, time.Now())
	ma.notePeerUnreachable(b, stopB, t0+1)
	if n := len(ma.peerUnreachableSinceNs); n != 0 {
		t.Fatalf("removed peer still has an unreachable clock: %d entries", n)
	}

	ma.OnPeerUpdate(&master_pb.ClusterNodeUpdate{Address: string(b), IsAdd: true}, time.Now())
	if ma.notePeerUnreachable(b, ma.peerChans[b], t0+int64(peerUnreachableRemovalAfter)) {
		t.Fatal("a re-added peer inherited the previous outage clock")
	}
}

// A peer still unreachable once its removal mark has been forgotten is no
// longer tracked, so it is not reported as newly marked on every retry.
func TestLongUnreachablePeerIsReportedOnce(t *testing.T) {
	ma := newTestAggregator()
	b := pb.ServerAddress("filer-b:8888")
	ma.initPeerWatermark(b)
	stopB := make(chan struct{})
	ma.peerChans[b] = stopB
	t0 := time.Now().UnixNano()
	ma.notePeerUnreachable(b, stopB, t0)
	if !ma.notePeerUnreachable(b, stopB, t0+int64(peerUnreachableRemovalAfter)) {
		t.Fatal("a peer unreachable for the bound must be marked removed")
	}
	ma.peerWatermarksLock.Lock()
	ma.peerRemovedAtNs[b] = time.Now().UnixNano() - int64(peerResumeRetention) - 1
	ma.dropExpiredRemovedPeersLocked()
	ma.peerWatermarksLock.Unlock()
	if ma.notePeerUnreachable(b, stopB, t0+2*int64(peerUnreachableRemovalAfter)) {
		t.Fatal("an untracked peer was reported as newly marked")
	}
}

func TestSharesStoreFollowsTheConnectedSubscription(t *testing.T) {
	ma := newTestAggregator()
	ma.peerSubscribeLoop = func(_ *Filer, _, _ pb.ServerAddress, _ time.Time, _ chan struct{}) {}
	shared, separate := pb.ServerAddress("filer-a:8888"), pb.ServerAddress("filer-b:8888")
	ma.OnPeerUpdate(&master_pb.ClusterNodeUpdate{Address: string(shared), IsAdd: true}, time.Now())
	ma.OnPeerUpdate(&master_pb.ClusterNodeUpdate{Address: string(separate), IsAdd: true}, time.Now())
	if ma.SharesStore(shared) {
		t.Fatal("a peer that has not connected yet must not count as sharing the store")
	}
	stopShared := ma.peerChans[shared]
	ma.notePeerStore(shared, stopShared, true)
	ma.notePeerStore(separate, ma.peerChans[separate], false)
	if !ma.SharesStore(shared) || ma.SharesStore(separate) {
		t.Fatalf("shared=%v separate=%v, want true false", ma.SharesStore(shared), ma.SharesStore(separate))
	}

	ma.OnPeerUpdate(&master_pb.ClusterNodeUpdate{Address: string(shared), IsAdd: false}, time.Now())
	ma.notePeerStore(shared, stopShared, true)
	if ma.SharesStore(shared) {
		t.Fatal("a removed peer, or its exiting subscription, must not keep the shared mark")
	}
}
