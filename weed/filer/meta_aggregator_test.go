package filer

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func TestOnPeerUpdateRepeatedAdd(t *testing.T) {
	peer := pb.ServerAddress("127.0.0.1:1")
	ma := NewMetaAggregator(nil, pb.ServerAddress("127.0.0.1:2"), nil)
	defer ma.Shutdown()

	add := &master_pb.ClusterNodeUpdate{Address: string(peer), IsAdd: true}
	ma.OnPeerUpdate(add, time.Now())
	first, found := ma.peerChans[peer]
	if !found {
		t.Fatal("expecting a subscription after the first add")
	}

	ma.OnPeerUpdate(add, time.Now())
	if ma.peerChans[peer] != first {
		t.Fatal("expecting the same subscription after a repeated add")
	}
	select {
	case <-first:
		t.Fatal("expecting the subscription to stay alive after a repeated add")
	default:
	}

	ma.OnPeerUpdate(&master_pb.ClusterNodeUpdate{Address: string(peer)}, time.Now())
	if _, found := ma.peerChans[peer]; found {
		t.Fatal("expecting the subscription to be removed")
	}
}

// Shutdown must close every peer's stopChan and wait for the loops, which is
// what lets each loop's deferred offset save run before the store closes.
func TestMetaAggregatorShutdownStopsPeerLoops(t *testing.T) {
	ma := NewMetaAggregator(nil, pb.ServerAddress("127.0.0.1:2"), nil)

	stopChan := make(chan struct{})
	peer := pb.ServerAddress("127.0.0.1:1")
	ma.peerChans[peer] = stopChan
	ma.peerWg.Add(1)
	exited := make(chan struct{})
	go func() {
		defer ma.peerWg.Done()
		<-stopChan
		close(exited)
	}()

	done := make(chan struct{})
	go func() {
		ma.Shutdown()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Shutdown did not return")
	}
	select {
	case <-exited:
	default:
		t.Fatal("peer loop was not stopped")
	}
}

// A peer removed and re-added must resume where its previous subscription
// was delivered through: subscribing from the update's arrival time drops
// whatever the peer logged while its stream was down.
func TestPeerResumeStartFromSurvivesRemoval(t *testing.T) {
	peer := pb.ServerAddress("127.0.0.1:1")
	ma := NewMetaAggregator(nil, pb.ServerAddress("127.0.0.1:2"), nil)
	defer ma.Shutdown()

	subscribeStarts := make(chan int64, 3)
	ma.peerSubscribeLoop = func(_ *Filer, _ pb.ServerAddress, _ pb.ServerAddress, startFrom time.Time, stopChan chan struct{}) {
		subscribeStarts <- startFrom.UnixNano()
		<-stopChan
	}
	nextSubscribeStart := func() int64 {
		select {
		case tsNs := <-subscribeStarts:
			return tsNs
		case <-time.After(3 * time.Second):
			t.Fatal("no peer subscription started")
			return 0
		}
	}

	add := &master_pb.ClusterNodeUpdate{Address: string(peer), IsAdd: true}
	firstStart := time.Now()
	ma.OnPeerUpdate(add, firstStart)
	if got := nextSubscribeStart(); got != firstStart.UnixNano() {
		t.Fatalf("expecting first subscribe at %d, got %d", firstStart.UnixNano(), got)
	}

	deliveredThrough := time.Now().Add(-time.Hour).UnixNano()
	ma.advancePeerWatermark(peer, deliveredThrough)
	if got := ma.peerResumeStartFrom(peer, time.Now()); got.UnixNano() != deliveredThrough {
		t.Fatalf("expecting resume at %d, got %d", deliveredThrough, got.UnixNano())
	}

	// Removal and even the watermark-grace expiry keep the resume position.
	ma.OnPeerUpdate(&master_pb.ClusterNodeUpdate{Address: string(peer)}, time.Now())
	ma.peerWatermarksLock.Lock()
	ma.peerRemovedAtNs[peer] = time.Now().Add(-2 * peerWatermarkRemovalGrace).UnixNano()
	ma.dropExpiredRemovedPeersLocked()
	ma.peerWatermarksLock.Unlock()

	ma.OnPeerUpdate(add, time.Now())
	if got := nextSubscribeStart(); got != deliveredThrough {
		t.Fatalf("expecting re-added peer to resume at %d, got %d", deliveredThrough, got)
	}

	// A peer gone past the retention is forgotten and starts afresh.
	ma.OnPeerUpdate(&master_pb.ClusterNodeUpdate{Address: string(peer)}, time.Now())
	ma.peerWatermarksLock.Lock()
	ma.peerRemovedAtNs[peer] = time.Now().Add(-2 * peerResumeRetention).UnixNano()
	ma.dropExpiredRemovedPeersLocked()
	_, retained := ma.peerResumeTsNs[peer]
	ma.peerWatermarksLock.Unlock()
	if retained {
		t.Fatal("expecting the resume point to be forgotten past the retention")
	}
	freshStart := time.Now()
	ma.OnPeerUpdate(add, freshStart)
	if got := nextSubscribeStart(); got != freshStart.UnixNano() {
		t.Fatalf("expecting forgotten peer to start at %d, got %d", freshStart.UnixNano(), got)
	}

	// A first-seen peer, or a caller start already behind the recorded
	// position, keeps the caller's start.
	other := pb.ServerAddress("127.0.0.1:3")
	start := time.Now().Add(-time.Minute)
	if got := ma.peerResumeStartFrom(other, start); !got.Equal(start) {
		t.Fatalf("expecting caller start %v for unseen peer, got %v", start, got)
	}
	early := time.Unix(0, deliveredThrough).Add(-time.Second)
	if got := ma.peerResumeStartFrom(peer, early); !got.Equal(early) {
		t.Fatalf("expecting caller start %v behind resume, got %v", early, got)
	}
}

// After Shutdown begins, a peer-add update arriving on the still-connected
// master watch must not start a subscription: it would have no stop signal
// and could run past the store close.
func TestOnPeerUpdateAfterShutdownRejected(t *testing.T) {
	ma := NewMetaAggregator(nil, pb.ServerAddress("127.0.0.1:2"), nil)
	ma.Shutdown()

	peer := pb.ServerAddress("127.0.0.1:1")
	ma.OnPeerUpdate(&master_pb.ClusterNodeUpdate{Address: string(peer), IsAdd: true}, time.Now())

	ma.peerChansLock.Lock()
	_, found := ma.peerChans[peer]
	ma.peerChansLock.Unlock()
	if found {
		t.Fatal("peer subscription started after shutdown")
	}
}
