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
