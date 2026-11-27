package topology

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/sequence"
)

func newGrowthFenceTopology() (*Topology, *VolumeGrowOption) {
	topo := NewTopology("test", sequence.NewMemorySequencer(), 32*1024, 5, false)
	return topo, &VolumeGrowOption{Collection: "bucket", collection: topo.getOrCreateCollection("bucket")}
}

func TestGrowthFenceWaitsForTheVolumeInFlight(t *testing.T) {
	topo, option := newGrowthFenceTopology()
	c := option.collection

	if !topo.lockForGrowth(option) {
		t.Fatal("growth refused by a collection nothing is deleting")
	}
	fenced := make(chan func(), 1)
	go func() { fenced <- c.FenceGrowth() }()
	select {
	case <-fenced:
		t.Fatal("FenceGrowth returned while growth was still creating a volume")
	case <-time.After(100 * time.Millisecond):
	}
	c.unlockForGrowth()

	var unfence func()
	select {
	case unfence = <-fenced:
	case <-time.After(5 * time.Second):
		t.Fatal("FenceGrowth did not return after the volume in flight finished")
	}
	if topo.lockForGrowth(option) {
		t.Fatal("growth entered a collection being deleted")
	}

	unfence()
	if !topo.lockForGrowth(option) {
		t.Fatal("growth still refused after a failed delete lifted the fence")
	}
	c.unlockForGrowth()
}

func TestGrowthStaysOutOfARecreatedCollection(t *testing.T) {
	topo, option := newGrowthFenceTopology()

	option.collection.FenceGrowth()
	topo.DeleteCollection("bucket")
	topo.getOrCreateCollection("bucket")

	if topo.lockForGrowth(option) {
		t.Fatal("growth that started before the delete entered the collection recreated under the same name")
	}
}

func TestGrowthFollowsACollectionDroppedForBeingEmpty(t *testing.T) {
	topo, option := newGrowthFenceTopology()

	topo.DeleteCollection("bucket")

	if !topo.lockForGrowth(option) {
		t.Fatal("growth refused after its collection was dropped without a delete")
	}
	defer option.collection.unlockForGrowth()
	if published, _ := topo.FindCollection("bucket"); published != option.collection {
		t.Error("growth did not follow the collection now published under its name")
	}
}
