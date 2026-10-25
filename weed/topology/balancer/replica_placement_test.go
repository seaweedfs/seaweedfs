package balancer

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

func loc(dc, rack, node string) Location {
	return Location{DataCenter: dc, Rack: rack, NodeID: node}
}

// When a node abnormally holds more than one replica of a volume, moving one of
// them off must keep the other in the after-move set, so the placement check
// still sees the rack's true occupancy.
func TestIsGoodMove_OverReplicatedSourceKeepsOtherReplica(t *testing.T) {
	rp, err := super_block.NewReplicaPlacementFromString("001") // up to 2 copies in a rack
	if err != nil {
		t.Fatal(err)
	}
	// n1 abnormally holds two replicas; n2 holds a third — all in rack r1.
	existing := []Location{loc("dc1", "r1", "n1"), loc("dc1", "r1", "n1"), loc("dc1", "r1", "n2")}

	// Moving ONE replica off n1 onto a new node n3 in r1 must be rejected: r1 would
	// still hold two replicas (the retained n1 and n2), at the SameRackCount limit.
	if IsGoodMove(rp, existing, "n1", loc("dc1", "r1", "n3")) {
		t.Error("expected reject: moving one replica off an over-replicated node must keep its other replica in the rack count")
	}

	// The normal case is unaffected: two replicas correctly in r1, move one to a
	// third node in r1 — still within the SameRackCount=1 (two-per-rack) limit.
	normal := []Location{loc("dc1", "r1", "n1"), loc("dc1", "r1", "n2")}
	if !IsGoodMove(rp, normal, "n1", loc("dc1", "r1", "n3")) {
		t.Error("expected allow: a normal move within the rack limit")
	}
}
