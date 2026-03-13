package balance

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

func rp(t *testing.T, code string) *super_block.ReplicaPlacement {
	t.Helper()
	r, err := super_block.NewReplicaPlacementFromString(code)
	if err != nil {
		t.Fatalf("invalid replica placement code %q: %v", code, err)
	}
	return r
}

func loc(dc, rack, node string) types.ReplicaLocation {
	return types.ReplicaLocation{DataCenter: dc, Rack: rack, NodeID: node}
}

func TestIsGoodMove_NoReplication(t *testing.T) {
	// 000 = no replication. Any move is fine.
	if !IsGoodMove(rp(t, "000"), []types.ReplicaLocation{loc("dc1", "r1", "n1")}, "n1", loc("dc1", "r1", "n2")) {
		t.Error("000: any move should be allowed")
	}
}

func TestIsGoodMove_001_SameRack(t *testing.T) {
	// 001 = 1 replica on same rack (2 total on same rack)
	existing := []types.ReplicaLocation{
		loc("dc1", "r1", "n1"),
		loc("dc1", "r1", "n2"),
	}
	// Move n1 -> n3 on same rack: good
	if !IsGoodMove(rp(t, "001"), existing, "n1", loc("dc1", "r1", "n3")) {
		t.Error("001: move to same rack should be allowed")
	}
	// Move n1 -> n3 on different rack: bad (would leave only 1 on r1, need 2)
	if IsGoodMove(rp(t, "001"), existing, "n1", loc("dc1", "r2", "n3")) {
		t.Error("001: move to different rack should not be allowed when it breaks same-rack count")
	}
}

func TestIsGoodMove_010_DiffRack(t *testing.T) {
	// 010 = 1 replica on different rack (2 racks total)
	existing := []types.ReplicaLocation{
		loc("dc1", "r1", "n1"),
		loc("dc1", "r2", "n2"),
	}
	// Move n1 -> n3 on r2: bad (both replicas on same rack)
	if IsGoodMove(rp(t, "010"), existing, "n1", loc("dc1", "r2", "n3")) {
		t.Error("010: move to same rack as other replica should not be allowed")
	}
	// Move n1 -> n3 on r3: good (still 2 different racks)
	if !IsGoodMove(rp(t, "010"), existing, "n1", loc("dc1", "r3", "n3")) {
		t.Error("010: move to different rack should be allowed")
	}
}

func TestIsGoodMove_100_DiffDC(t *testing.T) {
	// 100 = 1 replica in different DC
	existing := []types.ReplicaLocation{
		loc("dc1", "r1", "n1"),
		loc("dc2", "r1", "n2"),
	}
	// Move n1 -> n3 in dc2: bad (both in same DC)
	if IsGoodMove(rp(t, "100"), existing, "n1", loc("dc2", "r1", "n3")) {
		t.Error("100: move to same DC as other replica should not be allowed")
	}
	// Move n1 -> n3 in dc3: good (different DCs)
	if !IsGoodMove(rp(t, "100"), existing, "n1", loc("dc3", "r1", "n3")) {
		t.Error("100: move to different DC should be allowed")
	}
}

func TestIsGoodMove_SameNode(t *testing.T) {
	// Moving to the same node as an existing replica should always be rejected
	existing := []types.ReplicaLocation{
		loc("dc1", "r1", "n1"),
		loc("dc1", "r2", "n2"),
	}
	if IsGoodMove(rp(t, "010"), existing, "n1", loc("dc1", "r2", "n2")) {
		t.Error("should reject move to same node as existing replica")
	}
}

func TestIsGoodMove_011_Composite(t *testing.T) {
	// 011 = 1 same-rack + 1 different-rack (3 replicas: 2 on same rack, 1 on different)
	existing := []types.ReplicaLocation{
		loc("dc1", "r1", "n1"),
		loc("dc1", "r1", "n2"),
		loc("dc1", "r2", "n3"),
	}
	// Move n1 -> n4 on r1: good (maintains 2 on r1, 1 on r2)
	if !IsGoodMove(rp(t, "011"), existing, "n1", loc("dc1", "r1", "n4")) {
		t.Error("011: move within same rack should be allowed")
	}
	// Move n3 -> n4 on r1: bad (would have 3 on r1, 0 on different rack)
	if IsGoodMove(rp(t, "011"), existing, "n3", loc("dc1", "r1", "n4")) {
		t.Error("011: move that eliminates different-rack replica should not be allowed")
	}
}

func TestIsGoodMove_110_Composite(t *testing.T) {
	// 110 = 1 different-rack + 1 different-DC (3 replicas across 2 DCs and 2 racks)
	existing := []types.ReplicaLocation{
		loc("dc1", "r1", "n1"),
		loc("dc1", "r2", "n2"),
		loc("dc2", "r1", "n3"),
	}
	// Move n1 -> n4 in dc1/r3: good (dc1 still has r2+r3, dc2 has r1)
	if !IsGoodMove(rp(t, "110"), existing, "n1", loc("dc1", "r3", "n4")) {
		t.Error("110: move to new rack in same DC should be allowed")
	}
	// Move n3 -> n4 in dc1/r1: bad (would lose the different-DC replica)
	if IsGoodMove(rp(t, "110"), existing, "n3", loc("dc1", "r1", "n4")) {
		t.Error("110: move that eliminates different-DC replica should not be allowed")
	}
}

func TestIsGoodMove_NilReplicaPlacement(t *testing.T) {
	if !IsGoodMove(nil, []types.ReplicaLocation{loc("dc1", "r1", "n1")}, "n1", loc("dc1", "r1", "n2")) {
		t.Error("nil replica placement should allow any move")
	}
}
