package ec

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// TestEcBalanceMigratesCrossDiskTypeShards: a cross-tier encode generates its
// shards beside the source .dat — in the SOURCE disk-type bucket — while the
// encode's balance targets -diskType. The balance must still see and spread
// those shards when the volume is named as migrating; without that, the
// planner finds nothing in the target bucket, plans no moves, and the encode's
// spread guard aborts a perfectly good encode.
func TestEcBalanceMigratesCrossDiskTypeShards(t *testing.T) {
	allShards := []erasure_coding.ShardId{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}

	build := func() []*EcNode {
		// Volume 1's fresh shards sit in the default (hdd, "") bucket of dn1;
		// the balance runs with -diskType=ssd.
		return []*EcNode{
			newEcNode("dc1", "rack1", "dn1", 100).addEcVolumeAndShardsForTest(1, "c1", allShards),
			newEcNode("dc1", "rack2", "dn2", 100),
			newEcNode("dc1", "rack3", "dn3", 100),
		}
	}

	// Without the migrating hint the target-bucket filter hides the shards:
	// nothing moves. This is the standalone ec.balance semantic — shards of
	// other tiers stay where they are.
	ecb := &ecBalancer{
		ecNodes:        build(),
		applyBalancing: false,
		diskType:       types.SsdType,
	}
	if err := ecb.balance([]string{"c1"}); err != nil {
		t.Fatalf("balance without migrating hint: %v", err)
	}
	if got := ecb.ecNodes[0].LocalShardIdCount(1); got != len(allShards) {
		t.Fatalf("without migrating hint, cross-type shards must stay put; dn1 has %d", got)
	}

	// With the volume named as migrating, the balance ingests the shards from
	// the source bucket and spreads them.
	ecb = &ecBalancer{
		ecNodes:            build(),
		applyBalancing:     false,
		diskType:           types.SsdType,
		migratingVolumeIds: map[uint32]bool{1: true},
	}
	if err := ecb.balance([]string{"c1"}); err != nil {
		t.Fatalf("balance with migrating hint: %v", err)
	}
	onDn1 := ecb.ecNodes[0].LocalShardIdCount(1)
	spread := ecb.ecNodes[1].LocalShardIdCount(1) + ecb.ecNodes[2].LocalShardIdCount(1)
	if onDn1 == len(allShards) || spread == 0 {
		t.Fatalf("migrating volume's shards did not spread: dn1=%d others=%d", onDn1, spread)
	}
	if onDn1+spread != len(allShards) {
		t.Fatalf("shards lost or duplicated during dry-run spread: dn1=%d others=%d", onDn1, spread)
	}
}
