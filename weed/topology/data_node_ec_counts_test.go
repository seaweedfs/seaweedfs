package topology

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func newEcCountsTestNode(t *testing.T) (*Topology, *DataNode) {
	t.Helper()
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")
	dn := rack.GetOrCreateDataNode("127.0.0.1", 34534, 0, "127.0.0.1", "", map[string]uint32{"": 100})
	return topo, dn
}

func ecMessageWithCounts(vid uint32, diskId uint32, shardIds []erasure_coding.ShardId, fileCount, deleteCount uint64) *master_pb.VolumeEcShardInformationMessage {
	m := buildEcShardMessage(vid, "col", "", diskId, shardIds)
	m.FileCount = fileCount
	m.DeleteCount = deleteCount
	return m
}

func storedEcCounts(t *testing.T, dn *DataNode, vid uint32, diskId uint32) (*erasure_coding.EcVolumeInfo, uint64, uint64) {
	t.Helper()
	for _, ev := range dn.GetEcShards() {
		if ev.VolumeId == needle.VolumeId(vid) && ev.DiskId == diskId {
			return ev, ev.FileCount, ev.DeleteCount
		}
	}
	t.Fatalf("volume %d disk %d: no stored ec shard record", vid, diskId)
	return nil, 0, 0
}

func TestSyncRefreshesEcCountsWithoutShardChange(t *testing.T) {
	topo, dn := newEcCountsTestNode(t)
	shards := []erasure_coding.ShardId{0, 1, 2}

	topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		ecMessageWithCounts(100, 0, shards, 1000, 10),
	}, dn)
	before, fileCount, deleteCount := storedEcCounts(t, dn, 100, 0)
	if fileCount != 1000 || deleteCount != 10 {
		t.Fatalf("initial counts = %d/%d, want 1000/10", fileCount, deleteCount)
	}

	// Same shards, more tombstones: the next full heartbeat must land.
	newShards, deletedShards := topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		ecMessageWithCounts(100, 0, shards, 1000, 250),
	}, dn)
	if len(newShards) != 0 || len(deletedShards) != 0 {
		t.Errorf("count-only heartbeat reported %d new / %d deleted shards, want none", len(newShards), len(deletedShards))
	}
	after, fileCount, deleteCount := storedEcCounts(t, dn, 100, 0)
	if fileCount != 1000 || deleteCount != 250 {
		t.Errorf("counts after heartbeat = %d/%d, want 1000/250", fileCount, deleteCount)
	}
	if after != before {
		t.Errorf("count-only heartbeat replaced the stored record instead of updating it in place")
	}
	if after.ShardsInfo.Count() != len(shards) {
		t.Errorf("stored shard count = %d, want %d", after.ShardsInfo.Count(), len(shards))
	}
	locs, ok := topo.LookupEcShards(100)
	if !ok {
		t.Fatal("volume 100 lost its shard locations")
	}
	for _, sid := range shards {
		if len(locs.Locations[sid]) != 1 {
			t.Errorf("shard %d has %d locations, want 1", sid, len(locs.Locations[sid]))
		}
	}
}

func TestSyncKeepsEcCountsWhenNotReported(t *testing.T) {
	topo, dn := newEcCountsTestNode(t)
	shards := []erasure_coding.ShardId{0, 1, 2}

	topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		ecMessageWithCounts(100, 0, shards, 1000, 10),
	}, dn)

	// An older volume server reports no counts: keep what we have.
	newShards, deletedShards := topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		ecMessageWithCounts(100, 0, shards, 0, 0),
	}, dn)
	if len(newShards) != 0 || len(deletedShards) != 0 {
		t.Errorf("unchanged heartbeat reported %d new / %d deleted shards, want none", len(newShards), len(deletedShards))
	}
	if _, fileCount, deleteCount := storedEcCounts(t, dn, 100, 0); fileCount != 1000 || deleteCount != 10 {
		t.Errorf("counts after 0/0 heartbeat = %d/%d, want 1000/10 kept", fileCount, deleteCount)
	}

	// A membership change rewrites the record; an unreported count must not zero it.
	newShards, deletedShards = topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		ecMessageWithCounts(100, 0, append(shards, 3), 0, 0),
	}, dn)
	if len(newShards) != 1 || len(deletedShards) != 0 {
		t.Fatalf("adding a shard reported %d new / %d deleted shards, want 1 / 0", len(newShards), len(deletedShards))
	}
	if newShards[0].ShardsInfo.Count() != 1 || !newShards[0].ShardsInfo.Has(3) {
		t.Errorf("new shard delta = %v, want shard 3 only", newShards[0].ShardsInfo.Ids())
	}
	if _, fileCount, deleteCount := storedEcCounts(t, dn, 100, 0); fileCount != 1000 || deleteCount != 10 {
		t.Errorf("counts after rewrite with 0/0 = %d/%d, want 1000/10 kept", fileCount, deleteCount)
	}
}

func TestSyncMembershipChangeCarriesNewEcCounts(t *testing.T) {
	topo, dn := newEcCountsTestNode(t)

	topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		ecMessageWithCounts(100, 0, []erasure_coding.ShardId{0, 1, 2}, 1000, 10),
	}, dn)

	newShards, deletedShards := topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		ecMessageWithCounts(100, 0, []erasure_coding.ShardId{0, 1, 3}, 1000, 300),
	}, dn)
	if len(newShards) != 1 || len(deletedShards) != 1 {
		t.Fatalf("shard swap reported %d new / %d deleted shards, want 1 / 1", len(newShards), len(deletedShards))
	}
	if !newShards[0].ShardsInfo.Has(3) || newShards[0].ShardsInfo.Count() != 1 {
		t.Errorf("new shard delta = %v, want shard 3 only", newShards[0].ShardsInfo.Ids())
	}
	if !deletedShards[0].ShardsInfo.Has(2) || deletedShards[0].ShardsInfo.Count() != 1 {
		t.Errorf("deleted shard delta = %v, want shard 2 only", deletedShards[0].ShardsInfo.Ids())
	}
	if _, fileCount, deleteCount := storedEcCounts(t, dn, 100, 0); fileCount != 1000 || deleteCount != 300 {
		t.Errorf("counts after shard swap = %d/%d, want 1000/300", fileCount, deleteCount)
	}
}

func TestIncrementalSyncRefreshesEcCounts(t *testing.T) {
	topo, dn := newEcCountsTestNode(t)

	topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		ecMessageWithCounts(100, 0, []erasure_coding.ShardId{0, 1}, 1000, 10),
	}, dn)

	// The mount message carries no counts: keep the stored ones.
	topo.IncrementalSyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		ecMessageWithCounts(100, 0, []erasure_coding.ShardId{2}, 0, 0),
	}, nil, dn)
	record, fileCount, deleteCount := storedEcCounts(t, dn, 100, 0)
	if fileCount != 1000 || deleteCount != 10 {
		t.Errorf("counts after 0/0 mount = %d/%d, want 1000/10 kept", fileCount, deleteCount)
	}
	if record.ShardsInfo.Count() != 3 {
		t.Errorf("stored shard count after mount = %d, want 3", record.ShardsInfo.Count())
	}

	// A message that does count refreshes the existing record.
	topo.IncrementalSyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		ecMessageWithCounts(100, 0, []erasure_coding.ShardId{3}, 1000, 400),
	}, nil, dn)
	if _, fileCount, deleteCount := storedEcCounts(t, dn, 100, 0); fileCount != 1000 || deleteCount != 400 {
		t.Errorf("counts after counted mount = %d/%d, want 1000/400", fileCount, deleteCount)
	}
}
