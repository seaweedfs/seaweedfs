package topology

import (
	"sync"
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
	// Readers keep the pointers GetEcShards handed out after the lock is
	// released, so a refresh must publish a new record rather than write
	// into the one they may still be reading.
	if after == before {
		t.Errorf("count-only heartbeat updated the stored record in place instead of swapping in a copy")
	}
	if before.FileCount != 1000 || before.DeleteCount != 10 {
		t.Errorf("previously obtained record now reads %d/%d, want the 1000/10 snapshot it was taken with", before.FileCount, before.DeleteCount)
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

	// A message that does count refreshes the existing record, again by
	// swapping in a copy so an escaped pointer keeps its snapshot.
	topo.IncrementalSyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		ecMessageWithCounts(100, 0, []erasure_coding.ShardId{3}, 1000, 400),
	}, nil, dn)
	after, fileCount, deleteCount := storedEcCounts(t, dn, 100, 0)
	if fileCount != 1000 || deleteCount != 400 {
		t.Errorf("counts after counted mount = %d/%d, want 1000/400", fileCount, deleteCount)
	}
	if after == record {
		t.Errorf("counted mount updated the stored record in place instead of swapping in a copy")
	}
	if record.FileCount != 1000 || record.DeleteCount != 10 || record.ShardsInfo.Count() != 3 {
		t.Errorf("previously obtained record now reads %d/%d with %d shards, want the 1000/10, 3-shard snapshot it was taken with",
			record.FileCount, record.DeleteCount, record.ShardsInfo.Count())
	}
}

// TestEcCountRefreshDoesNotRaceReaders runs full and incremental heartbeats
// that only move the counts against readers that keep reading the records
// GetEcShards handed out, the way CollectionStatistics and ToDiskInfo do.
// Run with -race: an in-place refresh is a write to memory a reader holds
// without the lock. Without -race it still checks that every reader sees
// one heartbeat's pair, never a FileCount from one and a DeleteCount from
// another.
func TestEcCountRefreshDoesNotRaceReaders(t *testing.T) {
	topo, dn := newEcCountsTestNode(t)
	shards := []erasure_coding.ShardId{0, 1, 2}
	pairs := [][2]uint64{{1000, 10}, {2000, 20}}

	topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		ecMessageWithCounts(100, 0, shards, pairs[0][0], pairs[0][1]),
	}, dn)

	done := make(chan struct{})
	var readers sync.WaitGroup
	for r := 0; r < 4; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				for _, ev := range dn.GetEcShards() {
					fileCount, deleteCount := ev.FileCount, ev.DeleteCount
					if (fileCount != pairs[0][0] || deleteCount != pairs[0][1]) && (fileCount != pairs[1][0] || deleteCount != pairs[1][1]) {
						t.Errorf("reader saw counts %d/%d, which no heartbeat sent", fileCount, deleteCount)
						return
					}
				}
				topo.CollectionEcVolumeStats("col")
			}
		}()
	}

	for i := 1; i <= 300; i++ {
		pair := pairs[i%2]
		topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
			ecMessageWithCounts(100, 0, shards, pair[0], pair[1]),
		}, dn)
		pair = pairs[(i+1)%2]
		// A re-mount of a shard the node already holds takes the
		// existing-record branch of AddOrUpdateEcShard.
		topo.IncrementalSyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
			ecMessageWithCounts(100, 0, shards[:1], pair[0], pair[1]),
		}, nil, dn)
	}
	close(done)
	readers.Wait()

	if record, fileCount, deleteCount := storedEcCounts(t, dn, 100, 0); fileCount != pairs[1][0] || deleteCount != pairs[1][1] {
		t.Errorf("final counts = %d/%d, want %d/%d", fileCount, deleteCount, pairs[1][0], pairs[1][1])
	} else if record.ShardsInfo.Count() != len(shards) {
		t.Errorf("final shard count = %d, want %d", record.ShardsInfo.Count(), len(shards))
	}
}
