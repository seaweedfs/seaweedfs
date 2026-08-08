package topology

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func TestCollectionVolumeStats(t *testing.T) {
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)

	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")
	maxVolumeCounts := map[string]uint32{"": 25, "ssd": 12}
	dn := rack.GetOrCreateDataNode("127.0.0.1", 34534, 0, "127.0.0.1", "", maxVolumeCounts)

	volumeMessages := []*master_pb.VolumeInformationMessage{
		{Id: 1, Size: 1000, Collection: "", FileCount: 10, ReplicaPlacement: 0, Version: uint32(needle.GetCurrentVersion())},
		{Id: 2, Size: 2000, Collection: "c1", FileCount: 20, ReplicaPlacement: 0, Version: uint32(needle.GetCurrentVersion())},
		{Id: 3, Size: 3000, Collection: "c1", FileCount: 30, ReplicaPlacement: 0, Version: uint32(needle.GetCurrentVersion()), DiskType: "ssd"},
		{Id: 4, Size: 4000, Collection: "c2", FileCount: 40, ReplicaPlacement: 1, Version: uint32(needle.GetCurrentVersion())},
	}
	topo.SyncDataNodeRegistration(volumeMessages, dn)

	// volume 4 is replicated, so a second node holds a copy of it
	replicaDn := rack.GetOrCreateDataNode("127.0.0.2", 34534, 0, "127.0.0.2", "", maxVolumeCounts)
	topo.SyncDataNodeRegistration(volumeMessages[3:], replicaDn)

	// VolumeLocationList.Stats only counts nodes connected for over a minute
	dn.LastSeen = time.Now().Unix() - 61
	replicaDn.LastSeen = dn.LastSeen

	allStats := topo.CollectionVolumeStats("")
	assert(t, "all collections used size", int(allStats.UsedSize), 14000)
	assert(t, "all collections logical used size", int(allStats.LogicalUsedSize), 10000)
	assert(t, "all collections file count", int(allStats.FileCount), 100)

	c1Stats := topo.CollectionVolumeStats("c1")
	assert(t, "c1 used size across disk types", int(c1Stats.UsedSize), 5000)
	assert(t, "c1 file count", int(c1Stats.FileCount), 50)

	c2Stats := topo.CollectionVolumeStats("c2")
	// both copies of volume 4 count against the space, only one against the data
	assert(t, "c2 used size", int(c2Stats.UsedSize), 8000)
	assert(t, "c2 logical used size", int(c2Stats.LogicalUsedSize), 4000)

	missingStats := topo.CollectionVolumeStats("no-such-collection")
	assert(t, "missing collection used size", int(missingStats.UsedSize), 0)
	if _, found := topo.FindCollection("no-such-collection"); found {
		t.Errorf("stats query should not create a phantom collection")
	}
}

// ecShardMessage builds a heartbeat message for the given shard ids, sized
// (id+1)*sizeUnit bytes each.
func ecShardMessage(vid uint32, collection string, sizeUnit int, fileCount, deleteCount uint64, shardIds ...erasure_coding.ShardId) *master_pb.VolumeEcShardInformationMessage {
	shards := erasure_coding.NewShardsInfo()
	for _, id := range shardIds {
		shards.Set(erasure_coding.NewShardInfo(id, erasure_coding.ShardSize((int(id)+1)*sizeUnit)))
	}
	return &master_pb.VolumeEcShardInformationMessage{
		Id:          vid,
		Collection:  collection,
		EcIndexBits: shards.Bitmap(),
		ShardSizes:  shards.SizesInt64(),
		FileCount:   fileCount,
		DeleteCount: deleteCount,
	}
}

func TestCollectionVolumeStatsWithEcVolumes(t *testing.T) {
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)

	rack := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1")
	maxVolumeCounts := map[string]uint32{"": 25}
	dn1 := rack.GetOrCreateDataNode("127.0.0.1", 34534, 0, "127.0.0.1", "", maxVolumeCounts)
	dn2 := rack.GetOrCreateDataNode("127.0.0.2", 34534, 0, "127.0.0.2", "", maxVolumeCounts)

	// volume 10 spans both nodes, and shard 0 has a second copy on dn2. dn2 has
	// not finished loading its .ecx yet and reports a zero file count, and both
	// nodes report the 5 tombstones of the journal they share.
	topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		ecShardMessage(10, "c1", 100, 50, 5, 0, 1, 2, 3, 4, 5, 6),
		ecShardMessage(20, "", 10, 7, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13),
	}, dn1)
	topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		ecShardMessage(10, "c1", 100, 0, 5, 0, 7, 8, 9, 10, 11, 12, 13),
	}, dn2)

	// every shard copy of volume 10, parity included, each sized (id+1)*100:
	// 100..700 on dn1, plus a second copy of shard 0 and 800..1400 on dn2
	c1Stats := topo.CollectionVolumeStats("c1")
	assert(t, "c1 ec used size", int(c1Stats.UsedSize), 2800+7800)
	// data shards 0..9 once each, so the second copy of shard 0 and the
	// parity shards 10..13 are left out
	assert(t, "c1 ec logical used size", int(c1Stats.LogicalUsedSize), 5500)
	// the shared journal counts once, not once per holder: 50 - 5
	assert(t, "c1 ec file count", int(c1Stats.FileCount), 45)

	// volume 20 adds all 14 shards, each sized (id+1)*10
	allStats := topo.CollectionVolumeStats("")
	assert(t, "all collections ec used size", int(allStats.UsedSize), 10600+1050)
	assert(t, "all collections ec logical used size", int(allStats.LogicalUsedSize), 5500+550)
	assert(t, "all collections ec file count", int(allStats.FileCount), 45+7)

	if _, found := topo.FindCollection("c1"); found {
		t.Errorf("ec stats query should not create a phantom collection")
	}
}

func TestCollectionVolumeStatsClampsDeletedOverTotals(t *testing.T) {
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)

	rack := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1")
	dn := rack.GetOrCreateDataNode("127.0.0.1", 34534, 0, "127.0.0.1", "", map[string]uint32{"": 25})

	// Volume 1 reports more deleted than it holds, on both counters.
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		{Id: 1, Size: 1000, FileCount: 10, DeletedByteCount: 4000, DeleteCount: 40, Version: uint32(needle.GetCurrentVersion())},
		{Id: 2, Size: 3000, FileCount: 30, DeletedByteCount: 1000, DeleteCount: 10, Version: uint32(needle.GetCurrentVersion())},
	}, dn)

	// VolumeLocationList.Stats only counts nodes connected for over a minute
	dn.LastSeen = time.Now().Unix() - 61

	stats := topo.CollectionVolumeStats("")
	// Volume 1 contributes nothing rather than wrapping.
	assert(t, "used size", int(stats.UsedSize), 2000)
	assert(t, "logical used size", int(stats.LogicalUsedSize), 2000)
	assert(t, "file count", int(stats.FileCount), 20)
}
