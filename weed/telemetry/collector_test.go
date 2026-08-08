package telemetry

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

// ecShardMessage builds a heartbeat message for the given shard ids, each
// sized (id+1)*sizeUnit bytes.
func ecShardMessage(vid uint32, sizeUnit int, shardIds ...erasure_coding.ShardId) *master_pb.VolumeEcShardInformationMessage {
	shards := erasure_coding.NewShardsInfo()
	for _, id := range shardIds {
		shards.Set(erasure_coding.NewShardInfo(id, erasure_coding.ShardSize((int(id)+1)*sizeUnit)))
	}
	return &master_pb.VolumeEcShardInformationMessage{
		Id:          vid,
		EcIndexBits: shards.Bitmap(),
		ShardSizes:  shards.SizesInt64(),
	}
}

func TestCollectVolumeStatsCountsEcShards(t *testing.T) {
	topo := topology.NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)

	rack := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1")
	maxVolumeCounts := map[string]uint32{"": 25}
	dn1 := rack.GetOrCreateDataNode("127.0.0.1", 34534, 0, "127.0.0.1", "", maxVolumeCounts)
	dn2 := rack.GetOrCreateDataNode("127.0.0.2", 34534, 0, "127.0.0.2", "", maxVolumeCounts)

	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		{Id: 1, Size: 1000, FileCount: 10, Version: uint32(needle.GetCurrentVersion())},
		{Id: 2, Size: 2000, FileCount: 20, Version: uint32(needle.GetCurrentVersion())},
	}, dn1)

	// Volume 10's shards span both nodes, with a second copy of shard 0 on
	// dn2; volume 20 sits entirely on dn1.
	topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		ecShardMessage(10, 100, 0, 1, 2, 3, 4, 5, 6),
		ecShardMessage(20, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13),
	}, dn1)
	topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		ecShardMessage(10, 100, 0, 7, 8, 9, 10, 11, 12, 13),
	}, dn2)

	collector := NewCollector(nil, topo, nil)
	diskBytes, volumeCount := collector.collectVolumeStats()

	// 3000 from the two regular volumes, plus every shard copy of volume 10
	// sized (id+1)*100 — 100..700 on dn1, and 800..1400 plus the second copy
	// of shard 0 on dn2 — and volume 20's 14 shards at (id+1)*10.
	const expectedDiskBytes = 3000 + 2800 + (7700 + 100) + 1050
	if diskBytes != expectedDiskBytes {
		t.Errorf("Expected %d total disk bytes, got %d", expectedDiskBytes, diskBytes)
	}

	// 2 regular volumes plus EC volumes 10 and 20 — volume 10 counts once
	// even though two nodes hold its shards.
	if volumeCount != 4 {
		t.Errorf("Expected 4 volumes, got %d", volumeCount)
	}
}
