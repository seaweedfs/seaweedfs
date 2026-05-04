package topology

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// TestEcShardsAcrossMultipleDisksOnSameNode reproduces issue #9212.
// When a volume server reports EC shards of the same volume spread across
// multiple physical disks on the same node, the master must register ALL
// shards, not only a subset.
func TestEcShardsAcrossMultipleDisksOnSameNode(t *testing.T) {
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")
	maxVolumeCounts := map[string]uint32{"": 100}
	dn := rack.GetOrCreateDataNode("127.0.0.1", 34534, 0, "127.0.0.1", "", maxVolumeCounts)

	const vid = uint32(15)
	const collection = "grafana-loki"
	const diskType = "" // HDD / default type — all 4 disks share this type

	// Volume 15 has its 14 EC shards spread across 3 physical disks on this node.
	// Mirrors the "volume-2" row from issue #9212:
	//   /data1 (diskId 0): ec02, ec06, ec10
	//   /data2 (diskId 1): ec01, ec04, ec09
	//   /data4 (diskId 3): ec08, ec12
	disk0 := buildEcShardMessage(vid, collection, diskType, 0, []erasure_coding.ShardId{2, 6, 10})
	disk1 := buildEcShardMessage(vid, collection, diskType, 1, []erasure_coding.ShardId{1, 4, 9})
	disk3 := buildEcShardMessage(vid, collection, diskType, 3, []erasure_coding.ShardId{8, 12})

	msgs := []*master_pb.VolumeEcShardInformationMessage{disk0, disk1, disk3}
	topo.SyncDataNodeEcShards(msgs, dn)

	locs, ok := topo.LookupEcShards(needle.VolumeId(vid))
	if !ok {
		t.Fatalf("volume %d: no ec shard locations registered at all", vid)
	}

	// All 8 shards should be visible to the master: 1,2,4,6,8,9,10,12.
	wantShards := []erasure_coding.ShardId{1, 2, 4, 6, 8, 9, 10, 12}
	var gotShards []erasure_coding.ShardId
	for shardId, dataNodes := range locs.Locations {
		if len(dataNodes) > 0 {
			gotShards = append(gotShards, erasure_coding.ShardId(shardId))
		}
	}

	if len(gotShards) != len(wantShards) {
		t.Errorf("volume %d: topology.LookupEcShards sees %d shards %v, want %d shards %v",
			vid, len(gotShards), gotShards, len(wantShards), wantShards)
	}

	for _, want := range wantShards {
		if len(locs.Locations[want]) == 0 {
			t.Errorf("volume %d: shard %d missing from topology (bug #9212)", vid, want)
		}
	}

	// The DataNode's own view (what drives volume.list output, admin UI, and
	// ec.rebuild dry-run diagnostics) must also see all shards across all disks.
	dnShards := dn.GetEcShards()
	dnShardCount := 0
	dnShardBitmap := erasure_coding.ShardBits(0)
	for _, ev := range dnShards {
		if ev.VolumeId == needle.VolumeId(vid) {
			dnShardCount += ev.ShardsInfo.Count()
			dnShardBitmap |= erasure_coding.ShardBits(ev.ShardsInfo.Bitmap())
		}
	}
	if dnShardCount != len(wantShards) {
		t.Errorf("volume %d: DataNode.GetEcShards reports %d shards (bitmap=0b%b), want %d shards %v (bug #9212)",
			vid, dnShardCount, dnShardBitmap, len(wantShards), wantShards)
	}

	// Per-physical-disk attribution must survive all the way to the protobuf
	// DiskInfo.EcShardInfos consumed by ec.balance / ec.rebuild planners. The
	// admin shell groups shards by eci.DiskId, so each physical disk needs a
	// separate message with its own shard subset.
	wantPerDisk := map[uint32][]erasure_coding.ShardId{
		0: {2, 6, 10},
		1: {1, 4, 9},
		3: {8, 12},
	}
	dnInfo := dn.ToDataNodeInfo()
	gotPerDisk := map[uint32][]erasure_coding.ShardId{}
	for _, diskInfo := range dnInfo.DiskInfos {
		for _, eci := range diskInfo.EcShardInfos {
			if eci.Id != vid {
				continue
			}
			si := erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(eci)
			gotPerDisk[eci.DiskId] = append(gotPerDisk[eci.DiskId], si.Ids()...)
		}
	}
	for diskId, want := range wantPerDisk {
		got := gotPerDisk[diskId]
		if !shardSetEqual(got, want) {
			t.Errorf("volume %d diskId %d: DiskInfo.EcShardInfos report shards %v, want %v (per-physical-disk attribution lost)",
				vid, diskId, got, want)
		}
	}
	for diskId := range gotPerDisk {
		if _, ok := wantPerDisk[diskId]; !ok {
			t.Errorf("volume %d: unexpected diskId %d in DiskInfo.EcShardInfos, shards=%v",
				vid, diskId, gotPerDisk[diskId])
		}
	}
}

func shardSetEqual(a, b []erasure_coding.ShardId) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[erasure_coding.ShardId]int, len(a))
	for _, id := range a {
		seen[id]++
	}
	for _, id := range b {
		seen[id]--
		if seen[id] < 0 {
			return false
		}
	}
	return true
}

// TestEcShardsAfterRestartHeartbeat simulates the exact issue #9212 flow:
// the volume server starts up, loads shards from each disk, and sends a
// single full-sync heartbeat containing one VolumeEcShardInformationMessage
// per (disk, volume). The master must end up with all shards visible per
// DataNode, not just the subset belonging to whichever disk happened to be
// iterated last.
func TestEcShardsAfterRestartHeartbeat(t *testing.T) {
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")
	dn := rack.GetOrCreateDataNode("127.0.0.1", 34534, 0, "127.0.0.1", "", map[string]uint32{"": 100})

	// Matches "volume-1" from the bug report: ec13 on /data1, ec03 on /data3.
	msgs := []*master_pb.VolumeEcShardInformationMessage{
		buildEcShardMessage(15, "grafana-loki", "", 0, []erasure_coding.ShardId{13}),
		buildEcShardMessage(15, "grafana-loki", "", 2, []erasure_coding.ShardId{3}),
	}
	topo.SyncDataNodeEcShards(msgs, dn)

	dnShards := dn.GetEcShards()
	var combined erasure_coding.ShardBits
	for _, ev := range dnShards {
		if ev.VolumeId == 15 {
			combined |= erasure_coding.ShardBits(ev.ShardsInfo.Bitmap())
		}
	}
	if combined.Count() != 2 {
		t.Errorf("volume 15: DataNode sees %d shards (bitmap=0b%b) after full sync of 2 disks; want both shards 3 and 13 visible (bug #9212)",
			combined.Count(), combined)
	}
}

func buildEcShardMessage(vid uint32, collection, diskType string, diskId uint32, shardIds []erasure_coding.ShardId) *master_pb.VolumeEcShardInformationMessage {
	si := erasure_coding.NewShardsInfo()
	for _, sid := range shardIds {
		si.Set(erasure_coding.NewShardInfo(sid, erasure_coding.ShardSize(1024)))
	}
	return &master_pb.VolumeEcShardInformationMessage{
		Id:          vid,
		Collection:  collection,
		DiskType:    diskType,
		DiskId:      diskId,
		EcIndexBits: si.Bitmap(),
		ShardSizes:  si.SizesInt64(),
	}
}
