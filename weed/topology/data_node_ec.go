package topology

import (
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func (dn *DataNode) GetEcShards() (ret []*erasure_coding.EcVolumeInfo) {
	dn.RLock()
	for _, c := range dn.children {
		disk := c.(*Disk)
		ret = append(ret, disk.GetEcShards()...)
	}
	dn.RUnlock()
	return ret
}

// ecShardKey identifies a per-physical-disk EC shard entry on a DataNode.
// A single volume's EC shards can live on multiple physical disks of one
// DataNode (e.g. a 10+4 volume spread across 4 mount points), so entries
// must be tracked per (volume, physical disk) rather than per volume alone.
// See issue #9212.
type ecShardKey struct {
	vid    needle.VolumeId
	diskId types.DiskId
}

func (dn *DataNode) UpdateEcShards(actualShards []*erasure_coding.EcVolumeInfo) (newShards, deletedShards []*erasure_coding.EcVolumeInfo) {
	// Aggregate incoming messages by (volume, physical disk). Duplicates for
	// the same (vid, diskId) are merged; entries for the same vid on
	// different disks stay separate so per-physical-disk attribution is
	// preserved all the way to DiskInfo.EcShardInfos — which the admin
	// shell's ec.balance / ec.rebuild planners read via eci.DiskId.
	actualByKey := make(map[ecShardKey]*erasure_coding.EcVolumeInfo, len(actualShards))
	for _, ecShards := range actualShards {
		key := ecShardKey{vid: ecShards.VolumeId, diskId: types.DiskId(ecShards.DiskId)}
		if existing, ok := actualByKey[key]; ok {
			existing.ShardsInfo.Add(ecShards.ShardsInfo)
			continue
		}
		// Clone so subsequent merges for the same key don't mutate the
		// caller's EcVolumeInfo, and so the diff below is stable.
		clone := *ecShards
		clone.ShardsInfo = ecShards.ShardsInfo.Copy()
		actualByKey[key] = &clone
	}

	existingEcShards := dn.GetEcShards()

	// find out the newShards and deletedShards
	for _, ecShards := range existingEcShards {

		var newShardCount, deletedShardCount int
		disk := dn.getOrCreateDisk(ecShards.DiskType)

		key := ecShardKey{vid: ecShards.VolumeId, diskId: types.DiskId(ecShards.DiskId)}
		if actualEcShards, ok := actualByKey[key]; !ok {
			// dn registered ec shards not found in the new set of ec shards
			deletedShards = append(deletedShards, ecShards)
			deletedShardCount += ecShards.ShardsInfo.Count()
		} else {
			// found, but maybe the actual shard could be missing
			a := actualEcShards.Minus(ecShards)
			if a.ShardsInfo.Count() > 0 {
				newShards = append(newShards, a)
				newShardCount += a.ShardsInfo.Count()
			}
			d := ecShards.Minus(actualEcShards)
			if d.ShardsInfo.Count() > 0 {
				deletedShards = append(deletedShards, d)
				deletedShardCount += d.ShardsInfo.Count()
			}
		}

		if (newShardCount - deletedShardCount) != 0 {
			disk.UpAdjustDiskUsageDelta(types.ToDiskType(ecShards.DiskType), &DiskUsageCounts{
				ecShardCount: int64(newShardCount - deletedShardCount),
			})
		}

	}

	existingKeys := make(map[ecShardKey]struct{}, len(existingEcShards))
	for _, ev := range existingEcShards {
		existingKeys[ecShardKey{vid: ev.VolumeId, diskId: types.DiskId(ev.DiskId)}] = struct{}{}
	}
	for key, ecShards := range actualByKey {
		if _, found := existingKeys[key]; found {
			continue
		}

		newShards = append(newShards, ecShards)

		disk := dn.getOrCreateDisk(ecShards.DiskType)
		disk.UpAdjustDiskUsageDelta(types.ToDiskType(ecShards.DiskType), &DiskUsageCounts{
			ecShardCount: int64(ecShards.ShardsInfo.Count()),
		})
	}

	if len(newShards) > 0 || len(deletedShards) > 0 {
		// if changed, set to the new ec shard map
		dn.doUpdateEcShards(actualByKey)
	}

	return
}

func (dn *DataNode) HasEcShards(volumeId needle.VolumeId) (found bool) {
	dn.RLock()
	defer dn.RUnlock()
	for _, c := range dn.children {
		disk := c.(*Disk)
		if byDisk, ok := disk.ecShards[volumeId]; ok && len(byDisk) > 0 {
			return true
		}
	}
	return
}

func (dn *DataNode) doUpdateEcShards(actualByKey map[ecShardKey]*erasure_coding.EcVolumeInfo) {
	dn.Lock()
	for _, c := range dn.children {
		disk := c.(*Disk)
		disk.ecShards = make(map[needle.VolumeId]map[types.DiskId]*erasure_coding.EcVolumeInfo)
	}
	for _, shard := range actualByKey {
		disk := dn.getOrCreateDisk(shard.DiskType)
		byDisk, ok := disk.ecShards[shard.VolumeId]
		if !ok {
			byDisk = make(map[types.DiskId]*erasure_coding.EcVolumeInfo, 1)
			disk.ecShards[shard.VolumeId] = byDisk
		}
		byDisk[types.DiskId(shard.DiskId)] = shard
	}
	dn.Unlock()
}

func (dn *DataNode) DeltaUpdateEcShards(newShards, deletedShards []*erasure_coding.EcVolumeInfo) {
	for _, newShard := range newShards {
		dn.AddOrUpdateEcShard(newShard)
	}

	for _, deletedShard := range deletedShards {
		dn.DeleteEcShard(deletedShard)
	}

}

func (dn *DataNode) AddOrUpdateEcShard(s *erasure_coding.EcVolumeInfo) {
	disk := dn.getOrCreateDisk(s.DiskType)
	disk.AddOrUpdateEcShard(s)
}

func (dn *DataNode) DeleteEcShard(s *erasure_coding.EcVolumeInfo) {
	disk := dn.getOrCreateDisk(s.DiskType)
	disk.DeleteEcShard(s)
}

func (dn *DataNode) HasVolumesById(volumeId needle.VolumeId) (hasVolumeId bool) {

	dn.RLock()
	defer dn.RUnlock()
	for _, c := range dn.children {
		disk := c.(*Disk)
		if disk.HasVolumesById(volumeId) {
			return true
		}
	}
	return false

}
