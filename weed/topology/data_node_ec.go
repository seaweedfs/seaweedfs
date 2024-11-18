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

func (dn *DataNode) UpdateEcShards(actualShards []*erasure_coding.EcVolumeInfo) (newShards, deletedShards []*erasure_coding.EcVolumeInfo) {
	// prepare the new ec shard map
	actualEcShardMap := make(map[needle.VolumeId]*erasure_coding.EcVolumeInfo)
	for _, ecShards := range actualShards {
		actualEcShardMap[ecShards.VolumeId] = ecShards
	}

	existingEcShards := dn.GetEcShards()

	// find out the newShards and deletedShards
	for _, ecShards := range existingEcShards {

		var newShardCount, deletedShardCount int
		disk := dn.getOrCreateDisk(ecShards.DiskType)

		vid := ecShards.VolumeId
		if actualEcShards, ok := actualEcShardMap[vid]; !ok {
			// dn registered ec shards not found in the new set of ec shards
			deletedShards = append(deletedShards, ecShards)
			deletedShardCount += ecShards.ShardIdCount()
		} else {
			// found, but maybe the actual shard could be missing
			a := actualEcShards.Minus(ecShards)
			if a.ShardIdCount() > 0 {
				newShards = append(newShards, a)
				newShardCount += a.ShardIdCount()
			}
			d := ecShards.Minus(actualEcShards)
			if d.ShardIdCount() > 0 {
				deletedShards = append(deletedShards, d)
				deletedShardCount += d.ShardIdCount()
			}
		}

		if (newShardCount - deletedShardCount) != 0 {
			disk.UpAdjustDiskUsageDelta(types.ToDiskType(ecShards.DiskType), &DiskUsageCounts{
				ecShardCount: int64(newShardCount - deletedShardCount),
			})
		}

	}

	for _, ecShards := range actualShards {
		if dn.HasEcShards(ecShards.VolumeId) {
			continue
		}

		newShards = append(newShards, ecShards)

		disk := dn.getOrCreateDisk(ecShards.DiskType)
		disk.UpAdjustDiskUsageDelta(types.ToDiskType(ecShards.DiskType), &DiskUsageCounts{
			ecShardCount: int64(ecShards.ShardIdCount()),
		})
	}

	if len(newShards) > 0 || len(deletedShards) > 0 {
		// if changed, set to the new ec shard map
		dn.doUpdateEcShards(actualShards)
	}

	return
}

func (dn *DataNode) HasEcShards(volumeId needle.VolumeId) (found bool) {
	dn.RLock()
	defer dn.RUnlock()
	for _, c := range dn.children {
		disk := c.(*Disk)
		_, found = disk.ecShards[volumeId]
		if found {
			return
		}
	}
	return
}

func (dn *DataNode) doUpdateEcShards(actualShards []*erasure_coding.EcVolumeInfo) {
	dn.Lock()
	for _, c := range dn.children {
		disk := c.(*Disk)
		disk.ecShards = make(map[needle.VolumeId]*erasure_coding.EcVolumeInfo)
	}
	for _, shard := range actualShards {
		disk := dn.getOrCreateDisk(shard.DiskType)
		disk.ecShards[shard.VolumeId] = shard
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
