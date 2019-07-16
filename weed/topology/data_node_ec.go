package topology

import (
	"github.com/chrislusf/seaweedfs/weed/storage/erasure_coding"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

func (dn *DataNode) GetEcShards() (ret []*erasure_coding.EcVolumeInfo) {
	dn.RLock()
	for _, ecVolumeInfo := range dn.ecShards {
		ret = append(ret, ecVolumeInfo)
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

	// found out the newShards and deletedShards
	var newShardCount, deletedShardCount int
	dn.ecShardsLock.RLock()
	for vid, ecShards := range dn.ecShards {
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
	}
	for _, ecShards := range actualShards {
		if _, found := dn.ecShards[ecShards.VolumeId]; !found {
			newShards = append(newShards, ecShards)
			newShardCount += ecShards.ShardIdCount()
		}
	}
	dn.ecShardsLock.RUnlock()

	if len(newShards) > 0 || len(deletedShards) > 0 {
		// if changed, set to the new ec shard map
		dn.ecShardsLock.Lock()
		dn.ecShards = actualEcShardMap
		dn.UpAdjustEcShardCountDelta(int64(newShardCount - deletedShardCount))
		dn.ecShardsLock.Unlock()
	}

	return
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
	dn.ecShardsLock.Lock()
	defer dn.ecShardsLock.Unlock()

	delta := 0
	if existing, ok := dn.ecShards[s.VolumeId]; !ok {
		dn.ecShards[s.VolumeId] = s
		delta = s.ShardBits.ShardIdCount()
	} else {
		oldCount := existing.ShardBits.ShardIdCount()
		existing.ShardBits = existing.ShardBits.Plus(s.ShardBits)
		delta = existing.ShardBits.ShardIdCount() - oldCount
	}

	dn.UpAdjustEcShardCountDelta(int64(delta))

}

func (dn *DataNode) DeleteEcShard(s *erasure_coding.EcVolumeInfo) {
	dn.ecShardsLock.Lock()
	defer dn.ecShardsLock.Unlock()

	if existing, ok := dn.ecShards[s.VolumeId]; ok {
		oldCount := existing.ShardBits.ShardIdCount()
		existing.ShardBits = existing.ShardBits.Minus(s.ShardBits)
		delta := existing.ShardBits.ShardIdCount() - oldCount
		dn.UpAdjustEcShardCountDelta(int64(delta))
		if existing.ShardBits.ShardIdCount() == 0 {
			delete(dn.ecShards, s.VolumeId)
		}
	}

}

func (dn *DataNode) HasVolumesById(id needle.VolumeId) (hasVolumeId bool) {

	// check whether normal volumes has this volume id
	dn.RLock()
	_, ok := dn.volumes[id]
	if ok {
		hasVolumeId = true
	}
	dn.RUnlock()

	if hasVolumeId {
		return
	}

	// check whether ec shards has this volume id
	dn.ecShardsLock.RLock()
	_, ok = dn.ecShards[id]
	if ok {
		hasVolumeId = true
	}
	dn.ecShardsLock.RUnlock()

	return

}
