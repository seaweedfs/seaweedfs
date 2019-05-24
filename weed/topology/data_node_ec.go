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
		actualEcShardMap[ecShards.VolumeId]= ecShards
	}

	// found out the newShards and deletedShards
	dn.ecShardsLock.RLock()
	for vid, ecShards := range dn.ecShards{
		if actualEcShards, ok := actualEcShardMap[vid]; !ok {
			// dn registered ec shards not found in the new set of ec shards
			deletedShards = append(deletedShards, ecShards)
		} else {
			// found, but maybe the actual shard could be missing
			a := actualEcShards.Minus(ecShards)
			if len(a.ShardIds())>0 {
				newShards = append(newShards, a)
			}
			d := ecShards.Minus(actualEcShards)
			if len(d.ShardIds())>0 {
				deletedShards = append(deletedShards, d)
			}
		}
	}
	for _, ecShards := range actualShards {
		if _, found := dn.ecShards[ecShards.VolumeId]; !found {
			newShards = append(newShards, ecShards)
		}
	}
	dn.ecShardsLock.RUnlock()

	if len(newShards)>0 || len(deletedShards)>0{
		// if changed, set to the new ec shard map
		dn.ecShardsLock.Lock()
		dn.ecShards = actualEcShardMap
		dn.ecShardsLock.Unlock()
	}

	return
}
