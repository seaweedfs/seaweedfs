package erasure_coding

import (
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

// data structure used in master
type EcVolumeInfo struct {
	VolumeId   needle.VolumeId
	Collection string
	shardIds   uint16 // use bits to indicate the shard id
}

func (ecInfo *EcVolumeInfo) AddShardId(id ShardId) {
	ecInfo.shardIds |= (1 << id)
}

func (ecInfo *EcVolumeInfo) RemoveShardId(id ShardId) {
	ecInfo.shardIds &^= (1 << id)
}

func (ecInfo *EcVolumeInfo) HasShardId(id ShardId) bool {
	return ecInfo.shardIds&(1<<id) > 0
}

func (ecInfo *EcVolumeInfo) ShardIds() (ret []ShardId) {
	for i := ShardId(0); i < DataShardsCount+ParityShardsCount; i++ {
		if ecInfo.HasShardId(i) {
			ret = append(ret, i)
		}
	}
	return
}

func (ecInfo *EcVolumeInfo) ToVolumeEcShardInformationMessage() (ret []*master_pb.VolumeEcShardInformationMessage) {
	for _, shard := range ecInfo.ShardIds() {
		ret = append(ret, &master_pb.VolumeEcShardInformationMessage{
			Id:         uint32(ecInfo.VolumeId),
			EcIndex:    uint32(shard),
			Collection: ecInfo.Collection,
		})

	}
	return
}
