package erasure_coding

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// data structure used in master
type EcVolumeInfo struct {
	VolumeId    needle.VolumeId
	Collection  string
	ShardBits   ShardBits
	DiskType    string
	ExpireAtSec uint64 //ec volume destroy time, calculated from the ec volume was created
}

func NewEcVolumeInfo(diskType string, collection string, vid needle.VolumeId, shardBits ShardBits, expireAtSec uint64) *EcVolumeInfo {
	return &EcVolumeInfo{
		Collection:  collection,
		VolumeId:    vid,
		ShardBits:   shardBits,
		DiskType:    diskType,
		ExpireAtSec: expireAtSec,
	}
}

func (ecInfo *EcVolumeInfo) AddShardId(id ShardId) {
	ecInfo.ShardBits = ecInfo.ShardBits.AddShardId(id)
}

func (ecInfo *EcVolumeInfo) RemoveShardId(id ShardId) {
	ecInfo.ShardBits = ecInfo.ShardBits.RemoveShardId(id)
}

func (ecInfo *EcVolumeInfo) HasShardId(id ShardId) bool {
	return ecInfo.ShardBits.HasShardId(id)
}

func (ecInfo *EcVolumeInfo) ShardIds() (ret []ShardId) {
	return ecInfo.ShardBits.ShardIds()
}

func (ecInfo *EcVolumeInfo) ShardIdCount() (count int) {
	return ecInfo.ShardBits.ShardIdCount()
}

func (ecInfo *EcVolumeInfo) Minus(other *EcVolumeInfo) *EcVolumeInfo {
	ret := &EcVolumeInfo{
		VolumeId:   ecInfo.VolumeId,
		Collection: ecInfo.Collection,
		ShardBits:  ecInfo.ShardBits.Minus(other.ShardBits),
		DiskType:   ecInfo.DiskType,
	}

	return ret
}

func (ecInfo *EcVolumeInfo) ToVolumeEcShardInformationMessage() (ret *master_pb.VolumeEcShardInformationMessage) {
	return &master_pb.VolumeEcShardInformationMessage{
		Id:          uint32(ecInfo.VolumeId),
		EcIndexBits: uint32(ecInfo.ShardBits),
		Collection:  ecInfo.Collection,
		DiskType:    ecInfo.DiskType,
		ExpireAtSec: ecInfo.ExpireAtSec,
	}
}

type ShardBits uint32 // use bits to indicate the shard id, use 32 bits just for possible future extension

func (b ShardBits) AddShardId(id ShardId) ShardBits {
	return b | (1 << id)
}

func (b ShardBits) RemoveShardId(id ShardId) ShardBits {
	return b &^ (1 << id)
}

func (b ShardBits) HasShardId(id ShardId) bool {
	return b&(1<<id) > 0
}

func (b ShardBits) ShardIds() (ret []ShardId) {
	for i := ShardId(0); i < TotalShardsCount; i++ {
		if b.HasShardId(i) {
			ret = append(ret, i)
		}
	}
	return
}

func (b ShardBits) ToUint32Slice() (ret []uint32) {
	for i := uint32(0); i < TotalShardsCount; i++ {
		if b.HasShardId(ShardId(i)) {
			ret = append(ret, i)
		}
	}
	return
}

func (b ShardBits) ShardIdCount() (count int) {
	for count = 0; b > 0; count++ {
		b &= b - 1
	}
	return
}

func (b ShardBits) Minus(other ShardBits) ShardBits {
	return b &^ other
}

func (b ShardBits) Plus(other ShardBits) ShardBits {
	return b | other
}

func (b ShardBits) MinusParityShards() ShardBits {
	for i := DataShardsCount; i < TotalShardsCount; i++ {
		b = b.RemoveShardId(ShardId(i))
	}
	return b
}
