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
	DiskId      uint32            // ID of the disk this EC volume is on
	ExpireAtSec uint64            // ec volume destroy time, calculated from the ec volume was created
	ShardSizes  map[ShardId]int64 // map from shard ID to shard size in bytes
}

func NewEcVolumeInfo(diskType string, collection string, vid needle.VolumeId, shardBits ShardBits, expireAtSec uint64, diskId uint32, shardSizes map[uint32]int64) *EcVolumeInfo {
	ecInfo := &EcVolumeInfo{
		Collection:  collection,
		VolumeId:    vid,
		ShardBits:   shardBits,
		DiskType:    diskType,
		DiskId:      diskId,
		ExpireAtSec: expireAtSec,
		ShardSizes:  make(map[ShardId]int64),
	}

	// Convert uint32 shard IDs to ShardId type and populate sizes
	for shardId, size := range shardSizes {
		if shardId < TotalShardsCount {
			ecInfo.ShardSizes[ShardId(shardId)] = size
		}
	}

	return ecInfo
}

func (ecInfo *EcVolumeInfo) AddShardId(id ShardId) {
	ecInfo.ShardBits = ecInfo.ShardBits.AddShardId(id)
}

func (ecInfo *EcVolumeInfo) RemoveShardId(id ShardId) {
	ecInfo.ShardBits = ecInfo.ShardBits.RemoveShardId(id)
	// Also remove the shard size information
	delete(ecInfo.ShardSizes, id)
}

func (ecInfo *EcVolumeInfo) SetShardSize(id ShardId, size int64) {
	if ecInfo.ShardSizes == nil {
		ecInfo.ShardSizes = make(map[ShardId]int64)
	}
	ecInfo.ShardSizes[id] = size
}

func (ecInfo *EcVolumeInfo) GetShardSize(id ShardId) (int64, bool) {
	if ecInfo.ShardSizes == nil {
		return 0, false
	}
	size, exists := ecInfo.ShardSizes[id]
	return size, exists
}

func (ecInfo *EcVolumeInfo) GetTotalSize() int64 {
	var total int64
	if ecInfo.ShardSizes != nil {
		for _, size := range ecInfo.ShardSizes {
			total += size
		}
	}
	return total
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
		VolumeId:    ecInfo.VolumeId,
		Collection:  ecInfo.Collection,
		ShardBits:   ecInfo.ShardBits.Minus(other.ShardBits),
		DiskType:    ecInfo.DiskType,
		DiskId:      ecInfo.DiskId,
		ExpireAtSec: ecInfo.ExpireAtSec,
		ShardSizes:  make(map[ShardId]int64),
	}

	// Copy shard sizes for remaining shards
	if ecInfo.ShardSizes != nil {
		for shardId := ShardId(0); shardId < TotalShardsCount; shardId++ {
			if ret.ShardBits.HasShardId(shardId) {
				if size, exists := ecInfo.ShardSizes[shardId]; exists {
					ret.ShardSizes[shardId] = size
				}
			}
		}
	}

	return ret
}

func (ecInfo *EcVolumeInfo) ToVolumeEcShardInformationMessage() (ret *master_pb.VolumeEcShardInformationMessage) {
	shardIdToSize := make(map[uint32]int64)

	// Populate shard sizes from internal map
	if ecInfo.ShardSizes != nil {
		for shardId, size := range ecInfo.ShardSizes {
			shardIdToSize[uint32(shardId)] = size
		}
	}

	t := &master_pb.VolumeEcShardInformationMessage{
		Id:            uint32(ecInfo.VolumeId),
		EcIndexBits:   uint32(ecInfo.ShardBits),
		Collection:    ecInfo.Collection,
		DiskType:      ecInfo.DiskType,
		ExpireAtSec:   ecInfo.ExpireAtSec,
		DiskId:        ecInfo.DiskId,
		ShardIdToSize: shardIdToSize,
	}
	return t
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
