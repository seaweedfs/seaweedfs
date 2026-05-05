package erasure_coding

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// data structure used in master
type EcVolumeInfo struct {
	VolumeId    needle.VolumeId
	Collection  string
	DiskType    string
	DiskId      uint32 // ID of the disk this EC volume is on
	ExpireAtSec uint64 // ec volume destroy time, calculated from the ec volume was created
	ShardsInfo  *ShardsInfo
	FileCount   uint64 // live needle count for this EC volume (same on every node holding shards)
	DeleteCount uint64 // tombstoned needle count for this EC volume
}

func (ecInfo *EcVolumeInfo) Minus(other *EcVolumeInfo) *EcVolumeInfo {
	return &EcVolumeInfo{
		VolumeId:    ecInfo.VolumeId,
		Collection:  ecInfo.Collection,
		ShardsInfo:  ecInfo.ShardsInfo.Minus(other.ShardsInfo),
		DiskType:    ecInfo.DiskType,
		DiskId:      ecInfo.DiskId,
		ExpireAtSec: ecInfo.ExpireAtSec,
		FileCount:   ecInfo.FileCount,
		DeleteCount: ecInfo.DeleteCount,
	}
}

func (evi *EcVolumeInfo) ToVolumeEcShardInformationMessage() (ret *master_pb.VolumeEcShardInformationMessage) {
	return &master_pb.VolumeEcShardInformationMessage{
		Id:          uint32(evi.VolumeId),
		EcIndexBits: evi.ShardsInfo.Bitmap(),
		ShardSizes:  evi.ShardsInfo.SizesInt64(),
		Collection:  evi.Collection,
		DiskType:    evi.DiskType,
		ExpireAtSec: evi.ExpireAtSec,
		DiskId:      evi.DiskId,
		FileCount:   evi.FileCount,
		DeleteCount: evi.DeleteCount,
	}
}
