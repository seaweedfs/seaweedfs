package ecbalancer

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// shardDataShards returns the data-shard count of the volume an EC shard belongs
// to, used to size the shard's disk footprint. OSS uses the standard ratio for
// every volume; custom per-volume ratios are an enterprise feature, so the
// enterprise build overrides this to read the per-shard ratio.
func shardDataShards(eci *master_pb.VolumeEcShardInformationMessage) int {
	return erasure_coding.DataShardsCount
}
