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

// VolumeShardRatio returns the RAW per-volume (dataShards, parityShards) reported
// on an EC shard's heartbeat, with 0 meaning "not reported". Custom per-volume
// ratios are an enterprise feature and the OSS proto has no data_shards/parity_shards
// fields, so this returns 0, 0 and the balancer falls back to the collection ratio
// (the standard scheme). The enterprise build overrides this to read the per-shard
// ratio so a mixed-ratio collection is spread by each volume's own data/parity split.
func VolumeShardRatio(eci *master_pb.VolumeEcShardInformationMessage) (dataShards, parityShards int) {
	return 0, 0
}
