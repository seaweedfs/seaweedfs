package erasure_coding

import (
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// isStubReplica reports whether a regular replica's .dat is too small to hold
// any data — at most a bare superblock. An interrupted encode or copy can
// leave a 0-byte .dat, or write the 8-byte superblock and then fail; either
// way the file is not an encode source and must not be counted toward
// size/fullness checks.
func isStubReplica(size uint64) bool {
	return size <= uint64(super_block.SuperBlockSize)
}

// selectCanonicalMetric picks the metric that drives the encode checks and
// names the encode source server. It prefers the lowest-server credible
// replica — data-bearing and not already EC — so a 0-byte stub or a leftover
// EC shard set sharing the volume id cannot become canonical and strand the
// volume (a stub trips the min-size gate; an EC metric trips the IsECVolume
// guard, hiding the volume from both orphan-source cleanup and re-encode).
// When nothing is credible there is nothing to encode, so the lowest-server
// metric is returned unchanged and the downstream gates make the skip
// decision as before.
func selectCanonicalMetric(group []*types.VolumeHealthMetrics) *types.VolumeHealthMetrics {
	var lowest, credible *types.VolumeHealthMetrics
	for _, m := range group {
		if lowest == nil || m.Server < lowest.Server {
			lowest = m
		}
		if m.IsECVolume || isStubReplica(m.Size) {
			continue
		}
		if credible == nil || m.Server < credible.Server {
			credible = m
		}
	}
	if credible != nil {
		return credible
	}
	return lowest
}
