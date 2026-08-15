package dash

import (
	"sort"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// TierStats aggregates the volumes and EC shards that live on one storage
// tier: a local disk type ("hdd", "ssd", or a custom tag), or the remote
// storage a tiered volume was uploaded to. A remote-tiered volume reports
// the size of its cloud object, so its bytes belong to the remote tier,
// not to the local disk that holds only its index.
type TierStats struct {
	Name         string `json:"name"`
	IsRemote     bool   `json:"is_remote"`
	VolumeCount  int    `json:"volume_count"`
	EcShardCount int    `json:"ec_shard_count"`
	DataSize     int64  `json:"data_size"`
	DiskUsed     int64  `json:"disk_used"`
	DiskCapacity int64  `json:"disk_capacity"`
	MaxVolumes   int64  `json:"max_volumes"`
}

// UsagePercent is the tier's local disk usage in percent, clamped to
// [0, 100]. Remote tiers have no capacity and return 0.
func (t TierStats) UsagePercent() int {
	if t.IsRemote || t.DiskCapacity <= 0 {
		return 0
	}
	percent := int(float64(t.DiskUsed) / float64(t.DiskCapacity) * 100)
	if percent < 0 {
		return 0
	}
	if percent > 100 {
		return 100
	}
	return percent
}

// tierDiskType maps the empty disk type to its display name.
func tierDiskType(diskType string) string {
	if diskType == "" {
		return "hdd"
	}
	return diskType
}

// CollectTierStats walks the topology and groups capacity and usage by
// tier. DiskUsed/DiskCapacity come from the statfs numbers the volume
// servers report per disk type; a disk that predates disk_total_bytes
// falls back to the slot-based capacity estimate and to the logical
// bytes it holds, so mixed-version tiers don't underreport usage.
// Remote tiers have no local disk, so only VolumeCount and DataSize are
// meaningful there.
func CollectTierStats(topo *master_pb.TopologyInfo, volumeSizeLimitMb uint64) []TierStats {
	if topo == nil {
		return nil
	}
	tiers := make(map[string]*TierStats)
	tier := func(name string, isRemote bool) *TierStats {
		key := name
		if isRemote {
			key = "remote\x00" + name
		}
		t := tiers[key]
		if t == nil {
			t = &TierStats{Name: name, IsRemote: isRemote}
			tiers[key] = t
		}
		return t
	}

	for _, dc := range topo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, node := range rack.DataNodeInfos {
				for _, diskInfo := range node.DiskInfos {
					local := tier(tierDiskType(diskInfo.Type), false)
					local.MaxVolumes += diskInfo.MaxVolumeCount
					hasStatfs := diskInfo.DiskTotalBytes > 0
					if hasStatfs {
						local.DiskCapacity += int64(diskInfo.DiskTotalBytes)
						if diskInfo.DiskTotalBytes > diskInfo.DiskFreeBytes {
							local.DiskUsed += int64(diskInfo.DiskTotalBytes - diskInfo.DiskFreeBytes)
						}
					} else {
						local.DiskCapacity += diskInfo.MaxVolumeCount * int64(volumeSizeLimitMb) * 1024 * 1024
					}

					var diskLocalBytes int64
					for _, volInfo := range diskInfo.VolumeInfos {
						if volInfo.RemoteStorageName != "" {
							remote := tier(volInfo.RemoteStorageName, true)
							remote.VolumeCount++
							remote.DataSize += int64(volInfo.Size)
						} else {
							local.VolumeCount++
							local.DataSize += int64(volInfo.Size)
							diskLocalBytes += int64(volInfo.Size)
						}
					}

					// ShardSizes is local to this node, so summing across
					// nodes gives the tier's physical footprint.
					for _, ecShardInfo := range diskInfo.EcShardInfos {
						local.EcShardCount += erasure_coding.GetShardCount(ecShardInfo)
						ecBytes := erasure_coding.EcShardsTotalSize(ecShardInfo)
						local.DataSize += ecBytes
						diskLocalBytes += ecBytes
					}

					// Without statfs numbers, approximate this disk's
					// footprint with the logical bytes it holds.
					if !hasStatfs {
						local.DiskUsed += diskLocalBytes
					}
				}
			}
		}
	}

	result := make([]TierStats, 0, len(tiers))
	for _, t := range tiers {
		result = append(result, *t)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].IsRemote != result[j].IsRemote {
			return !result[i].IsRemote
		}
		return result[i].Name < result[j].Name
	})
	return result
}
