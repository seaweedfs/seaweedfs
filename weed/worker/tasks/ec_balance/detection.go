package ec_balance

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding/ecbalancer"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	storagetypes "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/topology/balancer"
	"github.com/seaweedfs/seaweedfs/weed/util/wildcard"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Detection builds an EC balance topology snapshot from the cluster's active
// topology, runs the shared ecbalancer planner, and converts the planned moves
// into worker task proposals. The balancing policy lives in
// weed/storage/erasure_coding/ecbalancer, shared with the shell ec.balance
// command so the two cannot drift.
func Detection(
	ctx context.Context,
	metrics []*types.VolumeHealthMetrics,
	clusterInfo *types.ClusterInfo,
	config base.TaskConfig,
	maxResults int,
) ([]*types.TaskDetectionResult, bool, error) {
	if !config.IsEnabled() {
		return nil, false, nil
	}

	ecConfig := config.(*Config)
	if maxResults < 0 {
		maxResults = 0
	}

	if clusterInfo == nil || clusterInfo.ActiveTopology == nil {
		return nil, false, fmt.Errorf("active topology not available for EC balance detection")
	}
	topoInfo := clusterInfo.ActiveTopology.GetTopologyInfo()
	if topoInfo == nil {
		return nil, false, fmt.Errorf("topology info not available")
	}

	topo, nodeCount, volumeRatio := buildBalancerTopology(topoInfo, ecConfig)
	if nodeCount < ecConfig.MinServerCount {
		glog.V(1).Infof("EC balance: only %d servers, need at least %d", nodeCount, ecConfig.MinServerCount)
		return nil, false, nil
	}

	replicaPlacement := resolveReplicaPlacement(ecConfig, clusterInfo)

	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
	}

	// Canonical disk type for placement/execution: "hdd" -> "" (HardDriveType),
	// matching the topology's disk keys and the volume server's move RPCs.
	normalizedDiskType := storagetypes.ToDiskType(ecConfig.DiskType).String()

	moves := ecbalancer.Plan(topo, ecbalancer.Options{
		DiskType:           normalizedDiskType,
		ImbalanceThreshold: ecConfig.ImbalanceThreshold,
		ReplicaPlacement:   replicaPlacement,
		Ratio: func(collection string) (int, int) {
			return resolveECRatio(clusterInfo, collection)
		},
		// Prefer each volume's own heartbeat-reported ratio over the collection
		// default so a mixed-ratio collection is spread per volume; 0 defers to
		// resolveECRatio (and is the always-0 OSS case).
		VolumeRatio: volumeRatio,
		// Move incrementally across detection cycles rather than draining a rack
		// in one batch; the scheduler re-evaluates each cycle.
		GlobalMaxMovesPerRack: 10,
		// Balance heterogeneous-capacity racks by fractional fullness.
		GlobalUtilizationBased: true,
	})
	if len(moves) == 0 {
		return nil, false, nil
	}

	hasMore := false
	if maxResults > 0 && len(moves) > maxResults {
		moves = moves[:maxResults]
		hasMore = true
	}

	now := time.Now()
	results := make([]*types.TaskDetectionResult, 0, len(moves))
	for i, m := range moves {
		taskID := fmt.Sprintf("ec_balance_%d_%d_%s_%s_%d_%d",
			m.VolumeID, m.ShardID, m.SourceNode, m.TargetNode, now.UnixNano(), i)
		results = append(results, &types.TaskDetectionResult{
			TaskID:     taskID,
			TaskType:   types.TaskTypeECBalance,
			VolumeID:   m.VolumeID,
			Server:     m.SourceNode,
			Collection: m.Collection,
			Priority:   movePhasePriority(m.Phase),
			Reason: fmt.Sprintf("EC shard %d.%d %s: %s → %s",
				m.VolumeID, m.ShardID, m.Phase, m.SourceNode, m.TargetNode),
			ScheduleAt: now,
			TypedParams: &worker_pb.TaskParams{
				TaskId:     taskID,
				VolumeId:   m.VolumeID,
				Collection: m.Collection,
				Sources: []*worker_pb.TaskSource{{
					Node:     m.SourceNode,
					DiskId:   m.SourceDisk,
					Rack:     m.SourceRack,
					ShardIds: []uint32{uint32(m.ShardID)},
				}},
				Targets: []*worker_pb.TaskTarget{{
					Node:     m.TargetNode,
					DiskId:   m.TargetDisk,
					Rack:     m.TargetRack,
					ShardIds: []uint32{uint32(m.ShardID)},
				}},
				TaskParams: &worker_pb.TaskParams_EcBalanceParams{
					EcBalanceParams: &worker_pb.EcBalanceTaskParams{
						DiskType:       normalizedDiskType,
						TimeoutSeconds: 600,
					},
				},
			},
		})
	}

	glog.V(1).Infof("EC balance detection: %d moves proposed", len(results))
	return results, hasMore, nil
}

// buildBalancerTopology builds an ecbalancer.Topology from the master topology,
// applying the data-center, disk-type, and collection filters. Rack keys are
// dc:rack composites to avoid cross-DC name collisions. Per-disk free capacity
// is split evenly from the node total because the wire collapses same-type disks.
// Returns the topology, the number of eligible nodes (for MinServerCount), and a
// per-volume ratio lookup built from each shard's heartbeat (0,0 when unreported,
// e.g. always in OSS) which Plan prefers over the collection ratio for mixed-ratio
// clusters.
func buildBalancerTopology(topoInfo *master_pb.TopologyInfo, config *Config) (*ecbalancer.Topology, int, func(collection string, vid uint32) (int, int)) {
	topo := ecbalancer.NewTopology()
	allowedCollections := wildcard.CompileWildcardMatchers(config.CollectionFilter)

	type volRatioKey struct {
		collection string
		vid        uint32
	}
	volRatios := make(map[volRatioKey][2]int)

	// Normalize the disk-type filter: "hdd" (and the default "") map to the
	// HardDriveType, which the topology reports under the empty-string key. Keep a
	// separate "filter requested" flag so a configured "hdd" still filters to HDD
	// disks instead of being mistaken for "all disk types".
	filterByDiskType := config.DiskType != ""
	wantDiskType := storagetypes.ToDiskType(config.DiskType).String()

	nodeCount := 0
	for _, dc := range topoInfo.DataCenterInfos {
		if config.DataCenterFilter != "" {
			matchers := wildcard.CompileWildcardMatchers(config.DataCenterFilter)
			if !wildcard.MatchesAnyWildcard(matchers, dc.Id) {
				continue
			}
		}

		for _, rack := range dc.RackInfos {
			rackKey := dc.Id + ":" + rack.Id

			for _, dn := range rack.DataNodeInfos {
				freeSlots := 0
				diskTypeOf := make(map[uint32]string) // physical disk_id -> disk type
				diskShardCount := make(map[uint32]int)
				fullDiskTypes := make(map[string]bool) // physically near-full, ineligible as a target
				hasMatchingDisk := false

				for diskType, diskInfo := range dn.DiskInfos {
					if filterByDiskType && diskType != wantDiskType {
						continue
					}
					hasMatchingDisk = true

					// Don't place EC shards on a physically near-full disk, even if slot
					// math says it has room (an over-set maxVolumeCount hides real
					// fullness). Statfs free bytes already include EC shard files.
					if balancer.DiskTooFullAfter(diskInfo.DiskTotalBytes, diskInfo.DiskFreeBytes, 0, balancer.DefaultMaxDiskUsagePercent) {
						fullDiskTypes[diskType] = true
					}

					fs := int(diskInfo.MaxVolumeCount-diskInfo.VolumeCount)*erasure_coding.DataShardsCount - countEcShards(diskInfo.EcShardInfos)
					if fs > 0 && !fullDiskTypes[diskType] {
						freeSlots += fs
					}
					// Discover physical disks from regular volumes too, so an
					// EC-empty disk is still a candidate destination.
					for _, vi := range diskInfo.VolumeInfos {
						if _, ok := diskTypeOf[vi.DiskId]; !ok {
							diskTypeOf[vi.DiskId] = diskType
						}
					}
					for _, eci := range diskInfo.EcShardInfos {
						if _, ok := diskTypeOf[eci.DiskId]; !ok {
							diskTypeOf[eci.DiskId] = diskType
						}
						// Disk occupancy counts ALL volumes' shards (capacity model),
						// independent of the collection filter below.
						diskShardCount[eci.DiskId] += erasure_coding.GetShardCount(eci)
					}
				}

				if !hasMatchingDisk {
					continue
				}

				node := topo.AddNode(dn.Id, dc.Id, rackKey, freeSlots)
				// Group servers sharing a host so a volume's shards spread across
				// machines, not just nodes (servers on one host are one fault domain).
				node.SetHost(pb.NewServerAddressFromDataNode(dn).ToHost())

				// Spread the node's free slots only over its non-full disks, so a
				// physically full disk type doesn't dilute the share the usable disks
				// advertise (its own disks get 0 below). The total is preserved.
				nonFullDiskCount := 0
				for _, dt := range diskTypeOf {
					if !fullDiskTypes[dt] {
						nonFullDiskCount++
					}
				}
				perDiskFree := 0
				if nonFullDiskCount > 0 && freeSlots > 0 {
					perDiskFree = freeSlots / nonFullDiskCount
				}
				for diskID, diskType := range diskTypeOf {
					diskFree := perDiskFree
					if fullDiskTypes[diskType] {
						diskFree = 0
					}
					node.AddDisk(diskID, diskType, diskFree, diskShardCount[diskID])
				}

				// Add shards only for volumes whose collection passes the filter;
				// those are the volumes the planner will balance.
				for diskType, diskInfo := range dn.DiskInfos {
					if filterByDiskType && diskType != wantDiskType {
						continue
					}
					for _, eci := range diskInfo.EcShardInfos {
						if len(allowedCollections) > 0 && !wildcard.MatchesAnyWildcard(allowedCollections, eci.Collection) {
							continue
						}
						node.AddShards(eci.Id, eci.Collection, eci.DiskId, erasure_coding.ShardBits(eci.EcIndexBits))
						if d, p := ecbalancer.VolumeShardRatio(eci); d > 0 || p > 0 {
							volRatios[volRatioKey{eci.Collection, eci.Id}] = [2]int{d, p}
						}
					}
				}

				nodeCount++
			}
		}
	}

	volumeRatio := func(collection string, vid uint32) (int, int) {
		r := volRatios[volRatioKey{collection, vid}]
		return r[0], r[1]
	}
	return topo, nodeCount, volumeRatio
}

// resolveECRatio returns the (dataShards, parityShards) for a collection from the
// admin EC config snapshot when present, else the local default. This keeps the
// enterprise-only custom-ratio plumbing out of the shared planner.
func resolveECRatio(_ *types.ClusterInfo, _ string) (int, int) {
	// Custom EC ratios are an enterprise feature; OSS uses the standard scheme.
	return normalizeECShardCounts(0, 0)
}

// resolveReplicaPlacement picks the EC shard replica placement constraint: an
// explicit config value wins; otherwise it falls back to the master's default
// replication (matching the shell ec.balance default). A missing, invalid, or
// zero-replication value yields nil, meaning even spread / no constraint.
func resolveReplicaPlacement(ecConfig *Config, clusterInfo *types.ClusterInfo) *super_block.ReplicaPlacement {
	clusterDefault := ""
	if clusterInfo != nil {
		clusterDefault = clusterInfo.DefaultReplicaPlacement
	}
	return super_block.ResolveReplicaPlacement(ecConfig.ReplicaPlacement, clusterDefault)
}

func normalizeECShardCounts(dataShards, parityShards int) (int, int) {
	if dataShards <= 0 {
		dataShards = erasure_coding.DataShardsCount
	}
	if parityShards <= 0 {
		parityShards = erasure_coding.ParityShardsCount
	}
	return dataShards, parityShards
}

func countEcShards(ecShardInfos []*master_pb.VolumeEcShardInformationMessage) int {
	count := 0
	for _, eci := range ecShardInfos {
		count += erasure_coding.GetShardCount(eci)
	}
	return count
}

func movePhasePriority(phase string) types.TaskPriority {
	switch phase {
	case "dedup":
		return types.TaskPriorityHigh
	case "cross_rack":
		return types.TaskPriorityMedium
	default:
		return types.TaskPriorityLow
	}
}
