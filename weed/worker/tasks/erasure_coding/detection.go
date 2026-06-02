package erasure_coding

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding/ecbalancer"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/util/wildcard"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	workerutil "github.com/seaweedfs/seaweedfs/weed/worker/tasks/util"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

const (
	minProposalsBeforeEarlyStop    = 10
	maxConsecutivePlanningFailures = 10
)

// Detection implements the detection logic for erasure coding tasks.
// It respects ctx cancellation and can stop early once maxResults is reached.
func Detection(ctx context.Context, metrics []*types.VolumeHealthMetrics, clusterInfo *types.ClusterInfo, config base.TaskConfig, maxResults int) ([]*types.TaskDetectionResult, bool, error) {
	if !config.IsEnabled() {
		return nil, false, nil
	}

	if maxResults < 0 {
		maxResults = 0
	}

	ecConfig := config.(*Config)
	var results []*types.TaskDetectionResult
	hasMore := false
	stoppedEarly := false
	now := time.Now()
	quietThreshold := time.Duration(ecConfig.QuietForSeconds) * time.Second
	minSizeBytes := uint64(ecConfig.MinSizeMB) * 1024 * 1024 // Configurable minimum

	debugCount := 0
	skippedAlreadyEC := 0
	skippedTooSmall := 0
	skippedCollectionFilter := 0
	skippedQuietTime := 0
	skippedFullness := 0
	skippedRemote := 0
	skippedTooFewNodes := 0
	consecutivePlanningFailures := 0

	// EC shard replica placement: explicit config wins, else the master default.
	var replicaPlacement *super_block.ReplicaPlacement
	if clusterInfo != nil {
		replicaPlacement = super_block.ResolveReplicaPlacement(ecConfig.ReplicaPlacement, clusterInfo.DefaultReplicaPlacement)
	}
	// EC placement honors only the rack/node digits; the data-center digit can't
	// express a useful per-DC EC shard cap (it maxes at 2). Warn once per cycle so a
	// 1xx/2xx setting isn't silently ineffective.
	if replicaPlacement != nil && replicaPlacement.DiffDataCenterCount > 0 {
		glog.Warningf("EC Detection: replica placement data-center digit (%d) is ignored for EC; only rack/node digits are honored", replicaPlacement.DiffDataCenterCount)
	}

	allowedCollections := wildcard.CompileWildcardMatchers(ecConfig.CollectionFilter)

	// Cluster node count for the min-node safety gate (mirrors the shell ec.encode
	// guard that refuses to encode when nodes < parity shards, so shards cannot be
	// spread for fault tolerance).
	clusterNodeCount := countTopologyNodes(clusterInfo.ActiveTopology)

	// Group metrics by VolumeID to handle replicas and select canonical server
	volumeGroups := make(map[uint32][]*types.VolumeHealthMetrics)
	for _, metric := range metrics {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return results, hasMore, err
			}
		}
		volumeGroups[metric.VolumeID] = append(volumeGroups[metric.VolumeID], metric)
	}

	groupKeys := make([]uint32, 0, len(volumeGroups))
	for volumeID := range volumeGroups {
		groupKeys = append(groupKeys, volumeID)
	}
	sort.Slice(groupKeys, func(i, j int) bool { return groupKeys[i] < groupKeys[j] })

	// Build the EC placement snapshot once per detection cycle. planECDestinations
	// reserves the shards it assigns directly into it, so volumes planned later in
	// this cycle see the reduced capacity. Rebuilding it per volume re-walked the
	// whole topology (and ActiveTopology's growing pending-task set) every time,
	// which is O(volumes × topology) and times out on large clusters. The node
	// address map is precomputed for the same reason (ResolveServerAddress rebuilds
	// the full node map on every call).
	var ecSnapshot *ecbalancer.Topology
	var nodeAddresses map[string]string
	if clusterInfo != nil && clusterInfo.ActiveTopology != nil {
		ecSnapshot = ecbalancer.FromActiveTopology(clusterInfo.ActiveTopology, erasure_coding.DataShardsCount)
		nodeAddresses = buildNodeAddressMap(clusterInfo.ActiveTopology)
	}

	// Iterate over groups to check criteria and creation tasks
	for idx, volumeID := range groupKeys {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return results, hasMore, err
			}
		}
		if maxResults > 0 && len(results) >= maxResults {
			if idx+1 < len(groupKeys) {
				hasMore = true
			}
			stoppedEarly = true
			break
		}

		groupMetrics := volumeGroups[volumeID]

		// Prefer the lowest-server credible replica so a 0-byte stub or a
		// leftover EC shard set on a lower server can't become canonical and
		// strand the volume in skippedTooSmall / skippedAlreadyEC.
		metric := selectCanonicalMetric(groupMetrics)

		// Skip if already EC volume
		if metric.IsECVolume {
			skippedAlreadyEC++
			continue
		}

		// Handle the "stuck source" state from #9448: a previous encode
		// succeeded but the post-encode source-delete left a regular replica
		// behind, so the master heartbeats BOTH the replica AND its EC shards.
		// metric.IsECVolume above is set only for the EC-side metric path, so
		// the canonical metric we picked is the regular replica with
		// IsECVolume=false. Re-proposing an encode in that state collides with
		// the mounted shards on the targets ("ec volume %d is mounted; refusing
		// overwrite") and the detector re-queues forever.
		//
		// We only act when the EC shard set is COMPLETE — fewer than
		// totalShards present means the existing recovery branch below
		// (around the `existingECShards` block) should keep its chance to
		// fold the partial shards into the new task. Counting walks
		// EcIndexBits to handle a single info entry carrying multiple shards.
		if clusterInfo.ActiveTopology != nil {
			shardCount := countExistingEcShardsForVolume(clusterInfo.ActiveTopology, metric.VolumeID, metric.Collection)
			totalShards := erasure_coding.DataShardsCount + erasure_coding.ParityShardsCount
			if shardCount >= totalShards {
				glog.Warningf("EC Detection: Volume %d has all %d EC shards in topology; "+
					"source replica on %s is orphaned (#9448).",
					metric.VolumeID, totalShards, metric.Server)
				if clusterInfo.GrpcDialOption != nil {
					deleted, cleanupErr := cleanupOrphanSourceReplicas(ctx, clusterInfo, metric, totalShards)
					switch {
					case cleanupErr != nil:
						// Don't fall through to a re-encode — that would just
						// collide with the mounted shards again. Surface the
						// failure and wait for the next cycle; the source is
						// still safe.
						glog.Warningf("EC Detection: failed to auto-clean orphaned source for volume %d: %v", metric.VolumeID, cleanupErr)
					case deleted > 0:
						glog.Infof("EC Detection: auto-cleaned %d orphaned source replica(s) for volume %d after verifying all %d EC shards present", deleted, metric.VolumeID, totalShards)
					default:
						glog.V(1).Infof("EC Detection: no orphaned regular replicas found in topology for volume %d (collection %q)", metric.VolumeID, metric.Collection)
					}
				} else {
					glog.Warningf("EC Detection: no gRPC dial option available to auto-clean orphaned source for volume %d; "+
						"to clean up by hand, send a targeted VolumeDelete RPC to %s only — DO NOT use the cluster-wide `volume.delete` shell command, which would also delete the EC shards.",
						metric.VolumeID, metric.Server)
				}
				skippedAlreadyEC++
				continue
			}
		}

		// Check minimum size requirement
		if metric.Size < minSizeBytes {
			skippedTooSmall++
			continue
		}

		// Check collection filter if specified
		if len(allowedCollections) > 0 && !wildcard.MatchesAnyWildcard(allowedCollections, metric.Collection) {
			skippedCollectionFilter++
			continue
		}

		// Skip remote/tiered volumes: encoding them would lose the tiering. The
		// shell ec.encode excludes remote volumes for the same reason.
		if metric.HasRemoteCopy {
			skippedRemote++
			continue
		}

		// Min-node safety gate: don't encode when the cluster has fewer nodes than
		// this collection's parity shards — shards could not be spread to tolerate
		// failures. Mirrors the shell ec.encode guard (node count < parity shards).
		if clusterNodeCount > 0 && clusterNodeCount < erasure_coding.ParityShardsCount {
			skippedTooFewNodes++
			continue
		}

		// Check quiet duration and fullness criteria
		if metric.Age >= quietThreshold && metric.FullnessRatio >= ecConfig.FullnessRatio {
			if ctx != nil {
				if err := ctx.Err(); err != nil {
					return results, hasMore, err
				}
			}
			glog.Infof("EC Detection: Volume %d meets all criteria, attempting to create task", metric.VolumeID)

			// Generate task ID for ActiveTopology integration
			taskID := fmt.Sprintf("ec_vol_%d_%d", metric.VolumeID, now.Unix())

			result := &types.TaskDetectionResult{
				TaskID:     taskID, // Link to ActiveTopology pending task
				TaskType:   types.TaskTypeErasureCoding,
				VolumeID:   metric.VolumeID,
				Server:     metric.Server,
				Collection: metric.Collection,
				Priority:   types.TaskPriorityLow, // EC is not urgent
				Reason: fmt.Sprintf("Volume meets EC criteria: quiet for %.1fs (>%ds), fullness=%.1f%% (>%.1f%%), size=%.1fMB (>%dMB)",
					metric.Age.Seconds(), ecConfig.QuietForSeconds, metric.FullnessRatio*100, ecConfig.FullnessRatio*100,
					float64(metric.Size)/(1024*1024), ecConfig.MinSizeMB),
				ScheduleAt: now,
			}

			// Plan EC destinations if ActiveTopology is available
			if clusterInfo.ActiveTopology != nil {
				// Check if ANY task already exists in ActiveTopology for this volume
				if clusterInfo.ActiveTopology.HasAnyTask(metric.VolumeID) {
					glog.V(2).Infof("EC Detection: Skipping volume %d, task already exists in ActiveTopology", metric.VolumeID)
					continue
				}

				glog.Infof("EC Detection: ActiveTopology available, planning destinations for volume %d", metric.VolumeID)
				dataShards := erasure_coding.DataShardsCount
				parityShards := erasure_coding.ParityShardsCount
				multiPlan, shardsPerPlan, err := planECDestinations(ecSnapshot, nodeAddresses, metric, ecConfig, replicaPlacement, dataShards, parityShards)
				if err != nil {
					glog.V(2).Infof("Failed to plan EC destinations for volume %d: %v", metric.VolumeID, err)
					consecutivePlanningFailures++
					if len(results) >= minProposalsBeforeEarlyStop && consecutivePlanningFailures >= maxConsecutivePlanningFailures {
						glog.Warningf("EC Detection: stopping early after %d consecutive placement failures with %d proposals already planned", consecutivePlanningFailures, len(results))
						hasMore = true
						stoppedEarly = true
						break
					}
					continue // Skip this volume if destination planning fails
				}
				consecutivePlanningFailures = 0
				glog.Infof("EC Detection: Successfully planned %d destinations for volume %d", len(multiPlan.Plans), metric.VolumeID)

				// Calculate expected shard size for EC operation
				// Each data shard will be approximately volumeSize / dataShards
				expectedShardSize := uint64(metric.Size) / uint64(dataShards)

				// Add pending EC shard task to ActiveTopology for capacity management

				// Extract shard destinations from multiPlan
				var shardDestinations []string
				var shardDiskIDs []uint32
				for _, plan := range multiPlan.Plans {
					shardDestinations = append(shardDestinations, plan.TargetNode)
					shardDiskIDs = append(shardDiskIDs, plan.TargetDisk)
				}

				// Find all volume replica locations (server + disk) from topology
				glog.Infof("EC Detection: Looking for replica locations for volume %d", metric.VolumeID)
				replicaLocations := findVolumeReplicaLocations(clusterInfo.ActiveTopology, metric.VolumeID, metric.Collection)
				if len(replicaLocations) == 0 {
					glog.Warningf("No replica locations found for volume %d, skipping EC", metric.VolumeID)
					continue
				}
				glog.Infof("EC Detection: Found %d replica locations for volume %d", len(replicaLocations), metric.VolumeID)

				// Find existing EC shards from previous failed attempts
				existingECShards := findExistingECShards(clusterInfo.ActiveTopology, metric.VolumeID, metric.Collection)

				// Combine volume replicas and existing EC shards for cleanup
				var sources []topology.TaskSourceSpec

				// Add volume replicas (will free volume slots)
				for _, replica := range replicaLocations {
					sources = append(sources, topology.TaskSourceSpec{
						ServerID:    replica.ServerID,
						DiskID:      replica.DiskID,
						DataCenter:  replica.DataCenter,
						Rack:        replica.Rack,
						CleanupType: topology.CleanupVolumeReplica,
					})
				}

				// Add existing EC shards (will free shard slots)
				duplicateCheck := make(map[string]bool)
				for _, replica := range replicaLocations {
					key := fmt.Sprintf("%s:%d", replica.ServerID, replica.DiskID)
					duplicateCheck[key] = true
				}

				for _, shard := range existingECShards {
					key := fmt.Sprintf("%s:%d", shard.ServerID, shard.DiskID)
					if !duplicateCheck[key] { // Avoid duplicates if EC shards are on same disk as volume replicas
						shardIds := append([]uint32(nil), shard.ShardIds...)
						// Free exactly the shards on this disk. Without an explicit
						// impact the cleanup falls back to CalculateECShardCleanupImpact,
						// which credits TotalShardsCount (14) regardless of ratio.
						cleanupImpact := topology.StorageSlotChange{ShardSlots: -int32(len(shardIds))}
						sources = append(sources, topology.TaskSourceSpec{
							ServerID:      shard.ServerID,
							DiskID:        shard.DiskID,
							DataCenter:    shard.DataCenter,
							Rack:          shard.Rack,
							CleanupType:   topology.CleanupECShards,
							ShardIds:      shardIds,
							StorageImpact: &cleanupImpact,
						})
						duplicateCheck[key] = true
					}
				}

				glog.V(2).Infof("Found %d volume replicas and %d existing EC shards for volume %d (total %d cleanup sources)",
					len(replicaLocations), len(existingECShards), metric.VolumeID, len(sources))

				// Convert shard destinations to TaskDestinationSpec. A destination may
				// hold several shards (small clusters), so reserve capacity for the
				// actual per-disk shard count that Place assigned (shardsPerPlan),
				// which is exactly what createECTargets writes.
				destinations := make([]topology.TaskDestinationSpec, len(shardDestinations))
				for i, dest := range shardDestinations {
					shardCount := len(shardsPerPlan[i])
					shardImpact := topology.CalculateECShardStorageImpact(int32(shardCount), int64(expectedShardSize))
					destSize := int64(expectedShardSize) * int64(shardCount)
					destinations[i] = topology.TaskDestinationSpec{
						ServerID:      dest,
						DiskID:        shardDiskIDs[i],
						StorageImpact: &shardImpact,
						EstimatedSize: &destSize,
					}
				}

				// Convert sources before mutating topology
				sourcesProto, err := convertTaskSourcesToProtobuf(sources, metric.VolumeID, clusterInfo.ActiveTopology)
				if err != nil {
					glog.Warningf("Failed to convert sources for EC task on volume %d: %v, skipping", metric.VolumeID, err)
					continue
				}

				err = clusterInfo.ActiveTopology.AddPendingTask(topology.TaskSpec{
					TaskID:       taskID,
					TaskType:     topology.TaskTypeErasureCoding,
					VolumeID:     metric.VolumeID,
					VolumeSize:   int64(metric.Size),
					Sources:      sources,
					Destinations: destinations,
				})
				if err != nil {
					glog.Warningf("Failed to add pending EC shard task to ActiveTopology for volume %d: %v", metric.VolumeID, err)
					continue // Skip this volume if topology task addition fails
				}

				// Cross-volume in-cycle capacity is tracked by ActiveTopology via the
				// pending task above, which the next volume's FromActiveTopology snapshot
				// reflects; no separate planner reservation is needed.

				glog.V(2).Infof("Added pending EC shard task %s to ActiveTopology for volume %d with %d cleanup sources and %d shard destinations",
					taskID, metric.VolumeID, len(sources), len(multiPlan.Plans))

				// Create unified sources and targets for EC task
				result.TypedParams = &worker_pb.TaskParams{
					TaskId:     taskID, // Link to ActiveTopology pending task
					VolumeId:   metric.VolumeID,
					Collection: metric.Collection,
					VolumeSize: metric.Size, // Store original volume size for tracking changes

					// Unified sources - all sources that will be processed/cleaned up
					Sources: sourcesProto,

					// Unified targets - all EC shard destinations
					Targets: createECTargets(multiPlan, shardsPerPlan),

					TaskParams: &worker_pb.TaskParams_ErasureCodingParams{
						ErasureCodingParams: createECTaskParams(dataShards, parityShards, metric.DiskType),
					},
				}

				glog.V(1).Infof("Planned EC destinations for volume %d: %d shards across %d racks, %d DCs",
					metric.VolumeID, len(multiPlan.Plans), multiPlan.SuccessfulRack, multiPlan.SuccessfulDCs)
			} else {
				glog.Warningf("No ActiveTopology available for destination planning in EC detection")
				continue // Skip this volume if no topology available
			}

			glog.Infof("EC Detection: Successfully created EC task for volume %d, adding to results", metric.VolumeID)
			results = append(results, result)
		} else {
			// Count debug reasons
			if metric.Age < quietThreshold {
				skippedQuietTime++
			}
			if metric.FullnessRatio < ecConfig.FullnessRatio {
				skippedFullness++
			}

			if debugCount < 5 { // Limit to avoid spam
				// Logic moved outside
			}
			debugCount++
		}
	}

	// Log debug summary if no tasks were created
	if len(results) == 0 && len(metrics) > 0 && !stoppedEarly {
		totalVolumes := len(metrics)
		glog.V(1).Infof("EC detection: No tasks created for %d volumes (skipped: %d already EC, %d too small, %d filtered, %d not quiet, %d not full, %d remote, %d too few nodes)",
			totalVolumes, skippedAlreadyEC, skippedTooSmall, skippedCollectionFilter, skippedQuietTime, skippedFullness, skippedRemote, skippedTooFewNodes)

		// Show details for first few volumes
		for i, metric := range metrics {
			if i >= 3 || metric.IsECVolume { // Limit to first 3 non-EC volumes
				continue
			}
			sizeMB := float64(metric.Size) / (1024 * 1024)
			glog.V(1).Infof("ERASURE CODING: Volume %d: size=%.1fMB (need ≥%dMB), age=%s (need ≥%s), fullness=%.1f%% (need ≥%.1f%%)",
				metric.VolumeID, sizeMB, ecConfig.MinSizeMB, metric.Age.Truncate(time.Minute), quietThreshold.Truncate(time.Minute),
				metric.FullnessRatio*100, ecConfig.FullnessRatio*100)
		}
	}

	return results, hasMore, nil
}

// countTopologyNodes counts volume-server nodes in the active topology, used by
// the min-node safety gate.
func countTopologyNodes(at *topology.ActiveTopology) int {
	if at == nil {
		return 0
	}
	topo := at.GetTopologyInfo()
	if topo == nil {
		return 0
	}
	n := 0
	for _, dc := range topo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			n += len(rack.DataNodeInfos)
		}
	}
	return n
}

// buildNodeAddressMap resolves every node's server address once per detection
// cycle. ResolveServerAddress rebuilds the full node map on each call, so
// resolving per shard destination is O(destinations × topology); callers build
// this map once and look up addresses from it.
func buildNodeAddressMap(at *topology.ActiveTopology) map[string]string {
	if at == nil {
		return nil
	}
	allNodes := at.GetAllNodes()
	m := make(map[string]string, len(allNodes))
	for id, n := range allNodes {
		m[id] = string(pb.NewServerAddressFromDataNode(n))
	}
	return m
}

// planECDestinations places all shards of the volume via the shared ecbalancer
// policy and returns the per-disk destination plans plus, parallel to them, the
// shard ids ecbalancer.Place assigned to each disk (so createECTargets and the
// capacity reservations use the real assignment, not a round-robin guess).
//
// snap is the cycle-wide placement snapshot, built once by the caller and reused
// across volumes: Place reserves the shards it assigns into it, so later volumes
// in the same detection cycle see the reduced capacity. Rebuilding it per volume
// is O(volumes × topology) and times out on large clusters.
//
// Encode is lenient (PlaceDurabilityFirst): it relaxes caps/anti-affinity/RP as
// needed rather than fail, and prefers the source disk type but spills if that
// type can't hold every shard. rp is the resolved replica placement (may be nil).
func planECDestinations(snap *ecbalancer.Topology, nodeAddresses map[string]string, metric *types.VolumeHealthMetrics, ecConfig *Config, rp *super_block.ReplicaPlacement, dataShards, parityShards int) (*topology.MultiDestinationPlan, [][]uint32, error) {
	if snap == nil {
		return nil, nil, fmt.Errorf("EC placement snapshot not available")
	}
	if dataShards <= 0 || parityShards <= 0 {
		return nil, nil, fmt.Errorf("invalid EC ratio: dataShards=%d parityShards=%d", dataShards, parityShards)
	}
	totalShards := dataShards + parityShards
	// Survive losing one disk: each disk holds at most parityShards shards,
	// so we need at least ceil(totalShards / parityShards) disks.
	minTotalDisks := (totalShards + parityShards - 1) / parityShards
	expectedShardSize := uint64(metric.Size) / uint64(dataShards)

	// Encode is greenfield: any EC shards already present for this volume are stale
	// leftovers from a prior failed attempt, which the task deletes
	// (cleanupStaleEcShards) before distributing the new shards. Release them so they
	// don't occupy capacity or skew anti-affinity / per-disk caps during planning.
	snap.ReleaseVolumeShards(metric.Collection, metric.VolumeID)
	need := make([]int, totalShards)
	for i := range need {
		need[i] = i
	}
	res, err := snap.Place(metric.VolumeID, metric.Collection, need, ecbalancer.Constraints{
		DiskType:         metric.DiskType,
		DiskTypePolicy:   ecbalancer.DiskTypePrefer,
		PreferredTags:    ecConfig.PreferredTags,
		ReplicaPlacement: rp,
		Ratio:            func(string) (int, int) { return dataShards, parityShards },
	}, ecbalancer.PlaceDurabilityFirst)
	if err != nil {
		return nil, nil, err
	}
	if res.SpilledToOtherDiskType {
		glog.Warningf("EC volume %d: placed shards outside preferred disk type %q", metric.VolumeID, metric.DiskType)
	}
	if res.SpilledOutsidePreferredTags {
		glog.Warningf("EC volume %d: placed shards outside preferred tags %v", metric.VolumeID, ecConfig.PreferredTags)
	}
	if len(res.Relaxed) > 0 {
		// Encode is best-effort (PlaceDurabilityFirst): it relaxes these constraints
		// rather than defer when the cluster can't satisfy them. Surface it so a tight
		// replica placement isn't silently weakened; rebalancing tightens the spread.
		glog.Warningf("EC volume %d: placed with relaxed constraints %v; replica placement not fully satisfied (rebalancing will adjust)", metric.VolumeID, res.Relaxed)
	}

	// Group the per-shard destinations into one plan per (node,disk), iterating
	// shard ids in order for determinism.
	type diskGroup struct {
		node, rack, dc string
		diskID         uint32
		shards         []uint32
	}
	type diskKey struct {
		node   string
		diskID uint32
	}
	groups := make(map[diskKey]*diskGroup, totalShards)
	order := make([]diskKey, 0, totalShards)
	for sid := 0; sid < totalShards; sid++ {
		d, ok := res.Destinations[sid]
		if !ok {
			return nil, nil, fmt.Errorf("EC volume %d: shard %d was not placed", metric.VolumeID, sid)
		}
		key := diskKey{node: d.Node, diskID: d.DiskID}
		g := groups[key]
		if g == nil {
			g = &diskGroup{node: d.Node, rack: d.Rack, dc: d.DataCenter, diskID: d.DiskID}
			groups[key] = g
			order = append(order, key)
		}
		g.shards = append(g.shards, uint32(sid))
	}
	if len(order) < minTotalDisks {
		return nil, nil, fmt.Errorf("placed onto %d disks, but EC %d+%d needs at least %d so no disk holds more than %d shards",
			len(order), dataShards, parityShards, minTotalDisks, parityShards)
	}

	var plans []*topology.DestinationPlan
	shardsPerPlan := make([][]uint32, 0, len(order))
	rackCount := make(map[string]int)
	dcCount := make(map[string]int)
	for _, key := range order {
		g := groups[key]
		targetAddress, ok := nodeAddresses[g.node]
		if !ok {
			return nil, nil, fmt.Errorf("failed to resolve address for target server %s", g.node)
		}
		plans = append(plans, &topology.DestinationPlan{
			TargetNode:    g.node,
			TargetAddress: targetAddress,
			TargetDisk:    g.diskID,
			TargetRack:    g.rack,
			TargetDC:      g.dc,
			ExpectedSize:  expectedShardSize,
		})
		shardsPerPlan = append(shardsPerPlan, g.shards)
		rackCount[fmt.Sprintf("%s:%s", g.dc, g.rack)]++
		dcCount[g.dc]++
	}

	glog.V(1).Infof("Planned EC destinations for volume %d (size=%d bytes): expected shard size=%d bytes, %d shards across %d disks, %d racks, %d DCs",
		metric.VolumeID, metric.Size, expectedShardSize, totalShards, len(plans), len(rackCount), len(dcCount))

	return &topology.MultiDestinationPlan{
		Plans:          plans,
		TotalShards:    totalShards,
		SuccessfulRack: len(rackCount),
		SuccessfulDCs:  len(dcCount),
	}, shardsPerPlan, nil
}

// createECTargets builds TaskTargets from the per-disk plans and the shard ids
// ecbalancer.Place assigned to each (shardsPerPlan is parallel to multiPlan.Plans).
func createECTargets(multiPlan *topology.MultiDestinationPlan, shardsPerPlan [][]uint32) []*worker_pb.TaskTarget {
	targets := make([]*worker_pb.TaskTarget, 0, len(multiPlan.Plans))
	for i, plan := range multiPlan.Plans {
		shardIDs := shardsPerPlan[i]
		targets = append(targets, &worker_pb.TaskTarget{
			Node:          plan.TargetAddress,
			DiskId:        plan.TargetDisk,
			Rack:          plan.TargetRack,
			DataCenter:    plan.TargetDC,
			ShardIds:      shardIDs,
			EstimatedSize: plan.ExpectedSize,
		})
		glog.V(2).Infof("EC planning: target %s disk %d assigned shards %v", plan.TargetNode, plan.TargetDisk, shardIDs)
	}
	return targets
}

// convertTaskSourcesToProtobuf converts topology.TaskSourceSpec to worker_pb.TaskSource
func convertTaskSourcesToProtobuf(sources []topology.TaskSourceSpec, volumeID uint32, activeTopology *topology.ActiveTopology) ([]*worker_pb.TaskSource, error) {
	var protobufSources []*worker_pb.TaskSource

	for _, source := range sources {
		serverAddress, err := workerutil.ResolveServerAddress(source.ServerID, activeTopology)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve address for source server %s: %v", source.ServerID, err)
		}

		pbSource := &worker_pb.TaskSource{
			Node:       serverAddress,
			DiskId:     source.DiskID,
			DataCenter: source.DataCenter,
			Rack:       source.Rack,
		}

		// Convert storage impact to estimated size
		if source.EstimatedSize != nil {
			pbSource.EstimatedSize = uint64(*source.EstimatedSize)
		}

		// Populated ShardIds is the wire-level marker that flags an
		// EC-shard cleanup source; the worker routes it through
		// cleanupStaleEcShards and skips it in getReplicas.
		switch source.CleanupType {
		case topology.CleanupVolumeReplica:
			pbSource.VolumeId = volumeID
		case topology.CleanupECShards:
			pbSource.VolumeId = volumeID
			pbSource.ShardIds = append([]uint32(nil), source.ShardIds...)
		}

		protobufSources = append(protobufSources, pbSource)
	}

	return protobufSources, nil
}

// createECTaskParams creates clean EC task parameters (destinations now in unified targets).
// sourceDiskType is forwarded to VolumeEcShardsMount so the resulting EC volume
// reports under the source's disk type rather than the target location's (#9423).
func createECTaskParams(dataShards, parityShards int, sourceDiskType string) *worker_pb.ErasureCodingTaskParams {
	return &worker_pb.ErasureCodingTaskParams{
		DataShards:     int32(dataShards),
		ParityShards:   int32(parityShards),
		SourceDiskType: sourceDiskType,
	}
}

// findVolumeReplicaLocations finds all replica locations (server + disk) for the specified volume
// Uses O(1) indexed lookup for optimal performance on large clusters.
func findVolumeReplicaLocations(activeTopology *topology.ActiveTopology, volumeID uint32, collection string) []topology.VolumeReplica {
	if activeTopology == nil {
		return nil
	}
	return activeTopology.GetVolumeLocations(volumeID, collection)
}

// findExistingECShards finds existing EC shards for a volume (from previous failed EC attempts)
// Uses O(1) indexed lookup for optimal performance on large clusters.
func findExistingECShards(activeTopology *topology.ActiveTopology, volumeID uint32, collection string) []topology.VolumeReplica {
	if activeTopology == nil {
		return nil
	}
	return activeTopology.GetECShardLocations(volumeID, collection)
}

// cleanupOrphanSourceReplicas deletes any regular volume replicas still
// present in the topology for (volumeID, collection) after re-verifying that
// the full EC shard set is intact. Caller must hold expectedShards equal to
// the configured totalShards count. Issues VolumeDelete RPC to each replica
// server's address — that endpoint only touches the regular volume on the
// targeted server, never EC shards (those live in a separate store path).
// The cluster-wide `volume.delete` shell command is what would have nuked
// the EC shards too; the targeted RPC used here is safe by construction.
// Returns the count of replicas successfully deleted plus any error.
func cleanupOrphanSourceReplicas(ctx context.Context, clusterInfo *types.ClusterInfo, metric *types.VolumeHealthMetrics, expectedShards int) (int, error) {
	if clusterInfo == nil || clusterInfo.ActiveTopology == nil {
		return 0, fmt.Errorf("active topology unavailable")
	}
	if clusterInfo.GrpcDialOption == nil {
		return 0, fmt.Errorf("grpc dial option unavailable")
	}

	// Re-verify shard completeness right before acting. Defensive: detection
	// processes many volumes sequentially and the topology snapshot we built
	// at start-of-detection could have lost shards in between (a volume
	// server going down between iterations). Refusing to delete the source
	// when we can no longer prove the shards are complete is the safer
	// failure mode — the source replica is the only complete copy.
	actualShards := countExistingEcShardsForVolume(clusterInfo.ActiveTopology, metric.VolumeID, metric.Collection)
	if actualShards < expectedShards {
		return 0, fmt.Errorf("EC shard set shrank between detection and cleanup (%d < %d); refusing to delete source replica", actualShards, expectedShards)
	}

	replicas := findVolumeReplicaLocations(clusterInfo.ActiveTopology, metric.VolumeID, metric.Collection)
	if len(replicas) == 0 {
		return 0, nil
	}

	deleted := 0
	var deleteErrors []string
	for _, replica := range replicas {
		serverAddress := replica.ServerID
		err := operation.WithVolumeServerClient(false, pb.ServerAddress(serverAddress), clusterInfo.GrpcDialOption,
			func(client volume_server_pb.VolumeServerClient) error {
				_, deleteErr := client.VolumeDelete(ctx, &volume_server_pb.VolumeDeleteRequest{
					VolumeId:  metric.VolumeID,
					OnlyEmpty: false,
				})
				return deleteErr
			})
		if err != nil {
			deleteErrors = append(deleteErrors, fmt.Sprintf("server %s: %v", serverAddress, err))
			continue
		}
		deleted++
		glog.V(1).Infof("EC Detection: deleted orphan regular replica for volume %d on %s", metric.VolumeID, serverAddress)
	}

	if len(deleteErrors) > 0 {
		return deleted, fmt.Errorf("%d of %d replica delete(s) failed: %s", len(deleteErrors), len(replicas), strings.Join(deleteErrors, "; "))
	}
	return deleted, nil
}

// countExistingEcShardsForVolume returns the number of distinct EC shard IDs
// for (volumeID, collection) present in the topology. Walks every disk's
// EcIndexBits bitmap rather than trusting len(EcShardInfos), because a single
// info entry can carry multiple shards. Used by the #9448 guard to decide
// whether the EC shard set is complete enough that the orphaned regular
// replica is safe to delete.
func countExistingEcShardsForVolume(activeTopology *topology.ActiveTopology, volumeID uint32, collection string) int {
	if activeTopology == nil {
		return 0
	}
	topologyInfo := activeTopology.GetTopologyInfo()
	if topologyInfo == nil {
		return 0
	}
	var seen erasure_coding.ShardBits
	for _, dc := range topologyInfo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, node := range rack.DataNodeInfos {
				for _, diskInfo := range node.DiskInfos {
					for _, ecShardInfo := range diskInfo.EcShardInfos {
						if ecShardInfo.Id != volumeID || ecShardInfo.Collection != collection {
							continue
						}
						seen |= erasure_coding.ShardBits(ecShardInfo.EcIndexBits)
					}
				}
			}
		}
	}
	return seen.Count()
}
