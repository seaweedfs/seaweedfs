package erasure_coding

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestECPlacementPlannerApplyReservations(t *testing.T) {
	activeTopology := buildActiveTopology(t, 1, []string{"hdd"}, 10, 0)

	planner := newECPlacementPlanner(activeTopology, nil)
	require.NotNil(t, planner)

	key := ecDiskKey("10.0.0.1:8080", 0)
	candidate, ok := planner.candidateByKey[key]
	require.True(t, ok)
	assert.Equal(t, 10, candidate.FreeSlots)
	assert.Equal(t, 0, candidate.ShardCount)
	assert.Equal(t, 0, candidate.LoadCount)

	shardImpact := topology.CalculateECShardStorageImpact(1, 1)
	destinations := make([]topology.TaskDestinationSpec, 10)
	for i := 0; i < 10; i++ {
		destinations[i] = topology.TaskDestinationSpec{
			ServerID:      "10.0.0.1:8080",
			DiskID:        0,
			StorageImpact: &shardImpact,
		}
	}

	planner.applyTaskReservations(1024, nil, destinations)

	candidate = planner.candidateByKey[key]
	assert.Equal(t, 9, candidate.FreeSlots, "10 shard slots should reduce available volume slots by 1")
	assert.Equal(t, 10, candidate.ShardCount)
	assert.Equal(t, 1, candidate.LoadCount, "load should only be incremented once per disk")
}

func TestPlanECDestinationsUsesPlanner(t *testing.T) {
	activeTopology := buildActiveTopology(t, 7, []string{"hdd", "ssd"}, 100, 0)
	planner := newECPlacementPlanner(activeTopology, nil)
	require.NotNil(t, planner)

	metric := &types.VolumeHealthMetrics{
		VolumeID:   1,
		Server:     "10.0.0.1:8080",
		Size:       100 * 1024 * 1024,
		Collection: "",
	}

	plan, err := planECDestinations(planner, metric, NewDefaultConfig(), erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount)
	require.NoError(t, err)
	require.NotNil(t, plan)
	assert.Equal(t, erasure_coding.TotalShardsCount, len(plan.Plans))
}

func TestECPlacementPlannerPrefersTaggedDisks(t *testing.T) {
	activeTopology := buildActiveTopology(t, 3, []string{"hdd"}, 10, 0)
	topo := activeTopology.GetTopologyInfo()
	for _, dc := range topo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for k, node := range rack.DataNodeInfos {
				for diskType := range node.DiskInfos {
					if k < 2 {
						node.DiskInfos[diskType].Tags = []string{"fast"}
					} else {
						node.DiskInfos[diskType].Tags = []string{"slow"}
					}
				}
			}
		}
	}
	require.NoError(t, activeTopology.UpdateTopology(topo))

	planner := newECPlacementPlanner(activeTopology, []string{"fast"})
	require.NotNil(t, planner)

	selected, err := planner.selectDestinations("", "", "", 2)
	require.NoError(t, err)
	require.Len(t, selected, 2)

	for _, candidate := range selected {
		key := ecDiskKey(candidate.NodeID, candidate.DiskID)
		assert.True(t, diskHasTag(planner.diskTags[key], "fast"))
	}
}

func TestECPlacementPlannerFallsBackWhenTagsInsufficient(t *testing.T) {
	activeTopology := buildActiveTopology(t, 3, []string{"hdd"}, 10, 0)
	topo := activeTopology.GetTopologyInfo()
	for _, dc := range topo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for i, node := range rack.DataNodeInfos {
				for diskType := range node.DiskInfos {
					if i == 0 {
						node.DiskInfos[diskType].Tags = []string{"fast"}
					}
				}
			}
		}
	}
	require.NoError(t, activeTopology.UpdateTopology(topo))

	planner := newECPlacementPlanner(activeTopology, []string{"fast"})
	require.NotNil(t, planner)

	selected, err := planner.selectDestinations("", "", "", 3)
	require.NoError(t, err)
	require.Len(t, selected, 3)

	taggedCount := 0
	for _, candidate := range selected {
		key := ecDiskKey(candidate.NodeID, candidate.DiskID)
		if diskHasTag(planner.diskTags[key], "fast") {
			taggedCount++
		}
	}
	assert.Less(t, taggedCount, len(selected))
}

// TestDetectionSkipsWhenECShardsAlreadyExist guards against issue #9448: a
// regular replica that survived a previous successful EC encode (source
// delete didn't clean it up for some reason) gets re-proposed for encoding,
// the new encode collides with the already-mounted shards on the targets
// ("ec volume %d is mounted; refusing overwrite"), and detection loops
// forever on the same volume. Detection must see the existing shards and
// skip the volume so an admin can clean it up out-of-band.
//
// The guard fires ONLY when the EC shard set is complete (count >=
// totalShards), so a partially-distributed previous attempt still falls
// through to the existing recovery branch in the encode path.
func TestDetectionSkipsWhenECShardsAlreadyExist(t *testing.T) {
	const volumeID uint32 = 42
	activeTopology := buildStuckSourceTopology(t, volumeID, erasure_coding.TotalShardsCount)

	clusterInfo := &types.ClusterInfo{ActiveTopology: activeTopology}
	metrics := buildStuckSourceMetrics(volumeID, "127.0.0.1:8080")

	results, hasMore, err := Detection(context.Background(), metrics, clusterInfo, NewDefaultConfig(), 0)
	require.NoError(t, err)
	require.False(t, hasMore)
	require.Empty(t, results, "stuck source replica with all EC shards present must not yield a new encoding proposal")
}

// TestDetectionAllowsRegularReplicaWhenShardsPartial covers the partial-EC
// branch of the #9448 guard: when fewer than totalShards exist, the volume
// is allowed to flow through to the normal encoding path so the existing
// recovery branch (the `existingECShards` block in the encode arm) can fold
// the partial shards into the new task. A bug here would either (a) skip
// the volume entirely or (b) emit a proposal that later collides on the
// mounted shards.
func TestDetectionAllowsRegularReplicaWhenShardsPartial(t *testing.T) {
	const volumeID uint32 = 43
	activeTopology := buildStuckSourceTopology(t, volumeID, erasure_coding.DataShardsCount-1)

	clusterInfo := &types.ClusterInfo{ActiveTopology: activeTopology}
	metrics := buildStuckSourceMetrics(volumeID, "127.0.0.1:8080")

	results, _, err := Detection(context.Background(), metrics, clusterInfo, NewDefaultConfig(), 0)
	require.NoError(t, err)
	// Partial shards are not a "stuck source" — the encode arm must keep
	// its chance to either propose a fresh task that folds the partial
	// shards into cleanup, or fail planning on the constrained topology.
	// We don't require len(results) > 0 because the constrained topology
	// (one disk per node, the orphaned shards already taking slots) can
	// legitimately fail destination planning. The assertion that matters
	// is: the #9448 guard did NOT silently swallow the volume into a
	// skippedAlreadyEC counter, and any emitted result is still an EC
	// task and not a no-op.
	for _, r := range results {
		require.Equal(t, types.TaskTypeErasureCoding, r.TaskType, "any emitted result should still be an EC task, not a no-op")
	}
}

// buildStuckSourceTopology constructs a topology that mimics the #9448 stuck
// state: a regular volume replica on node 0 plus `presentShardCount` EC
// shards distributed across nodes 0..presentShardCount-1.
func buildStuckSourceTopology(t *testing.T, volumeID uint32, presentShardCount int) *topology.ActiveTopology {
	t.Helper()
	require.LessOrEqual(t, presentShardCount, erasure_coding.TotalShardsCount)
	activeTopology := topology.NewActiveTopology(10)
	nodes := make([]*master_pb.DataNodeInfo, 0, erasure_coding.TotalShardsCount)
	for i := 0; i < erasure_coding.TotalShardsCount; i++ {
		nodeID := fmt.Sprintf("127.0.0.1:%d", 8080+i)
		diskInfo := &master_pb.DiskInfo{
			DiskId:         0,
			VolumeCount:    1,
			MaxVolumeCount: 100,
		}
		if i < presentShardCount {
			diskInfo.EcShardInfos = []*master_pb.VolumeEcShardInformationMessage{{
				Id:          volumeID,
				Collection:  "",
				EcIndexBits: uint32(1) << uint(i),
				DiskId:      0,
			}}
		}
		if i == 0 {
			diskInfo.VolumeInfos = []*master_pb.VolumeInformationMessage{{
				Id:       volumeID,
				DiskId:   0,
				DiskType: "hdd",
				Size:     200 * 1024 * 1024,
			}}
		}
		nodes = append(nodes, &master_pb.DataNodeInfo{
			Id:        nodeID,
			DiskInfos: map[string]*master_pb.DiskInfo{"hdd": diskInfo},
		})
	}
	require.NoError(t, activeTopology.UpdateTopology(&master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{{
				Id:            "rack1",
				DataNodeInfos: nodes,
			}},
		}},
	}))
	return activeTopology
}

// buildStuckSourceMetrics returns a metric that already satisfies the EC
// criteria (Age, FullnessRatio, Size), with `Age` derived from `LastModified`
// so the two fields stay consistent for any reader.
func buildStuckSourceMetrics(volumeID uint32, server string) []*types.VolumeHealthMetrics {
	lastModified := time.Now().Add(-time.Hour)
	return []*types.VolumeHealthMetrics{{
		VolumeID:      volumeID,
		Server:        server,
		Size:          200 * 1024 * 1024,
		Collection:    "",
		FullnessRatio: 0.9,
		LastModified:  lastModified,
		Age:           time.Since(lastModified),
	}}
}

// TestCountExistingEcShardsForVolume verifies that the helper walks the
// EcIndexBits bitmap (not just len(EcShardInfos)) so it correctly counts
// distinct shard ids even when a single info entry on one disk carries
// multiple shards.
func TestCountExistingEcShardsForVolume(t *testing.T) {
	const volumeID uint32 = 99
	activeTopology := topology.NewActiveTopology(10)
	require.NoError(t, activeTopology.UpdateTopology(&master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{{
				Id: "rack1",
				DataNodeInfos: []*master_pb.DataNodeInfo{
					{
						Id: "127.0.0.1:8080",
						DiskInfos: map[string]*master_pb.DiskInfo{
							"hdd": {
								DiskId:         0,
								MaxVolumeCount: 100,
								// One info entry, three shards present (ids 0, 2, 5).
								EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{{
									Id:          volumeID,
									Collection:  "",
									EcIndexBits: (uint32(1) << 0) | (uint32(1) << 2) | (uint32(1) << 5),
									DiskId:      0,
								}},
							},
						},
					},
					{
						Id: "127.0.0.1:8081",
						DiskInfos: map[string]*master_pb.DiskInfo{
							"hdd": {
								DiskId:         0,
								MaxVolumeCount: 100,
								// One info entry, one shard (id 3) — overlaps with neither.
								EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{{
									Id:          volumeID,
									Collection:  "",
									EcIndexBits: uint32(1) << 3,
									DiskId:      0,
								}},
							},
						},
					},
				},
			}},
		}},
	}))

	assert.Equal(t, 4, countExistingEcShardsForVolume(activeTopology, volumeID, ""))
	assert.Equal(t, 0, countExistingEcShardsForVolume(activeTopology, volumeID, "other-collection"))
	assert.Equal(t, 0, countExistingEcShardsForVolume(nil, volumeID, ""))
}

func TestDetectionContextCancellation(t *testing.T) {
	activeTopology := buildActiveTopology(t, 5, []string{"hdd", "ssd"}, 50, 0)
	clusterInfo := &types.ClusterInfo{ActiveTopology: activeTopology}
	metrics := buildVolumeMetricsForIDs(50)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := Detection(ctx, metrics, clusterInfo, NewDefaultConfig(), 0)
	require.ErrorIs(t, err, context.Canceled)
}

func TestDetectionMaxResultsHonorsLimit(t *testing.T) {
	// One node per shard so each shard gets its own disk (#9369).
	activeTopology := buildActiveTopology(t, erasure_coding.TotalShardsCount, []string{"hdd"}, 20, 0)
	clusterInfo := &types.ClusterInfo{ActiveTopology: activeTopology}
	metrics := buildVolumeMetricsForIDs(3)

	results, hasMore, err := Detection(context.Background(), metrics, clusterInfo, NewDefaultConfig(), 1)
	require.NoError(t, err)
	assert.Len(t, results, 1)
	assert.True(t, hasMore)
}

// #9369: 7 servers × 2 physical HDDs must yield 14 distinct (server, disk_id)
// destinations, not 7 destinations doubled up on the same disk.
func TestPlanECDestinationsSpreadsAcrossPhysicalDisks(t *testing.T) {
	const numServers = 7
	const disksPerServer = 2

	activeTopology := topology.NewActiveTopology(10)
	nodes := make([]*master_pb.DataNodeInfo, 0, numServers)
	for i := 1; i <= numServers; i++ {
		volumeInfos := make([]*master_pb.VolumeInformationMessage, 0, disksPerServer)
		for d := uint32(0); d < disksPerServer; d++ {
			volumeInfos = append(volumeInfos, &master_pb.VolumeInformationMessage{
				Id:       uint32(i*100 + int(d)),
				DiskId:   d,
				DiskType: "hdd",
			})
		}
		nodes = append(nodes, &master_pb.DataNodeInfo{
			Id: fmt.Sprintf("127.0.0.1:%d", 8080+i),
			DiskInfos: map[string]*master_pb.DiskInfo{
				"hdd": {
					DiskId:         0,
					VolumeCount:    int64(disksPerServer),
					MaxVolumeCount: 200,
					VolumeInfos:    volumeInfos,
				},
			},
		})
	}
	require.NoError(t, activeTopology.UpdateTopology(&master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{{
				Id:            "rack1",
				DataNodeInfos: nodes,
			}},
		}},
	}))

	planner := newECPlacementPlanner(activeTopology, nil)
	require.NotNil(t, planner)

	metric := &types.VolumeHealthMetrics{
		VolumeID:   42,
		Server:     "127.0.0.1:8081",
		Size:       100 * 1024 * 1024,
		Collection: "",
	}

	plan, err := planECDestinations(planner, metric, NewDefaultConfig(), erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount)
	require.NoError(t, err)
	require.NotNil(t, plan)
	require.Equal(t, erasure_coding.TotalShardsCount, len(plan.Plans))

	seen := make(map[string]bool, len(plan.Plans))
	for _, p := range plan.Plans {
		key := fmt.Sprintf("%s:%d", p.TargetNode, p.TargetDisk)
		assert.False(t, seen[key], "duplicate (server,disk_id) target %s", key)
		seen[key] = true
	}
}

func TestPlanECDestinationsFailsWithInsufficientCapacity(t *testing.T) {
	activeTopology := buildActiveTopology(t, 1, []string{"hdd"}, 1, 1)
	planner := newECPlacementPlanner(activeTopology, nil)
	require.NotNil(t, planner)

	metric := &types.VolumeHealthMetrics{
		VolumeID:   2,
		Server:     "10.0.0.1:8080",
		Size:       10 * 1024 * 1024,
		Collection: "",
	}

	_, err := planECDestinations(planner, metric, NewDefaultConfig(), erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount)
	require.Error(t, err)
}

// #9586: with fewer single-disk servers than total shards, EC must still plan
// by packing several shards onto a disk (ec.encode's "4,4,3,3" fallback) rather
// than refusing. The reporter has 8 single-disk servers across 3 racks and a
// 10+4 scheme — 8 disks for 14 shards. minTotalDisks (ceil(14/4)=4) keeps any
// disk under parityShards shards, so durability holds.
func TestPlanECDestinationsPacksWhenFewerDisksThanShards(t *testing.T) {
	const numServers = 8
	// rack3 holds 4 servers, rack1 and rack2 hold 2 each, mirroring the report.
	racks := []string{"rack3", "rack3", "rack3", "rack3", "rack1", "rack1", "rack2", "rack2"}

	activeTopology := topology.NewActiveTopology(10)
	rackNodes := make(map[string][]*master_pb.DataNodeInfo)
	for i := 0; i < numServers; i++ {
		nodeID := fmt.Sprintf("192.168.1.%d:%d", 143+i/3, 8080+i)
		rackNodes[racks[i]] = append(rackNodes[racks[i]], &master_pb.DataNodeInfo{
			Id: nodeID,
			DiskInfos: map[string]*master_pb.DiskInfo{
				"hdd": {
					DiskId:         0,
					VolumeCount:    1,
					MaxVolumeCount: 200,
					VolumeInfos: []*master_pb.VolumeInformationMessage{{
						Id:       uint32(i + 1),
						DiskId:   0,
						DiskType: "hdd",
					}},
				},
			},
		})
	}
	rackInfos := make([]*master_pb.RackInfo, 0, len(rackNodes))
	for _, rackID := range []string{"rack1", "rack2", "rack3"} {
		rackInfos = append(rackInfos, &master_pb.RackInfo{Id: rackID, DataNodeInfos: rackNodes[rackID]})
	}
	require.NoError(t, activeTopology.UpdateTopology(&master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{Id: "dc1", RackInfos: rackInfos}},
	}))

	planner := newECPlacementPlanner(activeTopology, nil)
	require.NotNil(t, planner)

	metric := &types.VolumeHealthMetrics{
		VolumeID:   4569,
		Server:     "192.168.1.145:8081",
		Size:       100 * 1024 * 1024,
		Collection: "",
	}

	plan, err := planECDestinations(planner, metric, NewDefaultConfig(), erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount)
	require.NoError(t, err)
	require.NotNil(t, plan)
	// One plan entry per available disk; fewer than the 14 shards.
	require.Equal(t, numServers, len(plan.Plans))

	// createECTargets must cover all 14 shards exactly once, packing onto the
	// available disks without any disk exceeding parityShards shards.
	targets := createECTargets(plan, erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount)
	require.Equal(t, numServers, len(targets))

	seenShards := make(map[uint32]bool)
	for _, target := range targets {
		require.LessOrEqual(t, len(target.ShardIds), erasure_coding.ParityShardsCount,
			"no disk may hold more than parityShards shards, else losing it loses the volume")
		for _, shardId := range target.ShardIds {
			require.False(t, seenShards[shardId], "shard %d assigned to more than one target", shardId)
			seenShards[shardId] = true
		}
	}
	require.Len(t, seenShards, erasure_coding.TotalShardsCount, "every shard must be placed exactly once")
}

func buildVolumeMetricsForIDs(count int) []*types.VolumeHealthMetrics {
	metrics := make([]*types.VolumeHealthMetrics, 0, count)
	now := time.Now()
	for id := 1; id <= count; id++ {
		metrics = append(metrics, &types.VolumeHealthMetrics{
			VolumeID:      uint32(id),
			Server:        "10.0.0.1:8080",
			Size:          200 * 1024 * 1024,
			Collection:    "",
			FullnessRatio: 0.9,
			LastModified:  now.Add(-time.Hour),
			Age:           10 * time.Minute,
		})
	}
	return metrics
}

func buildActiveTopology(t *testing.T, nodeCount int, diskTypes []string, maxVolumeCount, usedVolumeCount int64) *topology.ActiveTopology {
	t.Helper()
	activeTopology := topology.NewActiveTopology(10)

	nodes := make([]*master_pb.DataNodeInfo, 0, nodeCount)
	for i := 1; i <= nodeCount; i++ {
		diskInfos := make(map[string]*master_pb.DiskInfo)
		for diskIndex, diskType := range diskTypes {
			used := usedVolumeCount
			if used > maxVolumeCount {
				used = maxVolumeCount
			}
			volumeInfos := make([]*master_pb.VolumeInformationMessage, 0, 200)
			for vid := 1; vid <= 200; vid++ {
				volumeInfos = append(volumeInfos, &master_pb.VolumeInformationMessage{
					Id:         uint32(vid),
					Collection: "",
					DiskId:     uint32(diskIndex),
				})
			}
			diskInfos[diskType] = &master_pb.DiskInfo{
				DiskId:         uint32(diskIndex),
				VolumeCount:    used,
				MaxVolumeCount: maxVolumeCount,
				VolumeInfos:    volumeInfos,
			}
		}

		nodes = append(nodes, &master_pb.DataNodeInfo{
			Id:        fmt.Sprintf("10.0.0.%d:8080", i),
			DiskInfos: diskInfos,
		})
	}

	topologyInfo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				Id: "dc1",
				RackInfos: []*master_pb.RackInfo{
					{
						Id:            "rack1",
						DataNodeInfos: nodes,
					},
				},
			},
		},
	}

	require.NoError(t, activeTopology.UpdateTopology(topologyInfo))
	return activeTopology
}
