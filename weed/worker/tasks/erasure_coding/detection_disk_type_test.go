package erasure_coding

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPlanECDestinationsPrefersSourceDiskType_FullCluster covers the
// #9423 happy path through the worker plumbing: every node carries both
// an HDD and an SSD disk, so the cluster has enough SSD slots to fully
// satisfy a 10+4 placement. With metric.DiskType="ssd", every selected
// destination must land on an SSD disk.
func TestPlanECDestinationsPrefersSourceDiskType_FullCluster(t *testing.T) {
	// 14 nodes × 2 disks per node (hdd + ssd) — enough SSD slots alone
	// for a 10+4 layout with one-shard-per-(server,disk) diversity.
	activeTopology := buildActiveTopology(t, erasure_coding.TotalShardsCount, []string{"hdd", "ssd"}, 100, 0)

	planner := newECPlacementPlanner(activeTopology, nil)
	require.NotNil(t, planner)

	metric := &types.VolumeHealthMetrics{
		VolumeID:   1,
		Server:     "10.0.0.1:8080",
		Size:       100 * 1024 * 1024,
		Collection: "",
		DiskType:   "ssd", // the property being plumbed end-to-end
	}

	plan, err := planECDestinations(planner, metric, NewDefaultConfig(), erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount)
	require.NoError(t, err)
	require.Len(t, plan.Plans, erasure_coding.TotalShardsCount)

	// buildActiveTopology assigns DiskID by index into the diskTypes
	// slice, so SSD is DiskID 1. Every plan entry must point at an SSD
	// disk — anything else means metric.DiskType was dropped on the way
	// to the placement layer.
	for _, p := range plan.Plans {
		assert.Equalf(t, uint32(1), p.TargetDisk,
			"target %s diskID = %d, want SSD disk (DiskID=1)", p.TargetNode, p.TargetDisk)
	}
}

// TestPlanECDestinationsSpillsToOtherDiskType_WhenPreferredScarce pins
// the prefer-with-spillover policy: only one node in the cluster has an
// SSD disk; the rest are HDD-only. A 10+4 placement must still succeed
// (no hard fail), filling the one SSD slot first and the remaining 13
// from HDD nodes. Without the spillover behavior this would either
// error out or starve the placement.
func TestPlanECDestinationsSpillsToOtherDiskType_WhenPreferredScarce(t *testing.T) {
	// Start with every node carrying both HDD and SSD, then strip SSD
	// from all but the first node so the SSD pool is too small alone.
	activeTopology := buildActiveTopology(t, erasure_coding.TotalShardsCount, []string{"hdd", "ssd"}, 100, 0)
	topo := activeTopology.GetTopologyInfo()
	for _, dc := range topo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for i, node := range rack.DataNodeInfos {
				if i == 0 {
					continue // keep the first node's SSD
				}
				delete(node.DiskInfos, "ssd")
			}
		}
	}
	require.NoError(t, activeTopology.UpdateTopology(topo))

	planner := newECPlacementPlanner(activeTopology, nil)
	require.NotNil(t, planner)

	metric := &types.VolumeHealthMetrics{
		VolumeID:   2,
		Server:     "10.0.0.1:8080",
		Size:       100 * 1024 * 1024,
		Collection: "",
		DiskType:   "ssd",
	}

	plan, err := planECDestinations(planner, metric, NewDefaultConfig(), erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount)
	require.NoError(t, err)
	require.Len(t, plan.Plans, erasure_coding.TotalShardsCount)

	// Exactly one SSD target (the only SSD node), the rest on HDD.
	ssdHits := 0
	for _, p := range plan.Plans {
		if p.TargetDisk == 1 {
			ssdHits++
		}
	}
	assert.Equalf(t, 1, ssdHits,
		"expected exactly 1 SSD placement (the only SSD node in the cluster), got %d", ssdHits)
}
