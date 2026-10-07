package erasure_coding

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding/ecbalancer"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"github.com/stretchr/testify/require"
)

// StrictPlacement refuses a volume whose configured caps cannot be met,
// rather than relaxing them: here one rack must hold all 14 shards under
// a 2-shards-per-rack replica placement.
func TestPlanECDestinationsStrictPlacement(t *testing.T) {
	activeTopology := buildActiveTopology(t, 7, []string{"hdd"}, 100, 0, "")
	metric := &types.VolumeHealthMetrics{
		VolumeID: 1,
		Server:   "10.0.0.1:8080",
		Size:     100 * 1024 * 1024,
	}
	rp, err := super_block.NewReplicaPlacementFromString("020")
	require.NoError(t, err)
	nodeAddresses := buildNodeAddressMap(activeTopology)

	snap := ecbalancer.FromActiveTopology(activeTopology, erasure_coding.DataShardsCount)
	cfg := NewDefaultConfig()
	plan, shardsPerPlan, err := planECDestinations(snap, nodeAddresses, metric, cfg, rp, erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount)
	require.NoError(t, err, "lenient placement relaxes the unsatisfiable rack cap")
	requireAllShardsPlaced(t, plan, shardsPerPlan)

	snap = ecbalancer.FromActiveTopology(activeTopology, erasure_coding.DataShardsCount)
	cfg.StrictPlacement = true
	_, _, err = planECDestinations(snap, nodeAddresses, metric, cfg, rp, erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount)
	require.Error(t, err, "strict placement must refuse rather than weaken the rack cap")
}

func TestStrictPlacementRoundTripsThroughTaskPolicy(t *testing.T) {
	cfg := NewDefaultConfig()
	cfg.StrictPlacement = true

	restored := NewDefaultConfig()
	require.NoError(t, restored.FromTaskPolicy(cfg.ToTaskPolicy()))
	require.True(t, restored.StrictPlacement, "strict placement must survive the persisted policy round trip")

	restored.StrictPlacement = false
	require.NoError(t, restored.FromTaskPolicy(restored.ToTaskPolicy()))
	require.False(t, restored.StrictPlacement)
}
