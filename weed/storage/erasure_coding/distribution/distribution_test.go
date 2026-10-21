package distribution

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultECConfig(t *testing.T) {
	ec := DefaultECConfig()
	assert.Equal(t, 10, ec.DataShards)
	assert.Equal(t, 4, ec.ParityShards)
	assert.Equal(t, 14, ec.TotalShards())
	assert.Equal(t, 4, ec.CanLoseShards())
	assert.True(t, ec.IsDataShard(0))
	assert.True(t, ec.IsDataShard(9))
	assert.False(t, ec.IsDataShard(10))
	assert.True(t, ec.IsParityShard(13))
	assert.False(t, ec.IsParityShard(14))
	assert.Equal(t, []int{0, 1, 10, 11}, ec.SortShardsDataFirst([]int{11, 1, 10, 0}))
	assert.Equal(t, []int{13, 12, 2, 1}, ec.SortShardsParityFirst([]int{2, 13, 1, 12}))
}

func TestNewECConfig(t *testing.T) {
	ec, err := NewECConfig(8, 4)
	require.NoError(t, err)
	assert.Equal(t, 12, ec.TotalShards())

	for _, bad := range [][2]int{{0, 4}, {8, 0}, {-1, 4}} {
		_, err := NewECConfig(bad[0], bad[1])
		assert.Error(t, err, "config %v must be rejected", bad)
	}
}

func TestNewReplicationConfigFromString(t *testing.T) {
	rep, err := NewReplicationConfigFromString("110")
	require.NoError(t, err)
	assert.Equal(t, 2, rep.MinDataCenters)
	assert.Equal(t, 2, rep.MinRacksPerDC)
	assert.Equal(t, 1, rep.MinNodesPerRack)
	assert.Equal(t, "110", rep.Original)

	rep, err = NewReplicationConfigFromString("000")
	require.NoError(t, err)
	assert.Equal(t, 1, rep.MinDataCenters)
	assert.Equal(t, 1, rep.MinRacksPerDC)
	assert.Equal(t, 1, rep.MinNodesPerRack)

	_, err = NewReplicationConfigFromString("abc")
	assert.Error(t, err)
}

func TestCalculateDistribution(t *testing.T) {
	rep, err := NewReplicationConfigFromString("110")
	require.NoError(t, err)
	dist := CalculateDistribution(DefaultECConfig(), rep)

	assert.Equal(t, 7, dist.TargetShardsPerDC)
	assert.Equal(t, 4, dist.TargetShardsPerRack)
	assert.Equal(t, 4, dist.TargetShardsPerNode)
	assert.Equal(t, 9, dist.MaxShardsPerDC)
	assert.Equal(t, 6, dist.MaxShardsPerRack)
	assert.Equal(t, 6, dist.MaxShardsPerNode)

	expected := "EC Configuration: 10+4 (total: 14, can lose: 4)\n" +
		"Replication: replication=110 (DCs:2, Racks/DC:2, Nodes/Rack:1)\n" +
		"Distribution Plan:\n" +
		"  Data Centers: 2 (target 7 shards each, max 9)\n" +
		"  Racks per DC: 2 (target 4 shards each, max 6)\n" +
		"  Nodes per Rack: 1 (target 4 shards each, max 6)\n"
	assert.Equal(t, expected, dist.Summary())
}

func TestCalculateDistributionSingleDomain(t *testing.T) {
	rep, err := NewReplicationConfigFromString("000")
	require.NoError(t, err)
	dist := CalculateDistribution(DefaultECConfig(), rep)
	assert.Equal(t, 14, dist.TargetShardsPerDC)
	assert.Equal(t, 14, dist.MaxShardsPerDC)
}

func TestFaultToleranceAnalysis(t *testing.T) {
	ec, err := NewECConfig(8, 4)
	require.NoError(t, err)
	rep, err := NewReplicationConfigFromString("200")
	require.NoError(t, err)
	dist := CalculateDistribution(ec, rep)

	analysis := dist.FaultToleranceAnalysis()
	assert.Contains(t, analysis, "Fault Tolerance Analysis for 8+4:")
	assert.Contains(t, analysis, "DC Failure: SURVIVABLE")
	assert.Contains(t, analysis, "- Losing one DC loses ~4 shards")
	assert.Contains(t, analysis, "- Remaining: 8 shards (need 8)")
}

func TestFaultToleranceAnalysisNotSurvivable(t *testing.T) {
	rep, err := NewReplicationConfigFromString("000")
	require.NoError(t, err)
	dist := CalculateDistribution(DefaultECConfig(), rep)
	analysis := dist.FaultToleranceAnalysis()
	assert.Contains(t, analysis, "DC Failure: NOT SURVIVABLE")
	assert.Contains(t, analysis, "Rack Failure: NOT SURVIVABLE")
	assert.Contains(t, analysis, "Node Failure: NOT SURVIVABLE")
}

func makeNode(id, dc, rack string, freeSlots int, shards ...int) *TopologyNode {
	return &TopologyNode{NodeID: id, DataCenter: dc, Rack: rack, FreeSlots: freeSlots, ShardIDs: shards}
}

func TestTopologyAnalysis(t *testing.T) {
	a := NewTopologyAnalysis()
	a.AddNode(makeNode("n1", "dc1", "r1", 5, 0, 1))
	a.AddNode(makeNode("n2", "dc1", "r1", 5, 2))
	a.AddNode(makeNode("n3", "dc2", "r2", 5))
	a.Finalize()

	assert.Equal(t, 3, a.TotalShards)
	assert.Equal(t, 3, a.TotalNodes)
	assert.Equal(t, 2, a.TotalRacks)
	assert.Equal(t, 2, a.TotalDCs)
	assert.Equal(t, 3, a.ShardsByDC["dc1"])
	assert.Equal(t, 0, a.ShardsByDC["dc2"])
	assert.Equal(t, []string{"r1"}, a.DCToRacks["dc1"])
	assert.Equal(t, []int{0, 1, 2}, a.RackToShards[rackKey("dc1", "r1")])
}

// Shards may be registered either through TopologyNode.ShardIDs or through
// AddShardLocation — both must leave the node holding the shards.
func TestTopologyAnalysisAddShardLocation(t *testing.T) {
	a := NewTopologyAnalysis()
	a.AddNode(makeNode("n1", "dc1", "r1", 5))
	a.AddShardLocation(ShardLocation{ShardID: 0, NodeID: "n1", DataCenter: "dc1", Rack: "r1"})
	a.AddShardLocation(ShardLocation{ShardID: 1, NodeID: "n1", DataCenter: "dc1", Rack: "r1"})
	a.AddShardLocation(ShardLocation{ShardID: 1, NodeID: "n1", DataCenter: "dc1", Rack: "r1"})
	a.Finalize()

	assert.Equal(t, 2, a.TotalShards)
	assert.Equal(t, 2, a.ShardsByNode["n1"])
	assert.Equal(t, []int{0, 1}, a.AllNodes["n1"].ShardIDs)
}

// Rack names are only unique within a DC, so dc1/rack1 and dc2/rack1 are
// distinct domains.
func TestTopologyAnalysisSameRackNameAcrossDCs(t *testing.T) {
	a := NewTopologyAnalysis()
	a.AddNode(makeNode("n1", "dc1", "rack1", 5, 0, 1))
	a.AddNode(makeNode("n2", "dc2", "rack1", 5, 2))
	a.Finalize()

	assert.Equal(t, 2, a.TotalDCs)
	assert.Equal(t, 2, a.TotalRacks)
	assert.Equal(t, 2, a.ShardsByRack[rackKey("dc1", "rack1")])
	assert.Equal(t, 1, a.ShardsByRack[rackKey("dc2", "rack1")])
}

// A "/" inside a data-center or rack name must not merge distinct racks.
func TestTopologyAnalysisSlashInNames(t *testing.T) {
	a := NewTopologyAnalysis()
	a.AddNode(makeNode("n1", "a", "b/c", 5, 0))
	a.AddNode(makeNode("n2", "a/b", "c", 5, 1))
	a.Finalize()

	assert.Equal(t, 2, a.TotalRacks)
	assert.Equal(t, 1, a.ShardsByRack[rackKey("a", "b/c")])
	assert.Equal(t, 1, a.ShardsByRack[rackKey("a/b", "c")])
}

func TestPlanRebalanceAcrossDCs(t *testing.T) {
	a := NewTopologyAnalysis()
	a.AddNode(makeNode("a1", "dcA", "rA1", 10, 0, 1, 2, 3))
	a.AddNode(makeNode("a2", "dcA", "rA2", 10, 4, 5, 6, 7))
	a.AddNode(makeNode("a3", "dcA", "rA3", 10, 8, 9, 10, 11))
	a.AddNode(makeNode("a4", "dcA", "rA4", 10, 12, 13))
	a.AddNode(makeNode("b1", "dcB", "rB1", 10))
	a.AddNode(makeNode("b2", "dcB", "rB2", 10))
	a.Finalize()

	rep, err := NewReplicationConfigFromString("110")
	require.NoError(t, err)
	plan, err := NewRebalancer(DefaultECConfig(), rep).PlanRebalance(a)
	require.NoError(t, err)

	assert.Equal(t, 7, plan.MovesAcrossDC)
	assert.Equal(t, 7, plan.Analysis.ShardsByDC["dcA"])
	assert.Equal(t, 7, plan.Analysis.ShardsByDC["dcB"])

	// the given analysis still describes the un-moved layout
	assert.Equal(t, 14, a.ShardsByDC["dcA"])
	assert.Equal(t, 0, a.ShardsByDC["dcB"])
	assert.Equal(t, []int{0, 1, 2, 3}, a.AllNodes["a1"].ShardIDs)

	// parity shards (10-13) move before data shards
	assert.True(t, plan.Moves[0].ShardID >= 10)
	assert.Equal(t, "b1", plan.Moves[0].DestNode.NodeID)
}

func TestPlanRebalanceAcrossRacks(t *testing.T) {
	a := NewTopologyAnalysis()
	a.AddNode(makeNode("n1", "dc1", "r1", 10, 0, 1, 2, 3))
	a.AddNode(makeNode("n2", "dc1", "r2", 10))
	a.Finalize()

	rep, err := NewReplicationConfigFromString("110")
	require.NoError(t, err)
	plan, err := NewRebalancer(DefaultECConfig(), rep).PlanRebalance(a)
	require.NoError(t, err)

	assert.Equal(t, 2, plan.TotalMoves)
	assert.Equal(t, 2, plan.MovesAcrossRack)
	assert.Equal(t, 2, plan.Analysis.ShardsByRack[rackKey("dc1", "r1")])
	assert.Equal(t, 2, plan.Analysis.ShardsByRack[rackKey("dc1", "r2")])
	// parity-first ordering: shards 3 then 2 move out of r1
	assert.Equal(t, 3, plan.Moves[0].ShardID)
	assert.Equal(t, 2, plan.Moves[1].ShardID)
}

func TestPlanRebalanceWithinRack(t *testing.T) {
	a := NewTopologyAnalysis()
	a.AddNode(makeNode("n1", "dc1", "r1", 10, 0, 1, 2, 3))
	a.AddNode(makeNode("n2", "dc1", "r1", 10))
	a.Finalize()

	rep, err := NewReplicationConfigFromString("001")
	require.NoError(t, err)
	plan, err := NewRebalancer(DefaultECConfig(), rep).PlanRebalance(a)
	require.NoError(t, err)

	assert.Equal(t, 2, plan.TotalMoves)
	assert.Equal(t, 2, plan.MovesWithinRack)
	assert.Equal(t, 2, plan.Analysis.ShardsByNode["n1"])
	assert.Equal(t, 2, plan.Analysis.ShardsByNode["n2"])
	assert.Equal(t, []int{3, 2}, plan.Analysis.AllNodes["n2"].ShardIDs)
	assert.Equal(t, []int{0, 1, 2, 3}, a.AllNodes["n1"].ShardIDs)
}

func TestPlanRebalanceRespectsFreeSlots(t *testing.T) {
	a := NewTopologyAnalysis()
	a.AddNode(makeNode("n1", "dc1", "r1", 10, 0, 1, 2, 3))
	a.AddNode(makeNode("n2", "dc1", "r1", 1))
	a.Finalize()

	plan, err := NewRebalancer(DefaultECConfig(), ReplicationConfig{MinDataCenters: 1, MinRacksPerDC: 1, MinNodesPerRack: 2}).PlanRebalance(a)
	require.NoError(t, err)

	assert.Equal(t, 1, plan.TotalMoves, "only one slot is free on the destination")
	assert.Equal(t, 3, plan.Analysis.ShardsByNode["n1"])
	assert.Equal(t, 1, plan.Analysis.ShardsByNode["n2"])
	assert.Equal(t, 0, plan.Analysis.AllNodes["n2"].FreeSlots)
	assert.Equal(t, 1, a.AllNodes["n2"].FreeSlots)
}

func TestPlanRebalanceNoopWhenBalanced(t *testing.T) {
	a := NewTopologyAnalysis()
	a.AddNode(makeNode("n1", "dc1", "r1", 10, 0, 1))
	a.AddNode(makeNode("n2", "dc1", "r1", 10, 2, 3))
	a.Finalize()

	plan, err := NewRebalancer(DefaultECConfig(), ReplicationConfig{MinDataCenters: 1, MinRacksPerDC: 1, MinNodesPerRack: 2}).PlanRebalance(a)
	require.NoError(t, err)
	assert.Equal(t, 0, plan.TotalMoves)
}

func TestPlanRebalanceNilAnalysis(t *testing.T) {
	_, err := NewRebalancer(DefaultECConfig(), ReplicationConfig{}).PlanRebalance(nil)
	assert.Error(t, err)
}

// Node balancing inside a rack must aim for the rack's count after
// DC- and rack-level moves, not the count the rack started with.
func TestPlanRebalanceNodeTargetAfterMoves(t *testing.T) {
	a := NewTopologyAnalysis()
	a.AddNode(makeNode("n1", "dc1", "r1", 10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13))
	a.AddNode(makeNode("n2", "dc1", "r1", 10))
	a.AddNode(makeNode("n3", "dc1", "r2", 10))
	a.Finalize()

	rep, err := NewReplicationConfigFromString("010")
	require.NoError(t, err)
	plan, err := NewRebalancer(DefaultECConfig(), rep).PlanRebalance(a)
	require.NoError(t, err)

	assert.Equal(t, 7, plan.Analysis.ShardsByRack[rackKey("dc1", "r1")])
	assert.Equal(t, 7, plan.Analysis.ShardsByRack[rackKey("dc1", "r2")])
	assert.Equal(t, 4, plan.Analysis.ShardsByNode["n1"])
	assert.Equal(t, 3, plan.Analysis.ShardsByNode["n2"])
	assert.Equal(t, []int{0, 1, 2, 3}, plan.Analysis.AllNodes["n1"].ShardIDs)
	assert.Equal(t, []int{6, 5, 4}, plan.Analysis.AllNodes["n2"].ShardIDs)
}

func TestPlanRebalanceNoDuplicateShardOnNode(t *testing.T) {
	a := NewTopologyAnalysis()
	a.AddNode(makeNode("n1", "dc1", "r1", 10, 0, 1, 2, 3))
	a.AddNode(makeNode("n2", "dc1", "r1", 10, 3))
	a.Finalize()

	plan, err := NewRebalancer(DefaultECConfig(), ReplicationConfig{MinDataCenters: 1, MinRacksPerDC: 1, MinNodesPerRack: 2}).PlanRebalance(a)
	require.NoError(t, err)

	// n2 already holds 3, so the balancing move brings a different shard
	assert.Equal(t, 3, plan.Analysis.ShardsByNode["n1"])
	assert.Equal(t, 2, plan.Analysis.ShardsByNode["n2"])
	assert.Equal(t, []int{3, 2}, plan.Analysis.AllNodes["n2"].ShardIDs)
	for _, move := range plan.Moves {
		assert.NotContains(t, move.DestNode.ShardIDs[:len(move.DestNode.ShardIDs)-1], move.ShardID,
			"shard %d must not be moved onto a node already holding it", move.ShardID)
	}
}

func TestCalculateDistributionZeroReplication(t *testing.T) {
	dist := CalculateDistribution(DefaultECConfig(), ReplicationConfig{})
	assert.Equal(t, 14, dist.TargetShardsPerDC)
	assert.NotPanics(t, func() { dist.Summary(); dist.FaultToleranceAnalysis() })
}
