package shell

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding/distribution"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// ECDistribution is an alias to the distribution package type for backward compatibility
type ECDistribution = distribution.ECDistribution

// CalculateECDistribution computes the target EC shard distribution based on replication policy.
// This is a convenience wrapper that uses the default 10+4 EC configuration.
// For custom EC ratios, use the distribution package directly.
func CalculateECDistribution(totalShards, parityShards int, rp *super_block.ReplicaPlacement) *ECDistribution {
	ec := distribution.ECConfig{
		DataShards:   totalShards - parityShards,
		ParityShards: parityShards,
	}
	rep := distribution.NewReplicationConfig(rp)
	return distribution.CalculateDistribution(ec, rep)
}

// TopologyDistributionAnalysis holds the current shard distribution analysis
// This wraps the distribution package's TopologyAnalysis with shell-specific EcNode handling
type TopologyDistributionAnalysis struct {
	inner *distribution.TopologyAnalysis

	// Shell-specific mappings
	nodeMap map[string]*EcNode // nodeID -> EcNode
}

// NewTopologyDistributionAnalysis creates a new analysis structure
func NewTopologyDistributionAnalysis() *TopologyDistributionAnalysis {
	return &TopologyDistributionAnalysis{
		inner:   distribution.NewTopologyAnalysis(),
		nodeMap: make(map[string]*EcNode),
	}
}

// AddNode adds a node and its shards to the analysis
func (a *TopologyDistributionAnalysis) AddNode(node *EcNode, shardsInfo *erasure_coding.ShardsInfo) {
	nodeId := node.info.Id

	// Create distribution.TopologyNode from EcNode
	topoNode := &distribution.TopologyNode{
		NodeID:      nodeId,
		DataCenter:  string(node.dc),
		Rack:        string(node.rack),
		FreeSlots:   node.freeEcSlot,
		TotalShards: shardsInfo.Count(),
		ShardIDs:    shardsInfo.IdsInt(),
	}

	a.inner.AddNode(topoNode)
	a.nodeMap[nodeId] = node

	// Add shard locations
	for _, shardId := range shardsInfo.Ids() {
		a.inner.AddShardLocation(distribution.ShardLocation{
			ShardID:    int(shardId),
			NodeID:     nodeId,
			DataCenter: string(node.dc),
			Rack:       string(node.rack),
		})
	}
}

// Finalize completes the analysis
func (a *TopologyDistributionAnalysis) Finalize() {
	a.inner.Finalize()
}

// String returns a summary
func (a *TopologyDistributionAnalysis) String() string {
	return a.inner.String()
}

// DetailedString returns detailed analysis
func (a *TopologyDistributionAnalysis) DetailedString() string {
	return a.inner.DetailedString()
}

// GetShardsByDC returns shard counts by DC
func (a *TopologyDistributionAnalysis) GetShardsByDC() map[DataCenterId]int {
	result := make(map[DataCenterId]int)
	for dc, count := range a.inner.ShardsByDC {
		result[DataCenterId(dc)] = count
	}
	return result
}

// GetShardsByRack returns shard counts by rack
func (a *TopologyDistributionAnalysis) GetShardsByRack() map[RackId]int {
	result := make(map[RackId]int)
	for rack, count := range a.inner.ShardsByRack {
		result[RackId(rack)] = count
	}
	return result
}

// GetShardsByNode returns shard counts by node
func (a *TopologyDistributionAnalysis) GetShardsByNode() map[EcNodeId]int {
	result := make(map[EcNodeId]int)
	for nodeId, count := range a.inner.ShardsByNode {
		result[EcNodeId(nodeId)] = count
	}
	return result
}

// AnalyzeVolumeDistribution creates an analysis of current shard distribution for a volume
func AnalyzeVolumeDistribution(volumeId needle.VolumeId, locations []*EcNode, diskType types.DiskType) *TopologyDistributionAnalysis {
	analysis := NewTopologyDistributionAnalysis()

	for _, node := range locations {
		si := findEcVolumeShardsInfo(node, volumeId, diskType)
		if si.Count() > 0 {
			analysis.AddNode(node, si)
		}
	}

	analysis.Finalize()
	return analysis
}

// ECShardMove represents a planned shard move (shell-specific with EcNode references)
type ECShardMove struct {
	VolumeId   needle.VolumeId
	ShardId    erasure_coding.ShardId
	SourceNode *EcNode
	DestNode   *EcNode
	Reason     string
}

// String returns a human-readable description
func (m ECShardMove) String() string {
	return fmt.Sprintf("volume %d shard %d: %s -> %s (%s)",
		m.VolumeId, m.ShardId, m.SourceNode.info.Id, m.DestNode.info.Id, m.Reason)
}

// ProportionalECRebalancer implements proportional shard distribution for shell commands
type ProportionalECRebalancer struct {
	ecNodes          []*EcNode
	replicaPlacement *super_block.ReplicaPlacement
	diskType         types.DiskType
	ecConfig         distribution.ECConfig
}

// NewProportionalECRebalancer creates a new proportional rebalancer with default EC config
func NewProportionalECRebalancer(
	ecNodes []*EcNode,
	rp *super_block.ReplicaPlacement,
	diskType types.DiskType,
) *ProportionalECRebalancer {
	return NewProportionalECRebalancerWithConfig(
		ecNodes,
		rp,
		diskType,
		distribution.DefaultECConfig(),
	)
}

// NewProportionalECRebalancerWithConfig creates a rebalancer with custom EC configuration
func NewProportionalECRebalancerWithConfig(
	ecNodes []*EcNode,
	rp *super_block.ReplicaPlacement,
	diskType types.DiskType,
	ecConfig distribution.ECConfig,
) *ProportionalECRebalancer {
	return &ProportionalECRebalancer{
		ecNodes:          ecNodes,
		replicaPlacement: rp,
		diskType:         diskType,
		ecConfig:         ecConfig,
	}
}

// PlanMoves generates a plan for moving shards to achieve proportional distribution
func (r *ProportionalECRebalancer) PlanMoves(
	volumeId needle.VolumeId,
	locations []*EcNode,
) ([]ECShardMove, error) {
	// Build topology analysis
	analysis := distribution.NewTopologyAnalysis()
	nodeMap := make(map[string]*EcNode)

	// Add all EC nodes to the analysis (even those without shards)
	for _, node := range r.ecNodes {
		nodeId := node.info.Id
		topoNode := &distribution.TopologyNode{
			NodeID:     nodeId,
			DataCenter: string(node.dc),
			Rack:       string(node.rack),
			FreeSlots:  node.freeEcSlot,
		}
		analysis.AddNode(topoNode)
		nodeMap[nodeId] = node
	}

	// Add shard locations from nodes that have shards
	for _, node := range locations {
		nodeId := node.info.Id
		si := findEcVolumeShardsInfo(node, volumeId, r.diskType)
		for _, shardId := range si.Ids() {
			analysis.AddShardLocation(distribution.ShardLocation{
				ShardID:    int(shardId),
				NodeID:     nodeId,
				DataCenter: string(node.dc),
				Rack:       string(node.rack),
			})
		}
		if _, exists := nodeMap[nodeId]; !exists {
			nodeMap[nodeId] = node
		}
	}

	analysis.Finalize()

	// Create rebalancer and plan moves
	rep := distribution.NewReplicationConfig(r.replicaPlacement)
	rebalancer := distribution.NewRebalancer(r.ecConfig, rep)

	plan, err := rebalancer.PlanRebalance(analysis)
	if err != nil {
		return nil, err
	}

	// Convert distribution moves to shell moves
	var moves []ECShardMove
	for _, move := range plan.Moves {
		srcNode := nodeMap[move.SourceNode.NodeID]
		destNode := nodeMap[move.DestNode.NodeID]
		if srcNode == nil || destNode == nil {
			continue
		}

		moves = append(moves, ECShardMove{
			VolumeId:   volumeId,
			ShardId:    erasure_coding.ShardId(move.ShardID),
			SourceNode: srcNode,
			DestNode:   destNode,
			Reason:     move.Reason,
		})
	}

	return moves, nil
}

// GetDistributionSummary returns a summary of the planned distribution
func GetDistributionSummary(rp *super_block.ReplicaPlacement) string {
	ec := distribution.DefaultECConfig()
	rep := distribution.NewReplicationConfig(rp)
	dist := distribution.CalculateDistribution(ec, rep)
	return dist.Summary()
}

// GetDistributionSummaryWithConfig returns a summary with custom EC configuration
func GetDistributionSummaryWithConfig(rp *super_block.ReplicaPlacement, ecConfig distribution.ECConfig) string {
	rep := distribution.NewReplicationConfig(rp)
	dist := distribution.CalculateDistribution(ecConfig, rep)
	return dist.Summary()
}

// GetFaultToleranceAnalysis returns fault tolerance analysis for the given configuration
func GetFaultToleranceAnalysis(rp *super_block.ReplicaPlacement) string {
	ec := distribution.DefaultECConfig()
	rep := distribution.NewReplicationConfig(rp)
	dist := distribution.CalculateDistribution(ec, rep)
	return dist.FaultToleranceAnalysis()
}

// GetFaultToleranceAnalysisWithConfig returns fault tolerance analysis with custom EC configuration
func GetFaultToleranceAnalysisWithConfig(rp *super_block.ReplicaPlacement, ecConfig distribution.ECConfig) string {
	rep := distribution.NewReplicationConfig(rp)
	dist := distribution.CalculateDistribution(ecConfig, rep)
	return dist.FaultToleranceAnalysis()
}
