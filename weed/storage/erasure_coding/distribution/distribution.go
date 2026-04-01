package distribution

import (
	"fmt"
)

// ECDistribution represents the target distribution of EC shards
// based on EC configuration and replication policy.
type ECDistribution struct {
	// EC configuration
	ECConfig ECConfig

	// Replication configuration
	ReplicationConfig ReplicationConfig

	// Target shard counts per topology level (balanced distribution)
	TargetShardsPerDC   int
	TargetShardsPerRack int
	TargetShardsPerNode int

	// Maximum shard counts per topology level (fault tolerance limits)
	// These prevent any single failure domain from having too many shards
	MaxShardsPerDC   int
	MaxShardsPerRack int
	MaxShardsPerNode int
}

// CalculateDistribution computes the target EC shard distribution based on
// EC configuration and replication policy.
//
// The algorithm:
// 1. Uses replication policy to determine minimum topology spread
// 2. Calculates target shards per level (evenly distributed)
// 3. Calculates max shards per level (for fault tolerance)
func CalculateDistribution(ec ECConfig, rep ReplicationConfig) *ECDistribution {
	totalShards := ec.TotalShards()

	// Target distribution (balanced, rounded up to ensure all shards placed)
	targetShardsPerDC := ceilDivide(totalShards, rep.MinDataCenters)
	targetShardsPerRack := ceilDivide(targetShardsPerDC, rep.MinRacksPerDC)
	targetShardsPerNode := ceilDivide(targetShardsPerRack, rep.MinNodesPerRack)

	// Maximum limits for fault tolerance
	// The key constraint: losing one failure domain shouldn't lose more than parityShards
	// So max shards per domain = totalShards - parityShards + tolerance
	// We add small tolerance (+2) to allow for imbalanced topologies
	faultToleranceLimit := totalShards - ec.ParityShards + 1

	maxShardsPerDC := min(faultToleranceLimit, targetShardsPerDC+2)
	maxShardsPerRack := min(faultToleranceLimit, targetShardsPerRack+2)
	maxShardsPerNode := min(faultToleranceLimit, targetShardsPerNode+2)

	return &ECDistribution{
		ECConfig:            ec,
		ReplicationConfig:   rep,
		TargetShardsPerDC:   targetShardsPerDC,
		TargetShardsPerRack: targetShardsPerRack,
		TargetShardsPerNode: targetShardsPerNode,
		MaxShardsPerDC:      maxShardsPerDC,
		MaxShardsPerRack:    maxShardsPerRack,
		MaxShardsPerNode:    maxShardsPerNode,
	}
}

// String returns a human-readable description of the distribution
func (d *ECDistribution) String() string {
	return fmt.Sprintf(
		"ECDistribution{EC:%s, DCs:%d (target:%d/max:%d), Racks/DC:%d (target:%d/max:%d), Nodes/Rack:%d (target:%d/max:%d)}",
		d.ECConfig.String(),
		d.ReplicationConfig.MinDataCenters, d.TargetShardsPerDC, d.MaxShardsPerDC,
		d.ReplicationConfig.MinRacksPerDC, d.TargetShardsPerRack, d.MaxShardsPerRack,
		d.ReplicationConfig.MinNodesPerRack, d.TargetShardsPerNode, d.MaxShardsPerNode,
	)
}

// Summary returns a multi-line summary of the distribution plan
func (d *ECDistribution) Summary() string {
	summary := fmt.Sprintf("EC Configuration: %s\n", d.ECConfig.String())
	summary += fmt.Sprintf("Replication: %s\n", d.ReplicationConfig.String())
	summary += fmt.Sprintf("Distribution Plan:\n")
	summary += fmt.Sprintf("  Data Centers: %d (target %d shards each, max %d)\n",
		d.ReplicationConfig.MinDataCenters, d.TargetShardsPerDC, d.MaxShardsPerDC)
	summary += fmt.Sprintf("  Racks per DC: %d (target %d shards each, max %d)\n",
		d.ReplicationConfig.MinRacksPerDC, d.TargetShardsPerRack, d.MaxShardsPerRack)
	summary += fmt.Sprintf("  Nodes per Rack: %d (target %d shards each, max %d)\n",
		d.ReplicationConfig.MinNodesPerRack, d.TargetShardsPerNode, d.MaxShardsPerNode)
	return summary
}

// CanSurviveDCFailure returns true if the distribution can survive
// complete loss of one data center
func (d *ECDistribution) CanSurviveDCFailure() bool {
	// After losing one DC with max shards, check if remaining shards are enough
	remainingAfterDCLoss := d.ECConfig.TotalShards() - d.TargetShardsPerDC
	return remainingAfterDCLoss >= d.ECConfig.MinShardsForReconstruction()
}

// CanSurviveRackFailure returns true if the distribution can survive
// complete loss of one rack
func (d *ECDistribution) CanSurviveRackFailure() bool {
	remainingAfterRackLoss := d.ECConfig.TotalShards() - d.TargetShardsPerRack
	return remainingAfterRackLoss >= d.ECConfig.MinShardsForReconstruction()
}

// MinDCsForDCFaultTolerance calculates the minimum number of DCs needed
// to survive complete DC failure with this EC configuration
func (d *ECDistribution) MinDCsForDCFaultTolerance() int {
	// To survive DC failure, max shards per DC = parityShards
	maxShardsPerDC := d.ECConfig.MaxTolerableLoss()
	if maxShardsPerDC == 0 {
		return d.ECConfig.TotalShards() // Would need one DC per shard
	}
	return ceilDivide(d.ECConfig.TotalShards(), maxShardsPerDC)
}

// FaultToleranceAnalysis returns a detailed analysis of fault tolerance
func (d *ECDistribution) FaultToleranceAnalysis() string {
	analysis := fmt.Sprintf("Fault Tolerance Analysis for %s:\n", d.ECConfig.String())

	// DC failure
	dcSurvive := d.CanSurviveDCFailure()
	shardsAfterDC := d.ECConfig.TotalShards() - d.TargetShardsPerDC
	analysis += fmt.Sprintf("  DC Failure: %s\n", boolToResult(dcSurvive))
	analysis += fmt.Sprintf("    - Losing one DC loses ~%d shards\n", d.TargetShardsPerDC)
	analysis += fmt.Sprintf("    - Remaining: %d shards (need %d)\n", shardsAfterDC, d.ECConfig.DataShards)
	if !dcSurvive {
		analysis += fmt.Sprintf("    - Need at least %d DCs for DC fault tolerance\n", d.MinDCsForDCFaultTolerance())
	}

	// Rack failure
	rackSurvive := d.CanSurviveRackFailure()
	shardsAfterRack := d.ECConfig.TotalShards() - d.TargetShardsPerRack
	analysis += fmt.Sprintf("  Rack Failure: %s\n", boolToResult(rackSurvive))
	analysis += fmt.Sprintf("    - Losing one rack loses ~%d shards\n", d.TargetShardsPerRack)
	analysis += fmt.Sprintf("    - Remaining: %d shards (need %d)\n", shardsAfterRack, d.ECConfig.DataShards)

	// Node failure (usually survivable)
	shardsAfterNode := d.ECConfig.TotalShards() - d.TargetShardsPerNode
	nodeSurvive := shardsAfterNode >= d.ECConfig.DataShards
	analysis += fmt.Sprintf("  Node Failure: %s\n", boolToResult(nodeSurvive))
	analysis += fmt.Sprintf("    - Losing one node loses ~%d shards\n", d.TargetShardsPerNode)
	analysis += fmt.Sprintf("    - Remaining: %d shards (need %d)\n", shardsAfterNode, d.ECConfig.DataShards)

	return analysis
}

func boolToResult(b bool) string {
	if b {
		return "SURVIVABLE ✓"
	}
	return "NOT SURVIVABLE ✗"
}

// ceilDivide performs ceiling division
func ceilDivide(a, b int) int {
	if b <= 0 {
		return a
	}
	return (a + b - 1) / b
}
