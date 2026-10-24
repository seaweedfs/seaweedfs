package distribution

import (
	"fmt"
	"strings"
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

// CalculateDistribution computes the balanced shard counts each topology level
// should hold for the given EC ratio spread over the minimum topology the
// replication policy describes.
func CalculateDistribution(ec ECConfig, rep ReplicationConfig) *ECDistribution {
	total := ec.TotalShards()
	rep.MinDataCenters = max(rep.MinDataCenters, 1)
	rep.MinRacksPerDC = max(rep.MinRacksPerDC, 1)
	rep.MinNodesPerRack = max(rep.MinNodesPerRack, 1)
	dcUnits := rep.MinDataCenters
	rackUnits := dcUnits * rep.MinRacksPerDC
	nodeUnits := rackUnits * rep.MinNodesPerRack
	return &ECDistribution{
		ECConfig:            ec,
		ReplicationConfig:   rep,
		TargetShardsPerDC:   evenShare(total, dcUnits),
		TargetShardsPerRack: evenShare(total, rackUnits),
		TargetShardsPerNode: evenShare(total, nodeUnits),
		MaxShardsPerDC:      maxShare(total, dcUnits, ec.CanLoseShards()),
		MaxShardsPerRack:    maxShare(total, rackUnits, ec.CanLoseShards()),
		MaxShardsPerNode:    maxShare(total, nodeUnits, ec.CanLoseShards()),
	}
}

// evenShare is the balanced shard count per unit at one topology level.
func evenShare(total, units int) int {
	return ceilDivide(total, units)
}

// maxShare bounds how many shards a single failure domain may hold: the even
// share plus the slack that would pad the total to the next multiple of the
// unit count. When the even share is survivable on its own, the loss budget
// caps the bound so the fault tolerance report stays true even at the max.
func maxShare(total, units, lossBudget int) int {
	if units < 1 {
		units = 1
	}
	m := ceilDivide(total, units) + units - total%units
	if m > total {
		m = total
	}
	if evenShare(total, units) <= lossBudget && m > lossBudget {
		m = lossBudget
	}
	return m
}

func ceilDivide(a, b int) int {
	if b < 1 {
		return a
	}
	return (a + b - 1) / b
}

// Summary describes the distribution plan in human-readable form.
func (d *ECDistribution) Summary() string {
	var sb strings.Builder
	ec := d.ECConfig
	rep := d.ReplicationConfig
	fmt.Fprintf(&sb, "EC Configuration: %d+%d (total: %d, can lose: %d)\n",
		ec.DataShards, ec.ParityShards, ec.TotalShards(), ec.CanLoseShards())
	fmt.Fprintf(&sb, "Replication: replication=%s (DCs:%d, Racks/DC:%d, Nodes/Rack:%d)\n",
		rep.Original, rep.MinDataCenters, rep.MinRacksPerDC, rep.MinNodesPerRack)
	sb.WriteString("Distribution Plan:\n")
	fmt.Fprintf(&sb, "  Data Centers: %d (target %d shards each, max %d)\n",
		rep.MinDataCenters, d.TargetShardsPerDC, d.MaxShardsPerDC)
	fmt.Fprintf(&sb, "  Racks per DC: %d (target %d shards each, max %d)\n",
		rep.MinRacksPerDC, d.TargetShardsPerRack, d.MaxShardsPerRack)
	fmt.Fprintf(&sb, "  Nodes per Rack: %d (target %d shards each, max %d)\n",
		rep.MinNodesPerRack, d.TargetShardsPerNode, d.MaxShardsPerNode)
	return sb.String()
}

// FaultToleranceAnalysis reports whether losing one unit at each topology
// level still leaves enough shards to reconstruct the volume.
func (d *ECDistribution) FaultToleranceAnalysis() string {
	var sb strings.Builder
	ec := d.ECConfig
	fmt.Fprintf(&sb, "Fault Tolerance Analysis for %d+%d:\n", ec.DataShards, ec.ParityShards)
	writeLevel := func(name string, target int) {
		remaining := ec.TotalShards() - target
		status := "SURVIVABLE ✓"
		if remaining < ec.DataShards {
			status = "NOT SURVIVABLE ✗"
		}
		fmt.Fprintf(&sb, "  %s Failure: %s\n", name, status)
		fmt.Fprintf(&sb, "    - Losing one %s loses ~%d shards\n", name, target)
		fmt.Fprintf(&sb, "    - Remaining: %d shards (need %d)\n", remaining, ec.DataShards)
	}
	writeLevel("DC", d.TargetShardsPerDC)
	writeLevel("Rack", d.TargetShardsPerRack)
	writeLevel("Node", d.TargetShardsPerNode)
	return sb.String()
}
