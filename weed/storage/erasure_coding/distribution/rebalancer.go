package distribution

import (
	"fmt"
	"slices"
)

// ShardMove represents a planned shard move
type ShardMove struct {
	ShardID    int
	SourceNode *TopologyNode
	DestNode   *TopologyNode
	Reason     string
}

// String returns a human-readable description of the move
func (m ShardMove) String() string {
	return fmt.Sprintf("shard %d: %s -> %s (%s)",
		m.ShardID, m.SourceNode.NodeID, m.DestNode.NodeID, m.Reason)
}

// RebalancePlan contains the complete plan for rebalancing EC shards
type RebalancePlan struct {
	Moves        []ShardMove
	Distribution *ECDistribution
	Analysis     *TopologyAnalysis

	// Statistics
	TotalMoves     int
	MovesAcrossDC  int
	MovesAcrossRack int
	MovesWithinRack int
}

// String returns a summary of the plan
func (p *RebalancePlan) String() string {
	return fmt.Sprintf("RebalancePlan{moves:%d, acrossDC:%d, acrossRack:%d, withinRack:%d}",
		p.TotalMoves, p.MovesAcrossDC, p.MovesAcrossRack, p.MovesWithinRack)
}

// DetailedString returns a detailed multi-line summary
func (p *RebalancePlan) DetailedString() string {
	s := fmt.Sprintf("Rebalance Plan:\n")
	s += fmt.Sprintf("  Total Moves: %d\n", p.TotalMoves)
	s += fmt.Sprintf("  Across DC: %d\n", p.MovesAcrossDC)
	s += fmt.Sprintf("  Across Rack: %d\n", p.MovesAcrossRack)
	s += fmt.Sprintf("  Within Rack: %d\n", p.MovesWithinRack)
	s += fmt.Sprintf("\nMoves:\n")
	for i, move := range p.Moves {
		s += fmt.Sprintf("  %d. %s\n", i+1, move.String())
	}
	return s
}

// Rebalancer plans shard moves to achieve proportional distribution
type Rebalancer struct {
	ecConfig   ECConfig
	repConfig  ReplicationConfig
}

// NewRebalancer creates a new rebalancer with the given configuration
func NewRebalancer(ec ECConfig, rep ReplicationConfig) *Rebalancer {
	return &Rebalancer{
		ecConfig:  ec,
		repConfig: rep,
	}
}

// PlanRebalance creates a rebalancing plan based on current topology analysis
func (r *Rebalancer) PlanRebalance(analysis *TopologyAnalysis) (*RebalancePlan, error) {
	dist := CalculateDistribution(r.ecConfig, r.repConfig)

	plan := &RebalancePlan{
		Distribution: dist,
		Analysis:     analysis,
	}

	// Step 1: Balance across data centers
	dcMoves := r.planDCMoves(analysis, dist)
	for _, move := range dcMoves {
		plan.Moves = append(plan.Moves, move)
		plan.MovesAcrossDC++
	}

	// Update analysis after DC moves (for planning purposes)
	r.applyMovesToAnalysis(analysis, dcMoves)

	// Step 2: Balance across racks within each DC
	rackMoves := r.planRackMoves(analysis, dist)
	for _, move := range rackMoves {
		plan.Moves = append(plan.Moves, move)
		plan.MovesAcrossRack++
	}

	// Update analysis after rack moves
	r.applyMovesToAnalysis(analysis, rackMoves)

	// Step 3: Balance across nodes within each rack
	nodeMoves := r.planNodeMoves(analysis, dist)
	for _, move := range nodeMoves {
		plan.Moves = append(plan.Moves, move)
		plan.MovesWithinRack++
	}

	plan.TotalMoves = len(plan.Moves)

	return plan, nil
}

// planDCMoves plans moves to balance shards across data centers
func (r *Rebalancer) planDCMoves(analysis *TopologyAnalysis, dist *ECDistribution) []ShardMove {
	var moves []ShardMove

	overDCs := CalculateDCExcess(analysis, dist)
	underDCs := CalculateUnderservedDCs(analysis, dist)

	underIdx := 0
	for _, over := range overDCs {
		for over.Excess > 0 && underIdx < len(underDCs) {
			destDC := underDCs[underIdx]

			// Find a shard and source node
			shardID, srcNode := r.pickShardToMove(analysis, over.Nodes)
			if srcNode == nil {
				break
			}

			// Find destination node in target DC
			destNode := r.pickBestDestination(analysis, destDC, "", dist)
			if destNode == nil {
				underIdx++
				continue
			}

			moves = append(moves, ShardMove{
				ShardID:    shardID,
				SourceNode: srcNode,
				DestNode:   destNode,
				Reason:     fmt.Sprintf("balance DC: %s -> %s", srcNode.DataCenter, destDC),
			})

			over.Excess--
			analysis.ShardsByDC[srcNode.DataCenter]--
			analysis.ShardsByDC[destDC]++

			// Check if destDC reached target
			if analysis.ShardsByDC[destDC] >= dist.TargetShardsPerDC {
				underIdx++
			}
		}
	}

	return moves
}

// planRackMoves plans moves to balance shards across racks within each DC
func (r *Rebalancer) planRackMoves(analysis *TopologyAnalysis, dist *ECDistribution) []ShardMove {
	var moves []ShardMove

	for dc := range analysis.DCToRacks {
		dcShards := analysis.ShardsByDC[dc]
		numRacks := len(analysis.DCToRacks[dc])
		if numRacks == 0 {
			continue
		}

		targetPerRack := ceilDivide(dcShards, max(numRacks, dist.ReplicationConfig.MinRacksPerDC))

		overRacks := CalculateRackExcess(analysis, dc, targetPerRack)
		underRacks := CalculateUnderservedRacks(analysis, dc, targetPerRack)

		underIdx := 0
		for _, over := range overRacks {
			for over.Excess > 0 && underIdx < len(underRacks) {
				destRack := underRacks[underIdx]

				// Find shard and source node
				shardID, srcNode := r.pickShardToMove(analysis, over.Nodes)
				if srcNode == nil {
					break
				}

				// Find destination node in target rack
				destNode := r.pickBestDestination(analysis, dc, destRack, dist)
				if destNode == nil {
					underIdx++
					continue
				}

				moves = append(moves, ShardMove{
					ShardID:    shardID,
					SourceNode: srcNode,
					DestNode:   destNode,
					Reason:     fmt.Sprintf("balance rack: %s -> %s", srcNode.Rack, destRack),
				})

				over.Excess--
				analysis.ShardsByRack[srcNode.Rack]--
				analysis.ShardsByRack[destRack]++

				if analysis.ShardsByRack[destRack] >= targetPerRack {
					underIdx++
				}
			}
		}
	}

	return moves
}

// planNodeMoves plans moves to balance shards across nodes within each rack
func (r *Rebalancer) planNodeMoves(analysis *TopologyAnalysis, dist *ECDistribution) []ShardMove {
	var moves []ShardMove

	for rack, nodes := range analysis.RackToNodes {
		if len(nodes) <= 1 {
			continue
		}

		rackShards := analysis.ShardsByRack[rack]
		targetPerNode := ceilDivide(rackShards, max(len(nodes), dist.ReplicationConfig.MinNodesPerRack))

		// Find over and under nodes
		var overNodes []*TopologyNode
		var underNodes []*TopologyNode

		for _, node := range nodes {
			count := analysis.ShardsByNode[node.NodeID]
			if count > targetPerNode {
				overNodes = append(overNodes, node)
			} else if count < targetPerNode {
				underNodes = append(underNodes, node)
			}
		}

		// Sort by excess/deficit
		slices.SortFunc(overNodes, func(a, b *TopologyNode) int {
			return analysis.ShardsByNode[b.NodeID] - analysis.ShardsByNode[a.NodeID]
		})

		underIdx := 0
		for _, srcNode := range overNodes {
			excess := analysis.ShardsByNode[srcNode.NodeID] - targetPerNode

			for excess > 0 && underIdx < len(underNodes) {
				destNode := underNodes[underIdx]

				// Pick a shard from this node, preferring parity shards
				shards := analysis.NodeToShards[srcNode.NodeID]
				if len(shards) == 0 {
					break
				}

				// Find a parity shard first, fallback to data shard
				shardID := -1
				shardIdx := -1
				for i, s := range shards {
					if r.ecConfig.IsParityShard(s) {
						shardID = s
						shardIdx = i
						break
					}
				}
				if shardID == -1 {
					shardID = shards[0]
					shardIdx = 0
				}

				moves = append(moves, ShardMove{
					ShardID:    shardID,
					SourceNode: srcNode,
					DestNode:   destNode,
					Reason:     fmt.Sprintf("balance node: %s -> %s", srcNode.NodeID, destNode.NodeID),
				})

				excess--
				analysis.ShardsByNode[srcNode.NodeID]--
				analysis.ShardsByNode[destNode.NodeID]++

				// Update shard lists - remove the specific shard we picked
				analysis.NodeToShards[srcNode.NodeID] = append(
					shards[:shardIdx], shards[shardIdx+1:]...)
				analysis.NodeToShards[destNode.NodeID] = append(
					analysis.NodeToShards[destNode.NodeID], shardID)

				if analysis.ShardsByNode[destNode.NodeID] >= targetPerNode {
					underIdx++
				}
			}
		}
	}

	return moves
}

// pickShardToMove selects a shard and its node from the given nodes.
// It prefers to move parity shards first, keeping data shards spread out
// since data shards serve read requests while parity shards are only for reconstruction.
func (r *Rebalancer) pickShardToMove(analysis *TopologyAnalysis, nodes []*TopologyNode) (int, *TopologyNode) {
	// Sort by shard count (most shards first)
	slices.SortFunc(nodes, func(a, b *TopologyNode) int {
		return analysis.ShardsByNode[b.NodeID] - analysis.ShardsByNode[a.NodeID]
	})

	// First pass: try to find a parity shard to move (prefer moving parity)
	for _, node := range nodes {
		shards := analysis.NodeToShards[node.NodeID]
		for _, shardID := range shards {
			if r.ecConfig.IsParityShard(shardID) {
				return shardID, node
			}
		}
	}

	// Second pass: if no parity shards, move a data shard
	for _, node := range nodes {
		shards := analysis.NodeToShards[node.NodeID]
		if len(shards) > 0 {
			return shards[0], node
		}
	}

	return -1, nil
}

// pickBestDestination selects the best destination node
func (r *Rebalancer) pickBestDestination(analysis *TopologyAnalysis, targetDC, targetRack string, dist *ECDistribution) *TopologyNode {
	var candidates []*TopologyNode

	// Collect candidates
	for _, node := range analysis.AllNodes {
		// Filter by DC if specified
		if targetDC != "" && node.DataCenter != targetDC {
			continue
		}
		// Filter by rack if specified
		if targetRack != "" && node.Rack != targetRack {
			continue
		}
		// Check capacity
		if node.FreeSlots <= 0 {
			continue
		}
		// Check max shards limit
		if analysis.ShardsByNode[node.NodeID] >= dist.MaxShardsPerNode {
			continue
		}

		candidates = append(candidates, node)
	}

	if len(candidates) == 0 {
		return nil
	}

	// Sort by: 1) fewer shards, 2) more free slots
	slices.SortFunc(candidates, func(a, b *TopologyNode) int {
		aShards := analysis.ShardsByNode[a.NodeID]
		bShards := analysis.ShardsByNode[b.NodeID]
		if aShards != bShards {
			return aShards - bShards
		}
		return b.FreeSlots - a.FreeSlots
	})

	return candidates[0]
}

// applyMovesToAnalysis updates the analysis with the planned moves
func (r *Rebalancer) applyMovesToAnalysis(analysis *TopologyAnalysis, moves []ShardMove) {
	for _, move := range moves {
		// Update rack counts
		analysis.ShardsByRack[move.SourceNode.Rack]--
		analysis.ShardsByRack[move.DestNode.Rack]++

		// Update node counts
		analysis.ShardsByNode[move.SourceNode.NodeID]--
		analysis.ShardsByNode[move.DestNode.NodeID]++

		// Update shard lists (remove from source, add to dest)
		srcShards := analysis.NodeToShards[move.SourceNode.NodeID]
		for i, s := range srcShards {
			if s == move.ShardID {
				analysis.NodeToShards[move.SourceNode.NodeID] = append(srcShards[:i], srcShards[i+1:]...)
				break
			}
		}
		analysis.NodeToShards[move.DestNode.NodeID] = append(
			analysis.NodeToShards[move.DestNode.NodeID], move.ShardID)
	}
}

