package distribution

import (
	"fmt"
	"sort"
)

// ShardMove represents a planned shard move
type ShardMove struct {
	ShardID    int
	SourceNode *TopologyNode
	DestNode   *TopologyNode
	Reason     string
}

// RebalancePlan contains the complete plan for rebalancing EC shards
type RebalancePlan struct {
	Moves        []ShardMove
	Distribution *ECDistribution
	Analysis     *TopologyAnalysis

	// Statistics
	TotalMoves      int
	MovesAcrossDC   int
	MovesAcrossRack int
	MovesWithinRack int
}

// Rebalancer plans shard moves to achieve proportional distribution
type Rebalancer struct {
	ecConfig  ECConfig
	repConfig ReplicationConfig
}

// NewRebalancer creates a rebalancer for the given EC ratio and replication policy.
func NewRebalancer(ec ECConfig, rep ReplicationConfig) *Rebalancer {
	return &Rebalancer{ecConfig: ec, repConfig: rep}
}

// PlanRebalance generates shard moves that bring the volume's shard layout to
// the balanced distribution: first across data centers, then racks within each
// data center, then nodes within each rack. Moves stop when source and
// destination both reach their even share, or when destination nodes have no
// free slots.
func (r *Rebalancer) PlanRebalance(analysis *TopologyAnalysis) (*RebalancePlan, error) {
	if analysis == nil {
		return nil, fmt.Errorf("cannot plan rebalancing without a topology analysis")
	}
	plan := &RebalancePlan{
		Distribution: CalculateDistribution(r.ecConfig, r.repConfig),
		Analysis:     analysis,
	}

	recordMove := func(shardID int, src, dst *TopologyNode, level, domain string) {
		src.ShardIDs = removeShard(src.ShardIDs, shardID)
		dst.ShardIDs = append(dst.ShardIDs, shardID)
		src.FreeSlots++
		dst.FreeSlots--
		analysis.ShardsByDC[src.DataCenter]--
		analysis.ShardsByDC[dst.DataCenter]++
		analysis.ShardsByRack[rackKey(src.DataCenter, src.Rack)]--
		analysis.ShardsByRack[rackKey(dst.DataCenter, dst.Rack)]++
		analysis.ShardsByNode[src.NodeID]--
		analysis.ShardsByNode[dst.NodeID]++
		analysis.DCToShards[src.DataCenter] = removeShard(analysis.DCToShards[src.DataCenter], shardID)
		analysis.DCToShards[dst.DataCenter] = append(analysis.DCToShards[dst.DataCenter], shardID)
		analysis.RackToShards[rackKey(src.DataCenter, src.Rack)] = removeShard(analysis.RackToShards[rackKey(src.DataCenter, src.Rack)], shardID)
		analysis.RackToShards[rackKey(dst.DataCenter, dst.Rack)] = append(analysis.RackToShards[rackKey(dst.DataCenter, dst.Rack)], shardID)
		analysis.NodeToShards[src.NodeID] = removeShard(analysis.NodeToShards[src.NodeID], shardID)
		analysis.NodeToShards[dst.NodeID] = append(analysis.NodeToShards[dst.NodeID], shardID)
		plan.Moves = append(plan.Moves, ShardMove{
			ShardID:    shardID,
			SourceNode: src,
			DestNode:   dst,
			Reason:     fmt.Sprintf("%s balance: %s above target", level, domain),
		})
		switch {
		case src.DataCenter != dst.DataCenter:
			plan.MovesAcrossDC++
		case src.Rack != dst.Rack:
			plan.MovesAcrossRack++
		default:
			plan.MovesWithinRack++
		}
	}

	moveOut := func(level, srcDomain string, srcNodes, dstNodes []*TopologyNode) bool {
		var shardIDs []int
		for _, n := range srcNodes {
			shardIDs = append(shardIDs, n.ShardIDs...)
		}
		for _, shardID := range r.ecConfig.SortShardsParityFirst(shardIDs) {
			var src *TopologyNode
			for _, n := range srcNodes {
				if contains(n.ShardIDs, shardID) {
					src = n
					break
				}
			}
			if src == nil {
				continue
			}
			var dst *TopologyNode
			for _, n := range dstNodes {
				if n.FreeSlots > 0 && !contains(n.ShardIDs, shardID) && (dst == nil || nodeLess(n, dst)) {
					dst = n
				}
			}
			if dst == nil {
				continue
			}
			recordMove(shardID, src, dst, level, srcDomain)
			return true
		}
		return false
	}

	// balance moves shards between domains of one level until every domain
	// holds at most its even share or no destination can take a shard.
	balance := func(level string, domains []string, countOf func(string) int, target int, nodesOf func(string) []*TopologyNode, dstDomains func(string) []string) {
		for {
			src := ""
			srcFound := false
			for _, d := range domains {
				if countOf(d) > target && (!srcFound || countOf(d) > countOf(src)) {
					src, srcFound = d, true
				}
			}
			if !srcFound {
				return
			}
			dsts := append([]string(nil), dstDomains(src)...)
			sort.Slice(dsts, func(i, j int) bool {
				if countOf(dsts[i]) != countOf(dsts[j]) {
					return countOf(dsts[i]) < countOf(dsts[j])
				}
				return dsts[i] < dsts[j]
			})
			moved := false
			for _, dst := range dsts {
				if countOf(dst) < target && moveOut(level, src, nodesOf(src), nodesOf(dst)) {
					moved = true
					break
				}
			}
			if !moved {
				return
			}
		}
	}

	dcList := sortedKeys(analysis.DCToRacks)
	dcRackKeys := func(dc string) []string {
		racks := analysis.DCToRacks[dc]
		keys := make([]string, 0, len(racks))
		for _, rack := range racks {
			keys = append(keys, rackKey(dc, rack))
		}
		return keys
	}
	totalShards := 0
	for _, dc := range dcList {
		totalShards += shardCount(dcRackKeys(dc), analysis.RackToNodes)
	}
	dcCount := func(dc string) int { return shardCount(dcRackKeys(dc), analysis.RackToNodes) }
	balance("dc", dcList, dcCount, ceilDivide(totalShards, len(dcList)),
		func(dc string) []*TopologyNode { return nodesOfRacks(dcRackKeys(dc), analysis.RackToNodes) },
		func(string) []string { return dcList })

	for _, dc := range dcList {
		racks := analysis.DCToRacks[dc]
		rackCount := func(rack string) int { return shardCount([]string{rackKey(dc, rack)}, analysis.RackToNodes) }
		balance("rack", racks, rackCount, ceilDivide(dcCount(dc), len(racks)),
			func(rack string) []*TopologyNode { return analysis.RackToNodes[rackKey(dc, rack)] },
			func(string) []string { return racks })
	}

	for _, rack := range sortedKeys(analysis.RackToNodes) {
		nodes := analysis.RackToNodes[rack]
		nodeCount := func(nodeID string) int {
			for _, n := range nodes {
				if n.NodeID == nodeID {
					return len(n.ShardIDs)
				}
			}
			return 0
		}
		nodeIDs := make([]string, 0, len(nodes))
		for _, n := range nodes {
			nodeIDs = append(nodeIDs, n.NodeID)
		}
		sort.Strings(nodeIDs)
		balance("node", nodeIDs, nodeCount, ceilDivide(shardCount([]string{rack}, analysis.RackToNodes), len(nodes)),
			func(nodeID string) []*TopologyNode {
				for _, n := range nodes {
					if n.NodeID == nodeID {
						return []*TopologyNode{n}
					}
				}
				return nil
			},
			func(string) []string { return nodeIDs })
	}

	plan.TotalMoves = len(plan.Moves)
	return plan, nil
}

// nodeLess prefers the node holding fewer of the volume's shards, then the one
// with more free slots, for a stable destination choice.
func nodeLess(a, b *TopologyNode) bool {
	if len(a.ShardIDs) != len(b.ShardIDs) {
		return len(a.ShardIDs) < len(b.ShardIDs)
	}
	if a.FreeSlots != b.FreeSlots {
		return a.FreeSlots > b.FreeSlots
	}
	return a.NodeID < b.NodeID
}

// shardCount sums the shards the given racks currently hold for the volume.
func shardCount(racks []string, rackToNodes map[string][]*TopologyNode) int {
	total := 0
	for _, rack := range racks {
		for _, n := range rackToNodes[rack] {
			total += len(n.ShardIDs)
		}
	}
	return total
}

func nodesOfRacks(racks []string, rackToNodes map[string][]*TopologyNode) []*TopologyNode {
	var nodes []*TopologyNode
	for _, rack := range racks {
		nodes = append(nodes, rackToNodes[rack]...)
	}
	return nodes
}

func removeShard(shards []int, shardID int) []int {
	for i, s := range shards {
		if s == shardID {
			return append(shards[:i], shards[i+1:]...)
		}
	}
	return shards
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
