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
	Analysis     *TopologyAnalysis // simulated layout once every move applies; move endpoints reference its nodes

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
// free slots. The moves are planned on a copy of the topology, so the given
// analysis still describes the cluster as it is; plan.Analysis shows the
// layout the moves would produce.
func (r *Rebalancer) PlanRebalance(analysis *TopologyAnalysis) (*RebalancePlan, error) {
	if analysis == nil {
		return nil, fmt.Errorf("cannot plan rebalancing without a topology analysis")
	}
	sim := NewTopologyAnalysis()
	for _, id := range sortedKeys(analysis.AllNodes) {
		node := *analysis.AllNodes[id]
		node.ShardIDs = append([]int(nil), node.ShardIDs...)
		sim.AddNode(&node)
	}
	sim.Finalize()
	plan := &RebalancePlan{
		Distribution: CalculateDistribution(r.ecConfig, r.repConfig),
		Analysis:     sim,
	}

	recordMove := func(shardID int, src, dst *TopologyNode, level, domain string) {
		src.ShardIDs = removeShard(src.ShardIDs, shardID)
		dst.ShardIDs = append(dst.ShardIDs, shardID)
		src.FreeSlots++
		dst.FreeSlots--
		sim.ShardsByDC[src.DataCenter]--
		sim.ShardsByDC[dst.DataCenter]++
		sim.ShardsByRack[rackKey(src.DataCenter, src.Rack)]--
		sim.ShardsByRack[rackKey(dst.DataCenter, dst.Rack)]++
		sim.ShardsByNode[src.NodeID]--
		sim.ShardsByNode[dst.NodeID]++
		sim.DCToShards[src.DataCenter] = removeShard(sim.DCToShards[src.DataCenter], shardID)
		sim.DCToShards[dst.DataCenter] = append(sim.DCToShards[dst.DataCenter], shardID)
		sim.RackToShards[rackKey(src.DataCenter, src.Rack)] = removeShard(sim.RackToShards[rackKey(src.DataCenter, src.Rack)], shardID)
		sim.RackToShards[rackKey(dst.DataCenter, dst.Rack)] = append(sim.RackToShards[rackKey(dst.DataCenter, dst.Rack)], shardID)
		sim.NodeToShards[src.NodeID] = removeShard(sim.NodeToShards[src.NodeID], shardID)
		sim.NodeToShards[dst.NodeID] = append(sim.NodeToShards[dst.NodeID], shardID)
		plan.Moves = append(plan.Moves, ShardMove{
			ShardID:    shardID,
			SourceNode: src,
			DestNode:   dst,
			Reason:     fmt.Sprintf("%s balance: %s above target", level, domain),
		})
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
			// a shard already inside the destination domain gains nothing there
			duplicate := false
			for _, n := range dstNodes {
				if contains(n.ShardIDs, shardID) {
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
			var dst *TopologyNode
			for _, n := range dstNodes {
				if n.FreeSlots > 0 && (dst == nil || nodeLess(n, dst)) {
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
	// holds at most its even share or no destination can take a shard. A
	// source that cannot shed any shard is set aside rather than stopping
	// the level, so other overloaded sources still get to move.
	balance := func(level string, domains []string, countOf func(string) int, target int, nodesOf func(string) []*TopologyNode, dstDomains func(string) []string) {
		blocked := make(map[string]bool)
		for {
			src := ""
			srcFound := false
			for _, d := range domains {
				if blocked[d] {
					continue
				}
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
				blocked[src] = true
			}
		}
	}

	dcList := sortedKeys(sim.DCToRacks)
	dcRackKeys := func(dc string) []string {
		racks := sim.DCToRacks[dc]
		keys := make([]string, 0, len(racks))
		for _, rack := range racks {
			keys = append(keys, rackKey(dc, rack))
		}
		return keys
	}
	totalShards := 0
	for _, dc := range dcList {
		totalShards += shardCount(dcRackKeys(dc), sim.RackToNodes)
	}
	dcCount := func(dc string) int { return shardCount(dcRackKeys(dc), sim.RackToNodes) }
	balance("dc", dcList, dcCount, ceilDivide(totalShards, len(dcList)),
		func(dc string) []*TopologyNode { return nodesOfRacks(dcRackKeys(dc), sim.RackToNodes) },
		func(string) []string { return dcList })

	for _, dc := range dcList {
		racks := sim.DCToRacks[dc]
		rackCount := func(rack string) int { return shardCount([]string{rackKey(dc, rack)}, sim.RackToNodes) }
		balance("rack", racks, rackCount, ceilDivide(dcCount(dc), len(racks)),
			func(rack string) []*TopologyNode { return sim.RackToNodes[rackKey(dc, rack)] },
			func(string) []string { return racks })
	}

	for _, rack := range sortedKeys(sim.RackToNodes) {
		nodes := sim.RackToNodes[rack]
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
		balance("node", nodeIDs, nodeCount, ceilDivide(shardCount([]string{rack}, sim.RackToNodes), len(nodes)),
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

	// A copy of a shard moves at most once per level, and each finer level only
	// moves it deeper inside the domain the previous level sent it to, so its
	// moves form a chain from the first source to the final destination.
	// Collapse each chain into one transfer — intermediate hops would copy the
	// shard over the network only to move it again. Chains are keyed by where
	// the copy sits, so duplicate copies of a shard still move independently.
	pending := make(map[ShardLocation]int)
	moves := plan.Moves[:0]
	for _, m := range plan.Moves {
		key := ShardLocation{ShardID: m.ShardID, NodeID: m.SourceNode.NodeID}
		if i, ok := pending[key]; ok {
			delete(pending, key)
			moves[i].DestNode = m.DestNode
			pending[ShardLocation{ShardID: m.ShardID, NodeID: m.DestNode.NodeID}] = i
			continue
		}
		pending[ShardLocation{ShardID: m.ShardID, NodeID: m.DestNode.NodeID}] = len(moves)
		moves = append(moves, m)
	}
	// Order the collapsed moves so each destination has a free slot when its
	// move is applied: a chain's final destination may only gain a slot from a
	// later move's departure, so the early position the collapse kept can be
	// wrong. Emit every move whose destination has room first; a source gains
	// its slot back as soon as its move is emitted.
	free := make(map[string]int)
	for _, n := range sim.AllNodes {
		free[n.NodeID] = n.FreeSlots
	}
	for _, m := range moves {
		free[m.DestNode.NodeID]++
		free[m.SourceNode.NodeID]--
	}
	ordered := make([]ShardMove, 0, len(moves))
	for len(moves) > 0 {
		kept := moves[:0]
		for _, m := range moves {
			if free[m.DestNode.NodeID] > 0 {
				free[m.DestNode.NodeID]--
				free[m.SourceNode.NodeID]++
				ordered = append(ordered, m)
			} else {
				kept = append(kept, m)
			}
		}
		if len(kept) == len(moves) {
			ordered = append(ordered, kept...)
			break
		}
		moves = kept
	}
	plan.Moves = ordered
	for _, m := range ordered {
		switch {
		case m.SourceNode.DataCenter != m.DestNode.DataCenter:
			plan.MovesAcrossDC++
		case m.SourceNode.Rack != m.DestNode.Rack:
			plan.MovesAcrossRack++
		default:
			plan.MovesWithinRack++
		}
	}
	plan.TotalMoves = len(ordered)
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
