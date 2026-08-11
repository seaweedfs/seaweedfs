package balancer

// NodeLoad is the load score used when selecting a source or target node.
// Callers decide which nodes are eligible and provide the score appropriate to
// their balancing policy (for example, volume density or EC shard fullness).
type NodeLoad struct {
	ID    string
	Score float64
}

// PickMostLoaded returns the index of the highest-scored node. Ties are broken
// by node id so callers get the same plan regardless of map iteration order.
// It returns -1 when nodes is empty.
func PickMostLoaded(nodes []NodeLoad) int {
	best := -1
	for i, node := range nodes {
		if best < 0 || node.Score > nodes[best].Score ||
			(node.Score == nodes[best].Score && node.ID < nodes[best].ID) {
			best = i
		}
	}
	return best
}

// PickLeastLoaded returns the index of the lowest-scored node. Ties are broken
// by node id so callers get the same plan regardless of map iteration order.
// It returns -1 when nodes is empty.
func PickLeastLoaded(nodes []NodeLoad) int {
	best := -1
	for i, node := range nodes {
		if best < 0 || node.Score < nodes[best].Score ||
			(node.Score == nodes[best].Score && node.ID < nodes[best].ID) {
			best = i
		}
	}
	return best
}
