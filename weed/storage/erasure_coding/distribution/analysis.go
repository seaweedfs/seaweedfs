package distribution

import (
	"fmt"
	"slices"
)

// ShardLocation represents where a shard is located in the topology
type ShardLocation struct {
	ShardID    int
	NodeID     string
	DataCenter string
	Rack       string
}

// TopologyNode represents a node in the topology that can hold EC shards
type TopologyNode struct {
	NodeID      string
	DataCenter  string
	Rack        string
	FreeSlots   int   // Available slots for new shards
	ShardIDs    []int // Shard IDs currently on this node for a specific volume
	TotalShards int   // Total shards on this node (for all volumes)
}

// TopologyAnalysis holds the current shard distribution analysis for a volume
type TopologyAnalysis struct {
	// Shard counts at each level
	ShardsByDC   map[string]int
	ShardsByRack map[string]int
	ShardsByNode map[string]int

	// Detailed shard locations
	DCToShards   map[string][]int // DC -> list of shard IDs
	RackToShards map[string][]int // Rack -> list of shard IDs
	NodeToShards map[string][]int // NodeID -> list of shard IDs

	// Topology structure
	DCToRacks   map[string][]string        // DC -> list of rack IDs
	RackToNodes map[string][]*TopologyNode // Rack -> list of nodes
	AllNodes    map[string]*TopologyNode   // NodeID -> node info

	// Statistics
	TotalShards int
	TotalNodes  int
	TotalRacks  int
	TotalDCs    int
}

// NewTopologyAnalysis creates a new empty analysis
func NewTopologyAnalysis() *TopologyAnalysis {
	return &TopologyAnalysis{
		ShardsByDC:   make(map[string]int),
		ShardsByRack: make(map[string]int),
		ShardsByNode: make(map[string]int),
		DCToShards:   make(map[string][]int),
		RackToShards: make(map[string][]int),
		NodeToShards: make(map[string][]int),
		DCToRacks:    make(map[string][]string),
		RackToNodes:  make(map[string][]*TopologyNode),
		AllNodes:     make(map[string]*TopologyNode),
	}
}

// AddShardLocation adds a shard location to the analysis
func (a *TopologyAnalysis) AddShardLocation(loc ShardLocation) {
	// Update counts
	a.ShardsByDC[loc.DataCenter]++
	a.ShardsByRack[loc.Rack]++
	a.ShardsByNode[loc.NodeID]++

	// Update shard lists
	a.DCToShards[loc.DataCenter] = append(a.DCToShards[loc.DataCenter], loc.ShardID)
	a.RackToShards[loc.Rack] = append(a.RackToShards[loc.Rack], loc.ShardID)
	a.NodeToShards[loc.NodeID] = append(a.NodeToShards[loc.NodeID], loc.ShardID)

	a.TotalShards++
}

// AddNode adds a node to the topology (even if it has no shards)
func (a *TopologyAnalysis) AddNode(node *TopologyNode) {
	if _, exists := a.AllNodes[node.NodeID]; exists {
		return // Already added
	}

	a.AllNodes[node.NodeID] = node
	a.TotalNodes++

	// Update topology structure
	if !slices.Contains(a.DCToRacks[node.DataCenter], node.Rack) {
		a.DCToRacks[node.DataCenter] = append(a.DCToRacks[node.DataCenter], node.Rack)
	}
	a.RackToNodes[node.Rack] = append(a.RackToNodes[node.Rack], node)

	// Update counts
	if _, exists := a.ShardsByDC[node.DataCenter]; !exists {
		a.TotalDCs++
	}
	if _, exists := a.ShardsByRack[node.Rack]; !exists {
		a.TotalRacks++
	}
}

// Finalize computes final statistics after all data is added
func (a *TopologyAnalysis) Finalize() {
	// Ensure we have accurate DC and rack counts
	dcSet := make(map[string]bool)
	rackSet := make(map[string]bool)
	for _, node := range a.AllNodes {
		dcSet[node.DataCenter] = true
		rackSet[node.Rack] = true
	}
	a.TotalDCs = len(dcSet)
	a.TotalRacks = len(rackSet)
	a.TotalNodes = len(a.AllNodes)
}

// String returns a summary of the analysis
func (a *TopologyAnalysis) String() string {
	return fmt.Sprintf("TopologyAnalysis{shards:%d, nodes:%d, racks:%d, dcs:%d}",
		a.TotalShards, a.TotalNodes, a.TotalRacks, a.TotalDCs)
}

// DetailedString returns a detailed multi-line summary
func (a *TopologyAnalysis) DetailedString() string {
	s := fmt.Sprintf("Topology Analysis:\n")
	s += fmt.Sprintf("  Total Shards: %d\n", a.TotalShards)
	s += fmt.Sprintf("  Data Centers: %d\n", a.TotalDCs)
	for dc, count := range a.ShardsByDC {
		s += fmt.Sprintf("    %s: %d shards\n", dc, count)
	}
	s += fmt.Sprintf("  Racks: %d\n", a.TotalRacks)
	for rack, count := range a.ShardsByRack {
		s += fmt.Sprintf("    %s: %d shards\n", rack, count)
	}
	s += fmt.Sprintf("  Nodes: %d\n", a.TotalNodes)
	for nodeID, count := range a.ShardsByNode {
		if count > 0 {
			s += fmt.Sprintf("    %s: %d shards\n", nodeID, count)
		}
	}
	return s
}

// TopologyExcess represents a topology level (DC/rack/node) with excess shards
type TopologyExcess struct {
	ID     string          // DC/rack/node ID
	Level  string          // "dc", "rack", or "node"
	Excess int             // Number of excess shards (above target)
	Shards []int           // Shard IDs at this level
	Nodes  []*TopologyNode // Nodes at this level (for finding sources)
}

// CalculateDCExcess returns DCs with more shards than the target
func CalculateDCExcess(analysis *TopologyAnalysis, dist *ECDistribution) []TopologyExcess {
	var excess []TopologyExcess

	for dc, count := range analysis.ShardsByDC {
		if count > dist.TargetShardsPerDC {
			// Collect nodes in this DC
			var nodes []*TopologyNode
			for _, rack := range analysis.DCToRacks[dc] {
				nodes = append(nodes, analysis.RackToNodes[rack]...)
			}
			excess = append(excess, TopologyExcess{
				ID:     dc,
				Level:  "dc",
				Excess: count - dist.TargetShardsPerDC,
				Shards: analysis.DCToShards[dc],
				Nodes:  nodes,
			})
		}
	}

	// Sort by excess (most excess first)
	slices.SortFunc(excess, func(a, b TopologyExcess) int {
		return b.Excess - a.Excess
	})

	return excess
}

// CalculateRackExcess returns racks with more shards than the target (within a DC)
func CalculateRackExcess(analysis *TopologyAnalysis, dc string, targetPerRack int) []TopologyExcess {
	var excess []TopologyExcess

	for _, rack := range analysis.DCToRacks[dc] {
		count := analysis.ShardsByRack[rack]
		if count > targetPerRack {
			excess = append(excess, TopologyExcess{
				ID:     rack,
				Level:  "rack",
				Excess: count - targetPerRack,
				Shards: analysis.RackToShards[rack],
				Nodes:  analysis.RackToNodes[rack],
			})
		}
	}

	slices.SortFunc(excess, func(a, b TopologyExcess) int {
		return b.Excess - a.Excess
	})

	return excess
}

// CalculateUnderservedDCs returns DCs that have fewer shards than target
func CalculateUnderservedDCs(analysis *TopologyAnalysis, dist *ECDistribution) []string {
	var underserved []string

	// Check existing DCs
	for dc, count := range analysis.ShardsByDC {
		if count < dist.TargetShardsPerDC {
			underserved = append(underserved, dc)
		}
	}

	// Check DCs with nodes but no shards
	for dc := range analysis.DCToRacks {
		if _, exists := analysis.ShardsByDC[dc]; !exists {
			underserved = append(underserved, dc)
		}
	}

	return underserved
}

// CalculateUnderservedRacks returns racks that have fewer shards than target
func CalculateUnderservedRacks(analysis *TopologyAnalysis, dc string, targetPerRack int) []string {
	var underserved []string

	for _, rack := range analysis.DCToRacks[dc] {
		count := analysis.ShardsByRack[rack]
		if count < targetPerRack {
			underserved = append(underserved, rack)
		}
	}

	return underserved
}
