package distribution

// NewTopologyAnalysis creates an empty topology analysis.
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

// AddNode registers a node in the topology, including any shards it already
// holds for the volume being analyzed.
func (a *TopologyAnalysis) AddNode(node *TopologyNode) {
	a.AllNodes[node.NodeID] = node
	a.RackToNodes[rackKey(node.DataCenter, node.Rack)] = append(a.RackToNodes[rackKey(node.DataCenter, node.Rack)], node)
	if !contains(a.DCToRacks[node.DataCenter], node.Rack) {
		a.DCToRacks[node.DataCenter] = append(a.DCToRacks[node.DataCenter], node.Rack)
	}
	for _, shardID := range node.ShardIDs {
		a.AddShardLocation(ShardLocation{
			ShardID:    shardID,
			NodeID:     node.NodeID,
			DataCenter: node.DataCenter,
			Rack:       node.Rack,
		})
	}
}

// AddShardLocation records one shard's placement in the topology. Repeated
// calls for a shard already known to its node are ignored, so callers may
// either set ShardIDs on the TopologyNode or register each shard here.
func (a *TopologyAnalysis) AddShardLocation(loc ShardLocation) {
	if contains(a.NodeToShards[loc.NodeID], loc.ShardID) {
		return
	}
	a.ShardsByDC[loc.DataCenter]++
	a.ShardsByRack[rackKey(loc.DataCenter, loc.Rack)]++
	a.ShardsByNode[loc.NodeID]++
	a.DCToShards[loc.DataCenter] = append(a.DCToShards[loc.DataCenter], loc.ShardID)
	a.RackToShards[rackKey(loc.DataCenter, loc.Rack)] = append(a.RackToShards[rackKey(loc.DataCenter, loc.Rack)], loc.ShardID)
	a.NodeToShards[loc.NodeID] = append(a.NodeToShards[loc.NodeID], loc.ShardID)
	a.TotalShards++
	if node, ok := a.AllNodes[loc.NodeID]; ok && !contains(node.ShardIDs, loc.ShardID) {
		node.ShardIDs = append(node.ShardIDs, loc.ShardID)
	}
}

// rackKey qualifies a rack name with its data center: rack names are only
// unique within a DC, so two DCs can both hold a "rack1".
func rackKey(dc, rack string) string {
	return dc + "/" + rack
}

// Finalize computes topology statistics after all nodes and shards are added.
func (a *TopologyAnalysis) Finalize() {
	a.TotalNodes = len(a.AllNodes)
	a.TotalRacks = len(a.RackToNodes)
	a.TotalDCs = len(a.DCToRacks)
}

func contains[T comparable](list []T, s T) bool {
	for _, item := range list {
		if item == s {
			return true
		}
	}
	return false
}

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

// TopologyExcess represents a topology level (DC/rack/node) with excess shards
type TopologyExcess struct {
	ID     string          // DC/rack/node ID
	Level  string          // "dc", "rack", or "node"
	Excess int             // Number of excess shards (above target)
	Shards []int           // Shard IDs at this level
	Nodes  []*TopologyNode // Nodes at this level (for finding sources)
}
