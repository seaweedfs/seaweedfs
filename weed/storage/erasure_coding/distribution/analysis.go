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
// holds for the volume being analyzed. A node id may only register once;
// registering it again updates its capacity and adds any new shards. The
// first registration wins for the node's place in the topology.
func (a *TopologyAnalysis) AddNode(node *TopologyNode) {
	registered, ok := a.AllNodes[node.NodeID]
	if !ok {
		registered = node
		a.AllNodes[node.NodeID] = node
		a.RackToNodes[rackKey(node.DataCenter, node.Rack)] = append(a.RackToNodes[rackKey(node.DataCenter, node.Rack)], node)
		if !contains(a.DCToRacks[node.DataCenter], node.Rack) {
			a.DCToRacks[node.DataCenter] = append(a.DCToRacks[node.DataCenter], node.Rack)
		}
	} else {
		registered.FreeSlots = node.FreeSlots
		registered.TotalShards = node.TotalShards
	}
	seen := node.ShardIDs[:0]
	for _, shardID := range node.ShardIDs {
		if !contains(seen, shardID) {
			seen = append(seen, shardID)
		}
	}
	node.ShardIDs = seen
	for _, shardID := range node.ShardIDs {
		a.AddShardLocation(ShardLocation{
			ShardID:    shardID,
			NodeID:     registered.NodeID,
			DataCenter: registered.DataCenter,
			Rack:       registered.Rack,
		})
	}
}

// AddShardLocation records one shard's placement in the topology. Repeated
// calls for a shard already known to its node are ignored, so callers may
// either set ShardIDs on the TopologyNode or register each shard here. A
// location naming an unregistered node registers that node so the rebalancer
// can see the shard; for a registered node the node's own dc/rack win.
func (a *TopologyAnalysis) AddShardLocation(loc ShardLocation) {
	node, ok := a.AllNodes[loc.NodeID]
	if !ok {
		node = &TopologyNode{NodeID: loc.NodeID, DataCenter: loc.DataCenter, Rack: loc.Rack}
		a.AddNode(node)
	}
	if contains(a.NodeToShards[loc.NodeID], loc.ShardID) {
		return
	}
	a.ShardsByDC[node.DataCenter]++
	a.ShardsByRack[rackKey(node.DataCenter, node.Rack)]++
	a.ShardsByNode[loc.NodeID]++
	a.DCToShards[node.DataCenter] = append(a.DCToShards[node.DataCenter], loc.ShardID)
	a.RackToShards[rackKey(node.DataCenter, node.Rack)] = append(a.RackToShards[rackKey(node.DataCenter, node.Rack)], loc.ShardID)
	a.NodeToShards[loc.NodeID] = append(a.NodeToShards[loc.NodeID], loc.ShardID)
	a.TotalShards++
	if !contains(node.ShardIDs, loc.ShardID) {
		node.ShardIDs = append(node.ShardIDs, loc.ShardID)
	}
}

// rackKey qualifies a rack name with its data center: rack names are only
// unique within a DC, so two DCs can both hold a "rack1". NUL separates the
// parts so a "/" inside either name cannot merge distinct racks.
func rackKey(dc, rack string) string {
	return dc + "\x00" + rack
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
