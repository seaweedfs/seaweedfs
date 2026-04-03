package distribution

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
