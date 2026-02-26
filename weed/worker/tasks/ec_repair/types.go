package ec_repair

// VolumeKey identifies a set of EC shards for a volume within a collection and disk type.
type VolumeKey struct {
	VolumeID   uint32
	Collection string
	DiskType   string
}

// ShardLocation describes one shard copy for a volume.
type ShardLocation struct {
	NodeID      string
	NodeAddress string
	DataCenter  string
	Rack        string
	DiskType    string
	DiskID      uint32
	ShardID     uint32
	Size        int64
}

// RepairCandidate summarizes EC shard issues for detection.
type RepairCandidate struct {
	VolumeID         uint32
	Collection       string
	DiskType         string
	MissingShards    int
	ExtraShards      int
	MismatchedShards int
}

// ShardTarget describes where to place rebuilt shards.
type ShardTarget struct {
	NodeAddress string
	DiskID      uint32
	ShardIDs    []uint32
}

// RebuilderPlan describes the node that performs shard rebuilds.
type RebuilderPlan struct {
	NodeAddress string
	DiskID      uint32
	LocalShards []uint32
}

// RepairPlan describes all actions needed to repair an EC volume.
type RepairPlan struct {
	VolumeID      uint32
	Collection    string
	DiskType      string
	MissingShards []uint32
	Targets       []ShardTarget
	DeleteByNode  map[string][]uint32
	Rebuilder     RebuilderPlan
	CopySources   map[uint32]string // shardID -> source node address
}
