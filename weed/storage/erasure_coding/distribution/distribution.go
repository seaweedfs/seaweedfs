package distribution

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
