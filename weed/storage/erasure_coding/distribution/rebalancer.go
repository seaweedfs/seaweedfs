package distribution


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

