// Package distribution provides EC shard distribution algorithms with configurable EC ratios.
package distribution

// ECConfig holds erasure coding configuration parameters.
// This replaces hard-coded constants like DataShardsCount=10, ParityShardsCount=4.
type ECConfig struct {
	DataShards   int // Number of data shards (e.g., 10)
	ParityShards int // Number of parity shards (e.g., 4)
}

// ReplicationConfig holds the parsed replication policy
type ReplicationConfig struct {
	MinDataCenters  int // X+1 from XYZ replication (minimum DCs to use)
	MinRacksPerDC   int // Y+1 from XYZ replication (minimum racks per DC)
	MinNodesPerRack int // Z+1 from XYZ replication (minimum nodes per rack)

	// Original replication string (for logging/debugging)
	Original string
}
