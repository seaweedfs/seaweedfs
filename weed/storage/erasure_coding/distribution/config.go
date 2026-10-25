// Package distribution provides EC shard distribution algorithms with configurable EC ratios.
package distribution

import (
	"fmt"
	"sort"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

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

// NewECConfig creates an erasure coding configuration with the given shard counts.
func NewECConfig(dataShards, parityShards int) (ECConfig, error) {
	if dataShards < 1 || parityShards < 1 {
		return ECConfig{}, fmt.Errorf("data and parity shard counts must be positive, got %d+%d", dataShards, parityShards)
	}
	return ECConfig{DataShards: dataShards, ParityShards: parityShards}, nil
}

// DefaultECConfig returns the standard 10+4 erasure coding configuration.
func DefaultECConfig() ECConfig {
	return ECConfig{DataShards: erasure_coding.DataShardsCount, ParityShards: erasure_coding.ParityShardsCount}
}

// NewReplicationConfig parses a replication policy string like "110".
func NewReplicationConfigFromString(s string) (ReplicationConfig, error) {
	rp, err := super_block.NewReplicaPlacementFromString(s)
	if err != nil {
		return ReplicationConfig{}, err
	}
	return NewReplicationConfig(rp), nil
}

// NewReplicationConfig converts a replica placement into a replication config.
func NewReplicationConfig(rp *super_block.ReplicaPlacement) ReplicationConfig {
	if rp == nil {
		rp = &super_block.ReplicaPlacement{}
	}
	return ReplicationConfig{
		MinDataCenters:  rp.DiffDataCenterCount + 1,
		MinRacksPerDC:   rp.DiffRackCount + 1,
		MinNodesPerRack: rp.SameRackCount + 1,
		Original:        rp.String(),
	}
}

// TotalShards returns the number of shards each volume splits into.
func (ec ECConfig) TotalShards() int {
	return ec.DataShards + ec.ParityShards
}

// CanLoseShards returns how many shards of a volume can be missing before the
// volume is no longer readable.
func (ec ECConfig) CanLoseShards() int {
	return ec.ParityShards
}

// IsDataShard reports whether the shard id is a data shard, which serves reads.
func (ec ECConfig) IsDataShard(shardID int) bool {
	return shardID >= 0 && shardID < ec.DataShards
}

// IsParityShard reports whether the shard id is a parity shard, which is only
// used for reconstruction.
func (ec ECConfig) IsParityShard(shardID int) bool {
	return shardID >= ec.DataShards && shardID < ec.TotalShards()
}

// SortShardsDataFirst orders shard ids so data shards come before parity
// shards, for initial placement.
func (ec ECConfig) SortShardsDataFirst(shards []int) []int {
	sorted := append([]int(nil), shards...)
	sort.Ints(sorted)
	return sorted
}

// SortShardsParityFirst orders shard ids so parity shards come before data
// shards, for picking which shards to move away first when rebalancing.
func (ec ECConfig) SortShardsParityFirst(shards []int) []int {
	sorted := append([]int(nil), shards...)
	sort.Sort(sort.Reverse(sort.IntSlice(sorted)))
	return sorted
}
