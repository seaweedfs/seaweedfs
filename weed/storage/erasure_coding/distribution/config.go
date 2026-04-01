// Package distribution provides EC shard distribution algorithms with configurable EC ratios.
package distribution

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

// ECConfig holds erasure coding configuration parameters.
// This replaces hard-coded constants like DataShardsCount=10, ParityShardsCount=4.
type ECConfig struct {
	DataShards   int // Number of data shards (e.g., 10)
	ParityShards int // Number of parity shards (e.g., 4)
}

// DefaultECConfig returns the standard 10+4 EC configuration
func DefaultECConfig() ECConfig {
	return ECConfig{
		DataShards:   10,
		ParityShards: 4,
	}
}

// NewECConfig creates a new EC configuration with validation
func NewECConfig(dataShards, parityShards int) (ECConfig, error) {
	if dataShards <= 0 {
		return ECConfig{}, fmt.Errorf("dataShards must be positive, got %d", dataShards)
	}
	if parityShards <= 0 {
		return ECConfig{}, fmt.Errorf("parityShards must be positive, got %d", parityShards)
	}
	if dataShards+parityShards > 32 {
		return ECConfig{}, fmt.Errorf("total shards (%d+%d=%d) exceeds maximum of 32",
			dataShards, parityShards, dataShards+parityShards)
	}
	return ECConfig{
		DataShards:   dataShards,
		ParityShards: parityShards,
	}, nil
}

// TotalShards returns the total number of shards (data + parity)
func (c ECConfig) TotalShards() int {
	return c.DataShards + c.ParityShards
}

// MaxTolerableLoss returns the maximum number of shards that can be lost
// while still being able to reconstruct the data
func (c ECConfig) MaxTolerableLoss() int {
	return c.ParityShards
}

// MinShardsForReconstruction returns the minimum number of shards needed
// to reconstruct the original data
func (c ECConfig) MinShardsForReconstruction() int {
	return c.DataShards
}

// String returns a human-readable representation
func (c ECConfig) String() string {
	return fmt.Sprintf("%d+%d (total: %d, can lose: %d)",
		c.DataShards, c.ParityShards, c.TotalShards(), c.MaxTolerableLoss())
}

// IsDataShard returns true if the shard ID is a data shard (0 to DataShards-1)
func (c ECConfig) IsDataShard(shardID int) bool {
	return shardID >= 0 && shardID < c.DataShards
}

// IsParityShard returns true if the shard ID is a parity shard (DataShards to TotalShards-1)
func (c ECConfig) IsParityShard(shardID int) bool {
	return shardID >= c.DataShards && shardID < c.TotalShards()
}

// SortShardsDataFirst returns a copy of shards sorted with data shards first.
// This is useful for initial placement where data shards should be spread out first.
func (c ECConfig) SortShardsDataFirst(shards []int) []int {
	result := make([]int, len(shards))
	copy(result, shards)

	// Partition: data shards first, then parity shards
	dataIdx := 0
	parityIdx := len(result) - 1

	sorted := make([]int, len(result))
	for _, s := range result {
		if c.IsDataShard(s) {
			sorted[dataIdx] = s
			dataIdx++
		} else {
			sorted[parityIdx] = s
			parityIdx--
		}
	}

	return sorted
}

// SortShardsParityFirst returns a copy of shards sorted with parity shards first.
// This is useful for rebalancing where we prefer to move parity shards.
func (c ECConfig) SortShardsParityFirst(shards []int) []int {
	result := make([]int, len(shards))
	copy(result, shards)

	// Partition: parity shards first, then data shards
	parityIdx := 0
	dataIdx := len(result) - 1

	sorted := make([]int, len(result))
	for _, s := range result {
		if c.IsParityShard(s) {
			sorted[parityIdx] = s
			parityIdx++
		} else {
			sorted[dataIdx] = s
			dataIdx--
		}
	}

	return sorted
}

// ReplicationConfig holds the parsed replication policy
type ReplicationConfig struct {
	MinDataCenters  int // X+1 from XYZ replication (minimum DCs to use)
	MinRacksPerDC   int // Y+1 from XYZ replication (minimum racks per DC)
	MinNodesPerRack int // Z+1 from XYZ replication (minimum nodes per rack)

	// Original replication string (for logging/debugging)
	Original string
}

// NewReplicationConfig creates a ReplicationConfig from a ReplicaPlacement
func NewReplicationConfig(rp *super_block.ReplicaPlacement) ReplicationConfig {
	if rp == nil {
		return ReplicationConfig{
			MinDataCenters:  1,
			MinRacksPerDC:   1,
			MinNodesPerRack: 1,
			Original:        "000",
		}
	}
	return ReplicationConfig{
		MinDataCenters:  rp.DiffDataCenterCount + 1,
		MinRacksPerDC:   rp.DiffRackCount + 1,
		MinNodesPerRack: rp.SameRackCount + 1,
		Original:        rp.String(),
	}
}

// NewReplicationConfigFromString creates a ReplicationConfig from a replication string
func NewReplicationConfigFromString(replication string) (ReplicationConfig, error) {
	rp, err := super_block.NewReplicaPlacementFromString(replication)
	if err != nil {
		return ReplicationConfig{}, err
	}
	return NewReplicationConfig(rp), nil
}

// TotalPlacementSlots returns the minimum number of unique placement locations
// based on the replication policy
func (r ReplicationConfig) TotalPlacementSlots() int {
	return r.MinDataCenters * r.MinRacksPerDC * r.MinNodesPerRack
}

// String returns a human-readable representation
func (r ReplicationConfig) String() string {
	return fmt.Sprintf("replication=%s (DCs:%d, Racks/DC:%d, Nodes/Rack:%d)",
		r.Original, r.MinDataCenters, r.MinRacksPerDC, r.MinNodesPerRack)
}
