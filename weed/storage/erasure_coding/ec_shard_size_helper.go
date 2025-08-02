package erasure_coding

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// GetShardSize returns the size of a specific shard from VolumeEcShardInformationMessage
// Returns the size and true if the shard exists, 0 and false if not present
func GetShardSize(msg *master_pb.VolumeEcShardInformationMessage, shardId ShardId) (size int64, found bool) {
	if msg == nil || msg.ShardSizes == nil {
		return 0, false
	}

	shardBits := ShardBits(msg.EcIndexBits)
	index, found := shardBits.ShardIdToIndex(shardId)
	if !found || index >= len(msg.ShardSizes) {
		return 0, false
	}

	return msg.ShardSizes[index], true
}

// SetShardSize sets the size of a specific shard in VolumeEcShardInformationMessage
// Returns true if successful, false if the shard is not present in EcIndexBits
func SetShardSize(msg *master_pb.VolumeEcShardInformationMessage, shardId ShardId, size int64) bool {
	if msg == nil {
		return false
	}

	shardBits := ShardBits(msg.EcIndexBits)
	index, found := shardBits.ShardIdToIndex(shardId)
	if !found {
		return false
	}

	// Initialize ShardSizes slice if needed
	expectedLength := shardBits.ShardIdCount()
	if msg.ShardSizes == nil {
		msg.ShardSizes = make([]int64, expectedLength)
	} else if len(msg.ShardSizes) != expectedLength {
		// Resize the slice to match the expected length
		newSizes := make([]int64, expectedLength)
		copy(newSizes, msg.ShardSizes)
		msg.ShardSizes = newSizes
	}

	if index >= len(msg.ShardSizes) {
		return false
	}

	msg.ShardSizes[index] = size
	return true
}

// InitializeShardSizes initializes the ShardSizes slice based on EcIndexBits
// This ensures the slice has the correct length for all present shards
func InitializeShardSizes(msg *master_pb.VolumeEcShardInformationMessage) {
	if msg == nil {
		return
	}

	shardBits := ShardBits(msg.EcIndexBits)
	expectedLength := shardBits.ShardIdCount()

	if msg.ShardSizes == nil || len(msg.ShardSizes) != expectedLength {
		msg.ShardSizes = make([]int64, expectedLength)
	}
}
