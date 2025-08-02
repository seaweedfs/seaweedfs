package erasure_coding

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func TestShardSizeHelpers(t *testing.T) {
	// Create a message with shards 0, 2, and 5 present (EcIndexBits = 0b100101 = 37)
	msg := &master_pb.VolumeEcShardInformationMessage{
		Id:          123,
		EcIndexBits: 37, // Binary: 100101, shards 0, 2, 5 are present
	}

	// Test SetShardSize
	if !SetShardSize(msg, 0, 1000) {
		t.Error("Failed to set size for shard 0")
	}
	if !SetShardSize(msg, 2, 2000) {
		t.Error("Failed to set size for shard 2")
	}
	if !SetShardSize(msg, 5, 5000) {
		t.Error("Failed to set size for shard 5")
	}

	// Test setting size for non-present shard should fail
	if SetShardSize(msg, 1, 1500) {
		t.Error("Should not be able to set size for non-present shard 1")
	}

	// Verify ShardSizes slice has correct length (3 shards)
	if len(msg.ShardSizes) != 3 {
		t.Errorf("Expected ShardSizes length 3, got %d", len(msg.ShardSizes))
	}

	// Test GetShardSize
	if size, found := GetShardSize(msg, 0); !found || size != 1000 {
		t.Errorf("Expected shard 0 size 1000, got %d (found: %v)", size, found)
	}
	if size, found := GetShardSize(msg, 2); !found || size != 2000 {
		t.Errorf("Expected shard 2 size 2000, got %d (found: %v)", size, found)
	}
	if size, found := GetShardSize(msg, 5); !found || size != 5000 {
		t.Errorf("Expected shard 5 size 5000, got %d (found: %v)", size, found)
	}

	// Test getting size for non-present shard
	if size, found := GetShardSize(msg, 1); found {
		t.Errorf("Should not find shard 1, but got size %d", size)
	}

	// Test direct slice access
	if len(msg.ShardSizes) != 3 {
		t.Errorf("Expected 3 shard sizes in slice, got %d", len(msg.ShardSizes))
	}

	expectedSizes := []int64{1000, 2000, 5000} // Ordered by shard ID: 0, 2, 5
	for i, expectedSize := range expectedSizes {
		if i < len(msg.ShardSizes) && msg.ShardSizes[i] != expectedSize {
			t.Errorf("Expected ShardSizes[%d] = %d, got %d", i, expectedSize, msg.ShardSizes[i])
		}
	}
}

func TestShardBitsHelpers(t *testing.T) {
	// Test with EcIndexBits = 37 (binary: 100101, shards 0, 2, 5)
	shardBits := ShardBits(37)

	// Test ShardIdToIndex
	if index, found := shardBits.ShardIdToIndex(0); !found || index != 0 {
		t.Errorf("Expected shard 0 at index 0, got %d (found: %v)", index, found)
	}
	if index, found := shardBits.ShardIdToIndex(2); !found || index != 1 {
		t.Errorf("Expected shard 2 at index 1, got %d (found: %v)", index, found)
	}
	if index, found := shardBits.ShardIdToIndex(5); !found || index != 2 {
		t.Errorf("Expected shard 5 at index 2, got %d (found: %v)", index, found)
	}

	// Test for non-present shard
	if index, found := shardBits.ShardIdToIndex(1); found {
		t.Errorf("Should not find shard 1, but got index %d", index)
	}

	// Test IndexToShardId
	if shardId, found := shardBits.IndexToShardId(0); !found || shardId != 0 {
		t.Errorf("Expected index 0 to be shard 0, got %d (found: %v)", shardId, found)
	}
	if shardId, found := shardBits.IndexToShardId(1); !found || shardId != 2 {
		t.Errorf("Expected index 1 to be shard 2, got %d (found: %v)", shardId, found)
	}
	if shardId, found := shardBits.IndexToShardId(2); !found || shardId != 5 {
		t.Errorf("Expected index 2 to be shard 5, got %d (found: %v)", shardId, found)
	}

	// Test for invalid index
	if shardId, found := shardBits.IndexToShardId(3); found {
		t.Errorf("Should not find shard for index 3, but got shard %d", shardId)
	}

	// Test EachSetIndex
	var collectedShards []ShardId
	shardBits.EachSetIndex(func(shardId ShardId) {
		collectedShards = append(collectedShards, shardId)
	})
	expectedShards := []ShardId{0, 2, 5}
	if len(collectedShards) != len(expectedShards) {
		t.Errorf("Expected EachSetIndex to collect %v, got %v", expectedShards, collectedShards)
	}
	for i, expected := range expectedShards {
		if i >= len(collectedShards) || collectedShards[i] != expected {
			t.Errorf("Expected EachSetIndex to collect %v, got %v", expectedShards, collectedShards)
			break
		}
	}
}
