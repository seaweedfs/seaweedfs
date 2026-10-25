package erasure_coding

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestMinusParityShardsRatio covers the data/parity split for the default 10+4
// layout and other ratios. The pre-fix code hardcoded ids 10..13 as parity,
// which for a wide ratio (12+4, 16+6) DROPPED real data ids >= 10 and for a
// narrow ratio (9+3) kept parity id 9. The method now takes the data-shard
// count (<= 0 falls back to DataShardsCount).
func TestMinusParityShardsRatio(t *testing.T) {
	all := func(n int) []ShardId {
		ids := make([]ShardId, n)
		for i := range ids {
			ids[i] = ShardId(i)
		}
		return ids
	}
	tests := []struct {
		name          string
		dataShards    int
		inputShards   []ShardId
		expectedCount int
		expectedIds   []ShardId
	}{
		{"default 10+4 all present", DataShardsCount, all(14), 10, all(10)},
		{"zero ratio falls back to default", 0, all(14), 10, all(10)},
		{"9+3 all present", 9, all(12), 9, all(9)},
		{"9+3 drops parity id 9", 9, []ShardId{0, 1, 2, 9, 10, 11}, 3, []ShardId{0, 1, 2}},
		{"12+4 keeps data ids >= 10", 12, all(16), 12, all(12)},
		{"16+6 keeps data ids >= 10", 16, all(22), 16, all(16)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			si := NewShardsInfo()
			for _, id := range tt.inputShards {
				si.Set(NewShardInfo(id, ShardSize(1000)))
			}
			result := si.MinusParityShards(tt.dataShards)
			assert.Equal(t, tt.expectedCount, result.Count(), "count for ratio %d", tt.dataShards)
			for _, id := range tt.expectedIds {
				assert.True(t, result.Has(id), "data shard %d must remain for ratio %d", id, tt.dataShards)
			}
		})
	}
}
