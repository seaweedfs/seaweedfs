package erasure_coding

import (
	"fmt"

	"github.com/klauspost/reedsolomon"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// ECContext encapsulates erasure coding parameters for encoding/decoding operations
type ECContext struct {
	DataShards   int
	ParityShards int
	Collection   string
	VolumeId     needle.VolumeId
}

// Total returns the total number of shards (data + parity)
func (ctx *ECContext) Total() int {
	return ctx.DataShards + ctx.ParityShards
}

// NewDefaultECContext creates a context with default 10+4 shard configuration
func NewDefaultECContext(collection string, volumeId needle.VolumeId) *ECContext {
	return &ECContext{
		DataShards:   DataShardsCount,
		ParityShards: ParityShardsCount,
		Collection:   collection,
		VolumeId:     volumeId,
	}
}

// CreateEncoder creates a Reed-Solomon encoder for this context
func (ctx *ECContext) CreateEncoder() (reedsolomon.Encoder, error) {
	return reedsolomon.New(ctx.DataShards, ctx.ParityShards)
}

// ToExt returns the file extension for a given shard index
func (ctx *ECContext) ToExt(shardIndex int) string {
	return fmt.Sprintf(".ec%02d", shardIndex)
}

// String returns a human-readable representation of the EC configuration
func (ctx *ECContext) String() string {
	return fmt.Sprintf("%d+%d (total: %d)", ctx.DataShards, ctx.ParityShards, ctx.Total())
}
