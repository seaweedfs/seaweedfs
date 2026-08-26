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
	// BlockSize > 0 selects the uniform block layout: every shard is a single
	// contiguous block of this many bytes, so consecutive .dat ranges stay on
	// one shard instead of striping across all of them at 1MiB granularity.
	// 0 is the legacy two-tier 1GiB/1MiB layout.
	BlockSize int64
}

// Total returns the total number of shards (data + parity)
func (ctx *ECContext) Total() int {
	return ctx.DataShards + ctx.ParityShards
}

// ValidEcShardCounts reports whether a recorded (data, parity) pair could
// describe a real EC volume. The sum is taken in uint64 deliberately: on a
// 32-bit build `int` is 32 bits, so converting each count first and adding
// them wraps for values near the uint32 ceiling — 0x7fffffff + 0x7fffffff
// lands at -2, which slips under the MaxShardCount bound.
func ValidEcShardCounts(dataShards, parityShards uint32) bool {
	return dataShards > 0 && parityShards > 0 &&
		uint64(dataShards)+uint64(parityShards) <= uint64(MaxShardCount)
}

// LargeBlockSize returns the large-block length of this context's shard
// layout; nil-safe so callers can pass an unset context for the legacy layout.
func (ctx *ECContext) LargeBlockSize() int64 {
	if ctx != nil && ctx.BlockSize > 0 {
		return ctx.BlockSize
	}
	return ErasureCodingLargeBlockSize
}

// SmallBlockSize returns the small-block length of this context's shard layout.
func (ctx *ECContext) SmallBlockSize() int64 {
	if ctx != nil && ctx.BlockSize > 0 {
		return ctx.BlockSize
	}
	return ErasureCodingSmallBlockSize
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

// BackgroundECContext returns a non-nil placeholder EC context, analogous to
// context.Background(): pass it to WriteEcFiles / RebuildEcFiles when the caller
// has no specific layout, rather than a nil context. Its zero Total() is the
// "unset" signal — WriteEcFiles resolves it to the default ratio and
// RebuildEcFiles resolves it from the volume's .vif (falling back to default),
// so the placeholder itself never reaches the encoder. A fresh value is returned
// each call so callers cannot mutate a shared default.
func BackgroundECContext() *ECContext {
	return &ECContext{}
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
