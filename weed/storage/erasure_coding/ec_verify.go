package erasure_coding

import (
	"bytes"
	"fmt"
	"io"

	"github.com/klauspost/reedsolomon"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
)

// ShardReader is a function that reads a chunk of a specific shard
// It returns the data read, or an error.
// If the shard does not exist (isDeleted), it should return nil, nil.
type ShardReader func(shardId uint32, offset int64, size int64) (data []byte, err error)

// VerifyEcShards verifies the consistency of EC shards for a volume.
// it uses the provided reader to fetch data from shards (local or distributed).
func VerifyEcShards(baseFileName string, shardSize int64, shardReader ShardReader) (verified bool, suspectShardIds []uint32, err error) {
	// Attempt to load EC config from .vif file
	var ctx *ECContext
	if volumeInfo, _, found, _ := volume_info.MaybeLoadVolumeInfo(baseFileName + ".vif"); found && volumeInfo.EcShardConfig != nil {
		ds := int(volumeInfo.EcShardConfig.DataShards)
		ps := int(volumeInfo.EcShardConfig.ParityShards)

		if ds > 0 && ps > 0 && ds+ps <= MaxShardCount {
			ctx = &ECContext{
				DataShards:   ds,
				ParityShards: ps,
			}
			glog.V(1).Infof("Verifying EC shards for %s with config from .vif: %s", baseFileName, ctx.String())
		} else {
			glog.Warningf("Invalid EC config in .vif for %s (data=%d, parity=%d), using default", baseFileName, ds, ps)
			ctx = NewDefaultECContext("", 0)
		}
	} else {
		ctx = NewDefaultECContext("", 0)
	}

	return VerifyEcShardsWithContext(ctx, shardSize, shardReader)
}

// VerifyEcShardsWithContext verifies EC shards using the provided EC context
func VerifyEcShardsWithContext(ctx *ECContext, shardSize int64, shardReader ShardReader) (verified bool, suspectShardIds []uint32, err error) {
	return VerifyEcShardsWithReader(ctx, shardSize, shardReader)
}

// Function with size argument
func VerifyEcShardsWithReader(ctx *ECContext, shardSize int64, shardReader ShardReader) (verified bool, suspectShardIds []uint32, err error) {
	glog.V(2).Infof("EC verification: checking %d shards, size %d", ctx.Total(), shardSize)

	// Verify in chunks to manage memory usage
	// For very large shards, we verify block by block
	const maxBlockSize = ErasureCodingSmallBlockSize // 1MB per shard
	verified, suspectShardIds, err = verifyEcShardsInChunks(ctx, shardSize, maxBlockSize, shardReader)

	return verified, suspectShardIds, err
}

// verifyEcShardsInChunks verifies EC shards block by block
func verifyEcShardsInChunks(ctx *ECContext, shardSize int64, blockSize int64, shardReader ShardReader) (verified bool, suspectShardIds []uint32, err error) {
	encoder, err := ctx.CreateEncoder()
	if err != nil {
		return false, nil, fmt.Errorf("failed to create encoder: %w", err)
	}

	// Process the file in chunks
	var offset int64
	blockNumber := 0

	// We need to know which shards are present for the whole volume?
	// Or check per block?
	// EC shards should be uniform size (mostly).

	for offset < shardSize {
		remaining := shardSize - offset
		currentBlockSize := blockSize
		if remaining < blockSize {
			currentBlockSize = remaining
		}

		// Read one block from each shard
		buffers := make([][]byte, ctx.Total())
		existingShards := make([]bool, ctx.Total())
		shardCount := 0

		for i := 0; i < ctx.Total(); i++ {
			data, err := shardReader(uint32(i), offset, currentBlockSize)
			if err != nil {
				if err != io.EOF {
					// Treat retrieval error as missing shard for now, but log it
					glog.V(4).Infof("failed to read shard %d: %v", i, err)
				}
				buffers[i] = nil
			} else {
				if int64(len(data)) != currentBlockSize {
					// Padding if short read?
					// If it's the very last block, it might be short?
					// But ReedSolomon expects uniform length blocks.
					// SeaweedFS EC encoder pads with 0.
					padded := make([]byte, currentBlockSize)
					copy(padded, data)
					buffers[i] = padded
				} else {
					buffers[i] = data
				}
				existingShards[i] = true
				shardCount++
			}
		}

		if shardCount < ctx.DataShards {
			return false, nil, fmt.Errorf("insufficient shards at offset %d: found %d, need %d", offset, shardCount, ctx.DataShards)
		}

		// Verify this block
		ok, verifyErr := encoder.Verify(buffers)
		if verifyErr != nil {
			return false, nil, fmt.Errorf("verification error at block %d (offset %d): %w", blockNumber, offset, verifyErr)
		}

		if !ok {
			glog.Warningf("Verification failed at block %d (offset %d)", blockNumber, offset)
			suspects := identifyCorruptShards(buffers, existingShards, encoder, ctx)
			return false, suspects, nil
		}

		blockNumber++
		offset += currentBlockSize
	}

	glog.V(1).Infof("Successfully verified %d blocks", blockNumber)
	return true, nil, nil
}

func identifyCorruptShards(buffers [][]byte, existingShards []bool, encoder reedsolomon.Encoder, ctx *ECContext) []uint32 {
	glog.V(2).Info("Attempting to identify corrupt shard(s)...")

	// Try increasing number of corrupt shards, up to parity count
	maxCorrupt := ctx.ParityShards
	if maxCorrupt > 4 {
		maxCorrupt = 4 // Sanity limit
	}

	for n := 1; n <= maxCorrupt; n++ {
		glog.V(2).Infof("Searching for combinations of %d corrupt shards...", n)
		found, suspects := findCorruptCombination(buffers, existingShards, encoder, ctx, n, 0, []int{})
		if found {
			glog.V(1).Infof("Identified corrupt shards: %v", suspects)
			return suspects
		}
	}

	glog.Warning("Could not identify specific corrupt shard(s) (corruption might be widespread or ambiguous)")
	// Fallback: report all existing shards as suspects if we can't narrow it down
	var suspects []uint32
	for i := 0; i < ctx.Total(); i++ {
		if existingShards[i] {
			suspects = append(suspects, uint32(i))
		}
	}
	return suspects
}

func findCorruptCombination(buffers [][]byte, existingShards []bool, encoder reedsolomon.Encoder, ctx *ECContext, n int, start int, current []int) (bool, []uint32) {
	if len(current) == n {
		if ok, _ := tryVerifyWithMissing(buffers, current, encoder, ctx); ok {
			suspects := make([]uint32, n)
			for i, idx := range current {
				suspects[i] = uint32(idx)
			}
			return true, suspects
		}
		return false, nil
	}

	for i := start; i < ctx.Total(); i++ {
		if !existingShards[i] {
			continue
		}
		found, suspects := findCorruptCombination(buffers, existingShards, encoder, ctx, n, i+1, append(current, i))
		if found {
			return true, suspects
		}
	}

	return false, nil
}

func tryVerifyWithMissing(buffers [][]byte, missingIndices []int, encoder reedsolomon.Encoder, ctx *ECContext) (bool, error) {
	if len(buffers) < ctx.Total() {
		return false, fmt.Errorf("insufficient buffers")
	}

	// Calculate present indices
	presentIndices := []int{}
	for i := 0; i < ctx.Total(); i++ {
		isMissing := false
		for _, m := range missingIndices {
			if i == m {
				isMissing = true
				break
			}
		}
		if !isMissing && buffers[i] != nil {
			presentIndices = append(presentIndices, i)
		}
	}

	// We need at least DataShards to reconstruct anything
	if len(presentIndices) < ctx.DataShards {
		return false, nil
	}

	// Pick the first DataShards to act as the source for reconstruction
	// To be truly robust against ambiguity, we'd need to check if the extras are consistent.
	testBuffers := make([][]byte, ctx.Total())
	for i := 0; i < ctx.DataShards; i++ {
		idx := presentIndices[i]
		testBuffers[idx] = make([]byte, len(buffers[idx]))
		copy(testBuffers[idx], buffers[idx])
	}

	// Reconstruct the others
	if err := encoder.Reconstruct(testBuffers); err != nil {
		return false, err
	}

	// Now check if the other "present" shards match their reconstructed versions
	for i := ctx.DataShards; i < len(presentIndices); i++ {
		idx := presentIndices[i]
		if bytes.Compare(buffers[idx], testBuffers[idx]) != 0 {
			return false, nil // Inconsistency found
		}
	}

	return true, nil
}
