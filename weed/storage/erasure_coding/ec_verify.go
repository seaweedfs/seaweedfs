package erasure_coding

import (
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

	var suspects []uint32

	// We iterate through each shard, assume it is the corrupt one, and try to verify the others.
	// Since Reconstruct uses the first N available shards to rebuild missing ones,
	// and Verify checks consistency of ALL shards present.
	//
	// Algorithm:
	// 1. Temporarily treat specific shard 'i' as missing (nil).
	// 2. Reconstruct the set. This will rebuild 'i' using 10 other shards.
	// 3. Verify the resulting full set.
	//    - If Verify passes: It means the 10 shards used for reconstruction AND the other 3 unused shards are all consistent.
	//      This implies the only inconsistency was indeed 'i'. -> 'i' is corrupt.
	//    - If Verify fails: It means the remaining shards are still inconsistent. -> 'i' is likely not the (only) problem.

	for i := 0; i < ctx.Total(); i++ {
		if !existingShards[i] {
			continue
		}

		// Create a working copy of buffers
		testBuffers := make([][]byte, ctx.Total())
		for j := 0; j < ctx.Total(); j++ {
			if j == i {
				testBuffers[j] = nil // Mark candidate as missing
			} else {
				// Copy data to avoid mutation if Reconstruct acts in place on existing slices (usually it doesn't for inputs, but safest to copy slice headers)
				// Reconstruct writes to nil buffers. It reads from non-nil.
				// To be safe against modification, we should copy the byte slices if we were paranoid,
				// but reedsolomon Reconstruct usually only writes to nil slots.
				// However, if we want to be 100% sure we can just pass the slice reference,
				// but we must ensure `testBuffers` structure is fresh.
				testBuffers[j] = buffers[j]
			}
		}

		// Reconstruct the "missing" candidate
		// This uses the other shards to fill in testBuffers[i]
		if err := encoder.Reconstruct(testBuffers); err != nil {
			glog.V(4).Infof("Failed to reconstruct assuming shard %d is bad: %v", i, err)
			continue
		}

		// Now verify consistency of the reconstructed set
		// If the remaining shards were consistent, reconstruction + verify should pass.
		// Note: verification checks that parity shards match data shards.
		ok, err := encoder.Verify(testBuffers)
		if err == nil && ok {
			glog.V(1).Infof("Identified corrupt shard: %d", i)
			suspects = append(suspects, uint32(i))
		}
	}

	if len(suspects) == 0 {
		glog.Warning("Could not identify specific corrupt shard(s) (corruption might be widespread or ambiguous)")
		// Fallback: report all existing shards as suspects if we can't narrow it down
		for i := 0; i < ctx.Total(); i++ {
			if existingShards[i] {
				suspects = append(suspects, uint32(i))
			}
		}
	}

	return suspects
}
