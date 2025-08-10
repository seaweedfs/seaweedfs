package ec_vacuum

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"github.com/seaweedfs/seaweedfs/weed/worker/types/base"
	"google.golang.org/grpc"
)

// EcVacuumTask represents an EC vacuum task that collects, decodes, and re-encodes EC volumes
type EcVacuumTask struct {
	*base.BaseTask
	volumeID         uint32
	collection       string
	sourceNodes      map[pb.ServerAddress]erasure_coding.ShardBits
	sourceGeneration uint32 // generation to vacuum from (G)
	targetGeneration uint32 // generation to create (G+1)
	tempDir          string
	grpcDialOption   grpc.DialOption
}

// NewEcVacuumTask creates a new EC vacuum task instance
func NewEcVacuumTask(id string, volumeID uint32, collection string, sourceNodes map[pb.ServerAddress]erasure_coding.ShardBits, sourceGeneration uint32) *EcVacuumTask {
	return &EcVacuumTask{
		BaseTask:         base.NewBaseTask(id, types.TaskType("ec_vacuum")),
		volumeID:         volumeID,
		collection:       collection,
		sourceNodes:      sourceNodes,
		sourceGeneration: sourceGeneration,     // generation to vacuum from (G)
		targetGeneration: sourceGeneration + 1, // generation to create (G+1)
	}
}

// Execute performs the EC vacuum operation
func (t *EcVacuumTask) Execute(ctx context.Context, params *worker_pb.TaskParams) error {
	t.LogInfo("Starting EC vacuum task", map[string]interface{}{
		"volume_id":         t.volumeID,
		"collection":        t.collection,
		"source_generation": t.sourceGeneration,
		"target_generation": t.targetGeneration,
		"shard_nodes":       len(t.sourceNodes),
	})

	// Step 1: Create temporary working directory
	if err := t.createTempDir(); err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer t.cleanup()

	// Step 2: Collect EC shards to this worker
	targetNode, err := t.collectEcShardsToWorker()
	if err != nil {
		return fmt.Errorf("failed to collect EC shards: %w", err)
	}

	// Step 3: Decode EC shards into normal volume (skips deleted entries automatically)
	if err := t.decodeEcShardsToVolume(targetNode); err != nil {
		return fmt.Errorf("failed to decode EC shards to volume: %w", err)
	}

	// Step 4: Re-encode the cleaned volume into new EC shards
	if err := t.encodeVolumeToEcShards(targetNode); err != nil {
		return fmt.Errorf("failed to encode volume to EC shards: %w", err)
	}

	// Step 5: Distribute new EC shards to cluster
	if err := t.distributeNewEcShards(targetNode); err != nil {
		return fmt.Errorf("failed to distribute new EC shards: %w", err)
	}

	// Step 6: Clean up old EC shards
	if err := t.cleanupOldEcShards(); err != nil {
		t.LogWarning("Failed to clean up old EC shards", map[string]interface{}{
			"error": err.Error(),
		})
		// Don't fail the task for cleanup errors
	}

	t.LogInfo("EC vacuum task completed successfully", map[string]interface{}{
		"volume_id":  t.volumeID,
		"collection": t.collection,
	})

	return nil
}

// createTempDir creates a temporary directory for the vacuum operation
func (t *EcVacuumTask) createTempDir() error {
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("ec_vacuum_%d_%d", t.volumeID, time.Now().Unix()))
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return err
	}
	t.tempDir = tempDir
	t.LogInfo("Created temporary directory", map[string]interface{}{
		"temp_dir": tempDir,
	})
	return nil
}

// collectEcShardsToWorker collects all EC shards to the current worker
func (t *EcVacuumTask) collectEcShardsToWorker() (pb.ServerAddress, error) {
	// Find the node with the most shards as the target
	var targetNode pb.ServerAddress
	maxShardCount := 0
	var existingEcIndexBits erasure_coding.ShardBits

	for node, shardBits := range t.sourceNodes {
		shardCount := shardBits.ShardIdCount()
		if shardCount > maxShardCount {
			maxShardCount = shardCount
			targetNode = node
			existingEcIndexBits = shardBits
		}
	}

	t.LogInfo("Selected target node for shard collection", map[string]interface{}{
		"target_node":   targetNode,
		"existing_bits": existingEcIndexBits,
		"shard_count":   maxShardCount,
	})

	// Copy missing shards to target node
	for sourceNode, shardBits := range t.sourceNodes {
		if sourceNode == targetNode {
			continue
		}

		needToCopyBits := shardBits.Minus(existingEcIndexBits)
		if needToCopyBits.ShardIdCount() == 0 {
			continue
		}

		err := operation.WithVolumeServerClient(false, targetNode, t.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			t.LogInfo("Copying EC shards", map[string]interface{}{
				"volume_id": t.volumeID,
				"shard_ids": needToCopyBits.ShardIds(),
				"from":      sourceNode,
				"to":        targetNode,
			})

			_, copyErr := client.VolumeEcShardsCopy(context.Background(), &volume_server_pb.VolumeEcShardsCopyRequest{
				VolumeId:       t.volumeID,
				Collection:     t.collection,
				ShardIds:       needToCopyBits.ToUint32Slice(),
				CopyEcxFile:    false,
				CopyEcjFile:    true,
				CopyVifFile:    true,
				SourceDataNode: string(sourceNode),
				Generation:     t.sourceGeneration, // collect existing shards from source generation G
			})
			if copyErr != nil {
				return fmt.Errorf("failed to copy shards %v from %s to %s: %w", needToCopyBits.ShardIds(), sourceNode, targetNode, copyErr)
			}

			// Mount the copied shards
			_, mountErr := client.VolumeEcShardsMount(context.Background(), &volume_server_pb.VolumeEcShardsMountRequest{
				VolumeId:   t.volumeID,
				Collection: t.collection,
				ShardIds:   needToCopyBits.ToUint32Slice(),
				Generation: t.sourceGeneration, // mount collected shards from source generation G
			})
			if mountErr != nil {
				return fmt.Errorf("failed to mount shards %v on %s: %w", needToCopyBits.ShardIds(), targetNode, mountErr)
			}

			return nil
		})

		if err != nil {
			return "", err
		}

		existingEcIndexBits = existingEcIndexBits.Plus(needToCopyBits)
	}

	return targetNode, nil
}

// decodeEcShardsToVolume decodes EC shards into a normal volume, automatically skipping deleted entries
func (t *EcVacuumTask) decodeEcShardsToVolume(targetNode pb.ServerAddress) error {
	t.LogInfo("Decoding EC shards to normal volume", map[string]interface{}{
		"volume_id": t.volumeID,
		"target":    targetNode,
	})

	return operation.WithVolumeServerClient(false, targetNode, t.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		_, err := client.VolumeEcShardsToVolume(context.Background(), &volume_server_pb.VolumeEcShardsToVolumeRequest{
			VolumeId:   t.volumeID,
			Collection: t.collection,
		})
		return err
	})
}

// encodeVolumeToEcShards re-encodes the cleaned volume into new EC shards
func (t *EcVacuumTask) encodeVolumeToEcShards(targetNode pb.ServerAddress) error {
	t.LogInfo("Encoding cleaned volume to EC shards", map[string]interface{}{
		"volume_id": t.volumeID,
		"target":    targetNode,
	})

	return operation.WithVolumeServerClient(false, targetNode, t.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		_, err := client.VolumeEcShardsGenerate(context.Background(), &volume_server_pb.VolumeEcShardsGenerateRequest{
			VolumeId:   t.volumeID,
			Collection: t.collection,
			Generation: t.targetGeneration, // generate new EC shards as G+1
		})
		return err
	})
}

// distributeNewEcShards distributes the new EC shards across the cluster
func (t *EcVacuumTask) distributeNewEcShards(sourceNode pb.ServerAddress) error {
	t.LogInfo("Distributing new EC shards", map[string]interface{}{
		"volume_id":         t.volumeID,
		"source":            sourceNode,
		"target_generation": t.targetGeneration,
	})

	// For simplicity, we'll distribute to the same nodes as before
	// In a real implementation, you might want to use topology info for better placement

	// Create bit pattern for all shards (0-13)
	allShardBits := erasure_coding.ShardBits(0)
	for i := 0; i < erasure_coding.TotalShardsCount; i++ {
		allShardBits = allShardBits.AddShardId(erasure_coding.ShardId(i))
	}

	for targetNode, originalShardBits := range t.sourceNodes {
		if targetNode == sourceNode {
			continue // Skip source node
		}

		// Distribute the same shards that were originally on this target
		needToDistributeBits := originalShardBits
		if needToDistributeBits.ShardIdCount() == 0 {
			continue
		}

		err := operation.WithVolumeServerClient(false, targetNode, t.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			t.LogInfo("Copying new EC shards", map[string]interface{}{
				"volume_id": t.volumeID,
				"shard_ids": needToDistributeBits.ShardIds(),
				"from":      sourceNode,
				"to":        targetNode,
			})

			_, copyErr := client.VolumeEcShardsCopy(context.Background(), &volume_server_pb.VolumeEcShardsCopyRequest{
				VolumeId:       t.volumeID,
				Collection:     t.collection,
				ShardIds:       needToDistributeBits.ToUint32Slice(),
				CopyEcxFile:    true,
				CopyEcjFile:    true,
				CopyVifFile:    true,
				SourceDataNode: string(sourceNode),
				Generation:     t.targetGeneration, // copy new EC shards as G+1
			})
			if copyErr != nil {
				return fmt.Errorf("failed to copy new shards %v from %s to %s: %w", needToDistributeBits.ShardIds(), sourceNode, targetNode, copyErr)
			}

			// Mount the new shards
			_, mountErr := client.VolumeEcShardsMount(context.Background(), &volume_server_pb.VolumeEcShardsMountRequest{
				VolumeId:   t.volumeID,
				Collection: t.collection,
				ShardIds:   needToDistributeBits.ToUint32Slice(),
				Generation: t.targetGeneration, // mount new EC shards as G+1
			})
			if mountErr != nil {
				return fmt.Errorf("failed to mount new shards %v on %s: %w", needToDistributeBits.ShardIds(), targetNode, mountErr)
			}

			return nil
		})

		if err != nil {
			return err
		}
	}

	return nil
}

// cleanupOldEcShards removes the original volume after successful vacuum
func (t *EcVacuumTask) cleanupOldEcShards() error {
	t.LogInfo("Cleaning up original volume", map[string]interface{}{
		"volume_id": t.volumeID,
	})

	// Remove the original normal volume from the source node
	for targetNode := range t.sourceNodes {
		err := operation.WithVolumeServerClient(false, targetNode, t.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			_, err := client.VolumeDelete(context.Background(), &volume_server_pb.VolumeDeleteRequest{
				VolumeId: t.volumeID,
			})
			// Ignore errors if volume doesn't exist
			if err != nil {
				t.LogInfo("Volume delete completed or volume not found", map[string]interface{}{
					"volume_id": t.volumeID,
					"node":      targetNode,
					"note":      "This is normal if volume was already cleaned up",
				})
			}
			return nil
		})

		if err != nil {
			return err
		}
		break // Only need to delete from one node
	}

	return nil
}

// cleanup removes temporary files and directories
func (t *EcVacuumTask) cleanup() {
	if t.tempDir != "" {
		if err := os.RemoveAll(t.tempDir); err != nil {
			t.LogWarning("Failed to remove temporary directory", map[string]interface{}{
				"temp_dir": t.tempDir,
				"error":    err.Error(),
			})
		} else {
			t.LogInfo("Cleaned up temporary directory", map[string]interface{}{
				"temp_dir": t.tempDir,
			})
		}
	}
}

// GetVolumeID returns the volume ID being processed
func (t *EcVacuumTask) GetVolumeID() uint32 {
	return t.volumeID
}

// GetCollection returns the collection name
func (t *EcVacuumTask) GetCollection() string {
	return t.collection
}

// SetGrpcDialOption sets the GRPC dial option for volume server communication
func (t *EcVacuumTask) SetGrpcDialOption(option grpc.DialOption) {
	t.grpcDialOption = option
}
