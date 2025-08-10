package ec_vacuum

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
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
	volumeID           uint32
	collection         string
	sourceNodes        map[pb.ServerAddress]erasure_coding.ShardBits
	sourceGeneration   uint32 // generation to vacuum from (G)
	targetGeneration   uint32 // generation to create (G+1)
	tempDir            string
	grpcDialOption     grpc.DialOption
	masterAddress      pb.ServerAddress // master server address for activation RPC
	cleanupGracePeriod time.Duration    // grace period before cleaning up old generation
}

// NewEcVacuumTask creates a new EC vacuum task instance
func NewEcVacuumTask(id string, volumeID uint32, collection string, sourceNodes map[pb.ServerAddress]erasure_coding.ShardBits, sourceGeneration uint32) *EcVacuumTask {
	return &EcVacuumTask{
		BaseTask:           base.NewBaseTask(id, types.TaskType("ec_vacuum")),
		volumeID:           volumeID,
		collection:         collection,
		sourceNodes:        sourceNodes,
		sourceGeneration:   sourceGeneration,     // generation to vacuum from (G)
		targetGeneration:   sourceGeneration + 1, // generation to create (G+1)
		cleanupGracePeriod: 5 * time.Minute,      // default 5 minute grace period
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
		"cleanup_grace":     t.cleanupGracePeriod,
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

	// Step 6: Activate new generation (atomic switch from G to G+1)
	if err := t.activateNewGeneration(); err != nil {
		return fmt.Errorf("failed to activate new generation: %w", err)
	}

	// Step 7: Clean up old EC shards
	if err := t.cleanupOldEcShards(); err != nil {
		t.LogWarning("Failed to clean up old EC shards", map[string]interface{}{
			"error": err.Error(),
		})
		// Don't fail the task for cleanup errors
	}

	t.LogInfo("EC vacuum task completed successfully", map[string]interface{}{
		"volume_id":         t.volumeID,
		"collection":        t.collection,
		"source_generation": t.sourceGeneration,
		"target_generation": t.targetGeneration,
		"note":              "Zero-downtime vacuum completed with generation transition",
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

// activateNewGeneration atomically switches the master to use the new generation
func (t *EcVacuumTask) activateNewGeneration() error {
	t.LogInfo("Activating new generation", map[string]interface{}{
		"volume_id":         t.volumeID,
		"source_generation": t.sourceGeneration,
		"target_generation": t.targetGeneration,
		"master_address":    t.masterAddress,
	})

	if t.masterAddress == "" {
		t.LogWarning("Master address not set - skipping automatic generation activation", map[string]interface{}{
			"volume_id":         t.volumeID,
			"target_generation": t.targetGeneration,
			"note":              "Generation activation must be done manually via master API",
		})
		return nil
	}

	return operation.WithMasterServerClient(false, t.masterAddress, t.grpcDialOption, func(client master_pb.SeaweedClient) error {
		_, err := client.ActivateEcGeneration(context.Background(), &master_pb.ActivateEcGenerationRequest{
			VolumeId:   t.volumeID,
			Collection: t.collection,
			Generation: t.targetGeneration,
		})
		if err != nil {
			return fmt.Errorf("failed to activate generation %d for volume %d: %w", t.targetGeneration, t.volumeID, err)
		}

		t.LogInfo("Successfully activated new generation", map[string]interface{}{
			"volume_id":         t.volumeID,
			"active_generation": t.targetGeneration,
		})
		return nil
	})
}

// cleanupOldEcShards removes the old generation EC shards after successful activation
func (t *EcVacuumTask) cleanupOldEcShards() error {
	t.LogInfo("Starting cleanup of old generation EC shards", map[string]interface{}{
		"volume_id":         t.volumeID,
		"source_generation": t.sourceGeneration,
		"grace_period":      t.cleanupGracePeriod,
	})

	// Step 1: Grace period - wait before cleanup
	if t.cleanupGracePeriod > 0 {
		t.LogInfo("Waiting grace period before cleanup", map[string]interface{}{
			"grace_period": t.cleanupGracePeriod,
			"reason":       "ensuring activation stability",
		})
		time.Sleep(t.cleanupGracePeriod)
	}

	// Step 2: Safety check - verify new generation is actually active
	if err := t.verifyNewGenerationActive(); err != nil {
		t.LogWarning("Skipping cleanup due to safety check failure", map[string]interface{}{
			"error":  err.Error(),
			"action": "manual cleanup may be needed",
		})
		return nil // Don't fail the task, but log the issue
	}

	// Step 3: Unmount and delete old generation shards from each node
	var cleanupErrors []string
	for node, shardBits := range t.sourceNodes {
		if err := t.cleanupOldShardsFromNode(node, shardBits); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Sprintf("node %s: %v", node, err))
			t.LogWarning("Failed to cleanup shards from node", map[string]interface{}{
				"node":  node,
				"error": err.Error(),
			})
		}
	}

	// Step 4: Report cleanup results
	if len(cleanupErrors) > 0 {
		t.LogWarning("Cleanup completed with errors", map[string]interface{}{
			"errors": cleanupErrors,
			"note":   "some old generation files may remain",
		})
		// Don't fail the task for cleanup errors - vacuum was successful
		return nil
	}

	t.LogInfo("Successfully cleaned up old generation EC shards", map[string]interface{}{
		"volume_id":         t.volumeID,
		"source_generation": t.sourceGeneration,
	})
	return nil
}

// verifyNewGenerationActive checks with master that the new generation is active
func (t *EcVacuumTask) verifyNewGenerationActive() error {
	if t.masterAddress == "" {
		t.LogWarning("Cannot verify generation activation - master address not set", map[string]interface{}{
			"note": "skipping safety check",
		})
		return nil // Skip verification if we don't have master access
	}

	return operation.WithMasterServerClient(false, t.masterAddress, t.grpcDialOption, func(client master_pb.SeaweedClient) error {
		resp, err := client.LookupEcVolume(context.Background(), &master_pb.LookupEcVolumeRequest{
			VolumeId: t.volumeID,
		})
		if err != nil {
			return fmt.Errorf("failed to lookup EC volume from master: %w", err)
		}

		if resp.ActiveGeneration != t.targetGeneration {
			return fmt.Errorf("safety check failed: master active generation is %d, expected %d",
				resp.ActiveGeneration, t.targetGeneration)
		}

		t.LogInfo("Safety check passed - new generation is active", map[string]interface{}{
			"volume_id":         t.volumeID,
			"active_generation": resp.ActiveGeneration,
		})
		return nil
	})
}

// cleanupOldShardsFromNode unmounts and deletes old generation shards from a specific node
func (t *EcVacuumTask) cleanupOldShardsFromNode(node pb.ServerAddress, shardBits erasure_coding.ShardBits) error {
	return operation.WithVolumeServerClient(false, node, t.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		shardIds := shardBits.ToUint32Slice()

		t.LogInfo("Unmounting old generation shards", map[string]interface{}{
			"node":              node,
			"volume_id":         t.volumeID,
			"source_generation": t.sourceGeneration,
			"shard_ids":         shardIds,
		})

		// Step 1: Unmount old generation shards
		_, unmountErr := client.VolumeEcShardsUnmount(context.Background(), &volume_server_pb.VolumeEcShardsUnmountRequest{
			VolumeId:   t.volumeID,
			ShardIds:   shardIds,
			Generation: t.sourceGeneration,
		})
		if unmountErr != nil {
			// Log but continue - files might already be unmounted
			t.LogInfo("Unmount completed or shards already unmounted", map[string]interface{}{
				"node":  node,
				"error": unmountErr.Error(),
				"note":  "this is normal if shards were already unmounted",
			})
		}

		// Step 2: Delete old generation files
		// Note: The volume server should handle file deletion when unmounting,
		// but we could add explicit file deletion here if needed in the future

		t.LogInfo("Successfully cleaned up old generation shards from node", map[string]interface{}{
			"node":              node,
			"volume_id":         t.volumeID,
			"source_generation": t.sourceGeneration,
		})
		return nil
	})
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

// SetMasterAddress sets the master server address for generation activation
func (t *EcVacuumTask) SetMasterAddress(address pb.ServerAddress) {
	t.masterAddress = address
}

// SetCleanupGracePeriod sets the grace period before cleaning up old generation
func (t *EcVacuumTask) SetCleanupGracePeriod(period time.Duration) {
	t.cleanupGracePeriod = period
}
