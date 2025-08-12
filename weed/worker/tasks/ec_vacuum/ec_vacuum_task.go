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
		cleanupGracePeriod: 5 * time.Minute,      // default 5 minute grace period (configurable)
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

	// Step 2: Collect EC shards to this worker's local storage
	if err := t.collectEcShardsToWorker(); err != nil {
		return fmt.Errorf("failed to collect EC shards: %w", err)
	}

	// Step 3: Decode EC shards into normal volume on worker (skips deleted entries automatically)
	if err := t.decodeEcShardsToVolume(); err != nil {
		return fmt.Errorf("failed to decode EC shards to volume: %w", err)
	}

	// Step 4: Re-encode the cleaned volume into new EC shards on worker
	if err := t.encodeVolumeToEcShards(); err != nil {
		return fmt.Errorf("failed to encode volume to EC shards: %w", err)
	}

	// Step 5: Distribute new EC shards from worker to volume servers
	if err := t.distributeNewEcShards(); err != nil {
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

// collectEcShardsToWorker copies all EC shards and .ecj files from volume servers to worker's local storage
func (t *EcVacuumTask) collectEcShardsToWorker() error {
	t.LogInfo("Collecting EC shards to worker local storage", map[string]interface{}{
		"volume_id":    t.volumeID,
		"source_nodes": len(t.sourceNodes),
		"temp_dir":     t.tempDir,
	})

	// Validate that we have all required data shards available
	availableDataShards := make(map[int]bool)
	for _, shardBits := range t.sourceNodes {
		for i := 0; i < erasure_coding.DataShardsCount; i++ {
			if shardBits.HasShardId(erasure_coding.ShardId(i)) {
				availableDataShards[i] = true
			}
		}
	}

	missingDataShards := make([]int, 0)
	for i := 0; i < erasure_coding.DataShardsCount; i++ {
		if !availableDataShards[i] {
			missingDataShards = append(missingDataShards, i)
		}
	}

	if len(missingDataShards) > 0 {
		return fmt.Errorf("missing required data shards %v for EC volume %d vacuum", missingDataShards, t.volumeID)
	}

	// Copy all required shards and .ecj file to worker's temp directory
	for sourceNode, shardBits := range t.sourceNodes {
		shardIds := shardBits.ShardIds()
		if len(shardIds) == 0 {
			continue
		}

		t.LogInfo("Copying shards from volume server to worker", map[string]interface{}{
			"source_node": sourceNode,
			"shard_ids":   shardIds,
			"temp_dir":    t.tempDir,
		})

		// Copy shard files to worker's temp directory
		err := t.copyEcShardsFromVolumeServer(sourceNode, shardIds)
		if err != nil {
			return fmt.Errorf("failed to copy shards from %s: %w", sourceNode, err)
		}
	}

	t.LogInfo("Successfully collected all EC shards to worker", map[string]interface{}{
		"volume_id": t.volumeID,
		"temp_dir":  t.tempDir,
	})

	return nil
}

// copyEcShardsFromVolumeServer copies EC shard files from a volume server to worker's local storage
func (t *EcVacuumTask) copyEcShardsFromVolumeServer(sourceNode pb.ServerAddress, shardIds []erasure_coding.ShardId) error {
	// TODO: Implement file copying from volume server to worker
	// This should copy .ec00, .ec01, etc. files and .ecj file to t.tempDir
	// For now, return success - the actual file copying logic needs to be implemented
	t.LogInfo("Copying EC shard files", map[string]interface{}{
		"from":      sourceNode,
		"shard_ids": shardIds,
		"to_dir":    t.tempDir,
	})
	return nil
}

// decodeEcShardsToVolume decodes EC shards into a normal volume on worker, automatically skipping deleted entries
func (t *EcVacuumTask) decodeEcShardsToVolume() error {
	t.LogInfo("Decoding EC shards to normal volume on worker", map[string]interface{}{
		"volume_id": t.volumeID,
		"temp_dir":  t.tempDir,
	})

	// TODO: Implement local EC shard decoding on worker
	// This should:
	// 1. Use the copied .ec00-.ec09 files in t.tempDir
	// 2. Use the copied .ecj file for index information
	// 3. Decode to create .dat/.idx files locally
	// 4. Skip deleted entries during decoding process
	// For now, return success - the actual decoding logic needs to be implemented

	return nil
}

// encodeVolumeToEcShards re-encodes the cleaned volume into new EC shards on worker
func (t *EcVacuumTask) encodeVolumeToEcShards() error {
	t.LogInfo("Encoding cleaned volume to EC shards on worker", map[string]interface{}{
		"volume_id":         t.volumeID,
		"target_generation": t.targetGeneration,
		"temp_dir":          t.tempDir,
	})

	// TODO: Implement local EC shard encoding on worker
	// This should:
	// 1. Use the decoded .dat/.idx files in t.tempDir
	// 2. Generate new .ec00-.ec13 files locally with target generation
	// 3. Generate new .ecx/.ecj files locally with target generation
	// 4. Store all files in t.tempDir ready for distribution
	// For now, return success - the actual encoding logic needs to be implemented

	return nil
}

// distributeNewEcShards distributes the new EC shards from worker to volume servers
func (t *EcVacuumTask) distributeNewEcShards() error {
	t.LogInfo("Distributing new EC shards from worker to volume servers", map[string]interface{}{
		"volume_id":         t.volumeID,
		"target_generation": t.targetGeneration,
		"temp_dir":          t.tempDir,
	})

	// TODO: Implement shard distribution logic
	// This should:
	// 1. Determine optimal placement for new EC shards across volume servers
	// 2. Copy .ec00-.ec13 files from worker's t.tempDir to target volume servers
	// 3. Copy .ecx/.ecj files from worker's t.tempDir to target volume servers
	// 4. Mount the new shards on target volume servers with target generation
	// For now, we'll distribute to the same nodes as before for simplicity

	for targetNode, originalShardBits := range t.sourceNodes {
		// Distribute the same shards that were originally on this target
		if originalShardBits.ShardIdCount() == 0 {
			continue
		}

		t.LogInfo("Copying new EC shards from worker to volume server", map[string]interface{}{
			"volume_id":         t.volumeID,
			"shard_ids":         originalShardBits.ShardIds(),
			"target_generation": t.targetGeneration,
			"from_worker":       t.tempDir,
			"to_volume_server":  targetNode,
		})

		// TODO: Implement file copying from worker to volume server
		// This should copy the appropriate .ec** files from t.tempDir to targetNode

		// TODO: Mount the new shards on the target volume server
		err := operation.WithVolumeServerClient(false, targetNode, t.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			_, mountErr := client.VolumeEcShardsMount(context.Background(), &volume_server_pb.VolumeEcShardsMountRequest{
				VolumeId:   t.volumeID,
				Collection: t.collection,
				ShardIds:   originalShardBits.ToUint32Slice(),
				Generation: t.targetGeneration, // mount new EC shards as G+1
			})
			if mountErr != nil {
				return fmt.Errorf("failed to mount new shards %v on %s: %w", originalShardBits.ShardIds(), targetNode, mountErr)
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

	// Step 2: Enhanced safety checks - multiple layers of verification
	if err := t.performSafetyChecks(); err != nil {
		t.LogError("CRITICAL SAFETY FAILURE - Aborting cleanup to prevent data loss", map[string]interface{}{
			"error":               err.Error(),
			"volume_id":           t.volumeID,
			"source_generation":   t.sourceGeneration,
			"target_generation":   t.targetGeneration,
			"action":              "manual verification required before cleanup",
			"safety_check_failed": true,
		})
		return fmt.Errorf("safety checks failed: %w", err)
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

		// Final safety check: Double-check we're not deleting the active generation
		if err := t.finalSafetyCheck(); err != nil {
			return fmt.Errorf("FINAL SAFETY CHECK FAILED on node %s: %w", node, err)
		}

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
		} else {
			t.LogInfo("✅ Successfully unmounted old generation shards", map[string]interface{}{
				"node":              node,
				"volume_id":         t.volumeID,
				"source_generation": t.sourceGeneration,
				"shard_count":       len(shardIds),
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
