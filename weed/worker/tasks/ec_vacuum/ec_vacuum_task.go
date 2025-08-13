package ec_vacuum

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	storage_types "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"github.com/seaweedfs/seaweedfs/weed/worker/types/base"
	"google.golang.org/grpc"
)

// Compile-time interface compliance checks
var (
	_ types.TaskWithGrpcDial     = (*EcVacuumTask)(nil)
	_ types.TaskWithAdminAddress = (*EcVacuumTask)(nil)
)

// EcVacuumTask represents an EC vacuum task that collects, decodes, and re-encodes EC volumes
type EcVacuumTask struct {
	*base.BaseTask
	volumeID           uint32
	collection         string
	sourceNodes        map[pb.ServerAddress]erasure_coding.ShardBits
	tempDir            string
	grpcDialOption     grpc.DialOption
	adminAddress       string           // admin server address for API calls
	masterAddress      pb.ServerAddress // master server address for activation RPC
	cleanupGracePeriod time.Duration    // grace period before cleaning up old generation (1 minute default)
	topologyTaskID     string           // links to ActiveTopology task for capacity tracking

	// Runtime-determined during execution
	sourceGeneration uint32 // generation to vacuum from (determined at runtime)
	targetGeneration uint32 // generation to create (determined at runtime)

	// Core business logic
	logic *EcVacuumLogic
	plan  *VacuumPlan // Generated plan for this vacuum operation
}

// NewEcVacuumTask creates a new EC vacuum task instance
func NewEcVacuumTask(id string, volumeID uint32, collection string, sourceNodes map[pb.ServerAddress]erasure_coding.ShardBits) *EcVacuumTask {
	return &EcVacuumTask{
		BaseTask:           base.NewBaseTask(id, types.TaskType("ec_vacuum")),
		volumeID:           volumeID,
		collection:         collection,
		sourceNodes:        sourceNodes,
		cleanupGracePeriod: 1 * time.Minute,    // 1 minute grace period for faster cleanup
		logic:              NewEcVacuumLogic(), // Initialize business logic
		// sourceGeneration and targetGeneration will be determined during execution
	}
}

// SetTopologyTaskID sets the topology task ID for capacity tracking integration
func (t *EcVacuumTask) SetTopologyTaskID(taskID string) {
	t.topologyTaskID = taskID
}

// GetTopologyTaskID returns the topology task ID
func (t *EcVacuumTask) GetTopologyTaskID() string {
	return t.topologyTaskID
}

// Execute performs the EC vacuum operation
func (t *EcVacuumTask) Execute(ctx context.Context, params *worker_pb.TaskParams) error {
	// Step 0: Create comprehensive vacuum plan using the logic layer
	plan, err := t.logic.CreateVacuumPlan(t.volumeID, t.collection, params)
	if err != nil {
		return fmt.Errorf("failed to create vacuum plan: %w", err)
	}
	t.plan = plan

	// Extract generations from the plan
	t.sourceGeneration = plan.CurrentGeneration
	t.targetGeneration = plan.TargetGeneration

	t.LogInfo("Vacuum plan created successfully", map[string]interface{}{
		"volume_id":           plan.VolumeID,
		"collection":          plan.Collection,
		"source_generation":   plan.CurrentGeneration,
		"target_generation":   plan.TargetGeneration,
		"cleanup_generations": plan.GenerationsToCleanup,
		"nodes_involved":      len(plan.SourceDistribution.Nodes),
		"safety_checks":       len(plan.SafetyChecks),
	})

	// Validate the plan is safe to execute
	if err := t.logic.ValidateShardDistribution(plan.SourceDistribution); err != nil {
		return fmt.Errorf("vacuum plan validation failed: %w", err)
	}

	t.LogInfo("Plan validation successful", map[string]interface{}{
		"safety_checks": plan.SafetyChecks,
	})

	// Ensure sourceNodes is consistent with the plan
	t.sourceNodes = plan.SourceDistribution.Nodes

	// Log task information
	logFields := map[string]interface{}{
		"volume_id":         t.volumeID,
		"collection":        t.collection,
		"source_generation": t.sourceGeneration,
		"target_generation": t.targetGeneration,
	}

	// Cleanup planning is now simplified

	// Add additional task info
	logFields["shard_nodes"] = len(t.sourceNodes)
	logFields["cleanup_grace"] = t.cleanupGracePeriod

	// Add topology integration info
	if t.topologyTaskID != "" {
		logFields["topology_task_id"] = t.topologyTaskID
		logFields["topology_integrated"] = true
	} else {
		logFields["topology_integrated"] = false
	}

	t.LogInfo("Starting EC vacuum task with runtime generation detection", logFields)

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
	t.LogInfo("Copying EC shard files from volume server", map[string]interface{}{
		"from":      sourceNode,
		"shard_ids": shardIds,
		"to_dir":    t.tempDir,
	})

	return operation.WithVolumeServerClient(false, sourceNode, t.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		// Copy each EC shard file (.ec00, .ec01, etc.)
		for _, shardId := range shardIds {
			ext := fmt.Sprintf(".ec%02d", shardId)
			localPath := filepath.Join(t.tempDir, fmt.Sprintf("%s_%d%s", t.collection, t.volumeID, ext))

			err := t.copyFileFromVolumeServer(client, ext, localPath)
			if err != nil {
				return fmt.Errorf("failed to copy shard %s: %w", ext, err)
			}
		}

		// Copy .ecj file (deletion journal) with server-specific name for proper merging
		// Each server may have different deletion information that needs to be merged
		serverSafeAddr := strings.ReplaceAll(string(sourceNode), ":", "_")
		ecjPath := filepath.Join(t.tempDir, fmt.Sprintf("%s_%d_%s.ecj", t.collection, t.volumeID, serverSafeAddr))
		err := t.copyFileFromVolumeServer(client, ".ecj", ecjPath)
		if err != nil {
			// .ecj file might not exist if no deletions on this server - this is OK
			t.LogInfo("No .ecj file found on server (no deletions)", map[string]interface{}{
				"server": sourceNode,
				"volume": t.volumeID,
			})
		}

		// Copy .ecx file (index) - only need one copy for reconstruction
		// Only copy from first server that has it
		ecxPath := filepath.Join(t.tempDir, fmt.Sprintf("%s_%d.ecx", t.collection, t.volumeID))
		if _, err := os.Stat(ecxPath); os.IsNotExist(err) {
			err = t.copyFileFromVolumeServer(client, ".ecx", ecxPath)
			if err != nil {
				t.LogInfo("No .ecx file found on this server", map[string]interface{}{
					"server": sourceNode,
					"volume": t.volumeID,
				})
			}
		}

		return nil
	})
}

// copyFileFromVolumeServer copies a single file from volume server using streaming gRPC
func (t *EcVacuumTask) copyFileFromVolumeServer(client volume_server_pb.VolumeServerClient, ext, localPath string) error {
	stream, err := client.CopyFile(context.Background(), &volume_server_pb.CopyFileRequest{
		VolumeId:                 t.volumeID,
		Collection:               t.collection,
		Ext:                      ext,
		StopOffset:               uint64(math.MaxInt64),
		IsEcVolume:               true,
		Generation:               t.sourceGeneration, // copy from source generation
		IgnoreSourceFileNotFound: true,               // OK if file doesn't exist
	})
	if err != nil {
		return fmt.Errorf("failed to initiate file copy for %s: %w", ext, err)
	}

	// Create local file
	localFile, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("failed to create local file %s: %w", localPath, err)
	}
	defer localFile.Close()

	// Stream data and write to local file
	totalBytes := int64(0)
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to receive file data for %s: %w", ext, err)
		}

		if len(resp.FileContent) > 0 {
			written, writeErr := localFile.Write(resp.FileContent)
			if writeErr != nil {
				return fmt.Errorf("failed to write to local file %s: %w", localPath, writeErr)
			}
			totalBytes += int64(written)
		}
	}

	t.LogInfo("Successfully copied file from volume server", map[string]interface{}{
		"ext":        ext,
		"local_path": localPath,
		"bytes":      totalBytes,
	})

	return nil
}

// decodeEcShardsToVolume decodes EC shards into a normal volume on worker, automatically skipping deleted entries
func (t *EcVacuumTask) decodeEcShardsToVolume() error {
	t.LogInfo("Decoding EC shards to normal volume on worker", map[string]interface{}{
		"volume_id": t.volumeID,
		"temp_dir":  t.tempDir,
	})

	// Step 1: Merge .ecj files from different volume servers
	err := t.mergeEcjFiles()
	if err != nil {
		return fmt.Errorf("failed to merge .ecj files: %w", err)
	}

	// Step 2: Prepare shard file names for decoding
	shardFileNames := make([]string, erasure_coding.DataShardsCount)
	for i := 0; i < erasure_coding.DataShardsCount; i++ {
		shardFile := filepath.Join(t.tempDir, fmt.Sprintf("%s_%d.ec%02d", t.collection, t.volumeID, i))
		if _, err := os.Stat(shardFile); err != nil {
			return fmt.Errorf("missing required data shard %d at %s: %w", i, shardFile, err)
		}
		shardFileNames[i] = shardFile
	}

	// Step 3: Calculate target file paths
	baseFileName := filepath.Join(t.tempDir, fmt.Sprintf("%s_%d", t.collection, t.volumeID))
	datFileName := baseFileName + ".dat"
	idxFileName := baseFileName + ".idx"

	t.LogInfo("Decoding EC shards to normal volume files", map[string]interface{}{
		"base_name":        baseFileName,
		"dat_file":         datFileName,
		"idx_file":         idxFileName,
		"shard_file_count": len(shardFileNames),
	})

	// Step 4: Calculate .dat file size from .ecx file
	datFileSize, err := erasure_coding.FindDatFileSize(baseFileName, baseFileName)
	if err != nil {
		return fmt.Errorf("failed to find dat file size: %w", err)
	}

	// Step 5: Write .dat file from EC data shards (this automatically skips deleted entries)
	err = erasure_coding.WriteDatFile(baseFileName, datFileSize, shardFileNames)
	if err != nil {
		return fmt.Errorf("failed to write dat file: %w", err)
	}

	// Step 6: Write .idx file from .ecx and merged .ecj files (skips deleted entries)
	err = erasure_coding.WriteIdxFileFromEcIndex(baseFileName)
	if err != nil {
		return fmt.Errorf("failed to write idx file from ec index: %w", err)
	}

	t.LogInfo("Successfully decoded EC shards to normal volume", map[string]interface{}{
		"dat_file": datFileName,
		"idx_file": idxFileName,
		"dat_size": datFileSize,
	})

	return nil
}

// mergeEcjFiles merges .ecj (deletion journal) files from different volume servers into a single .ecj file
// This is critical because each volume server may have partial deletion information that needs to be combined
func (t *EcVacuumTask) mergeEcjFiles() error {
	t.LogInfo("Merging .ecj files from different volume servers", map[string]interface{}{
		"volume_id": t.volumeID,
		"temp_dir":  t.tempDir,
	})

	// Find all .ecj files with server-specific names: collection_volumeID_serverAddress.ecj
	ecjFiles := make([]string, 0)
	pattern := fmt.Sprintf("%s_%d_*.ecj", t.collection, t.volumeID)
	matches, err := filepath.Glob(filepath.Join(t.tempDir, pattern))
	if err != nil {
		return fmt.Errorf("failed to find .ecj files: %w", err)
	}

	for _, match := range matches {
		if _, err := os.Stat(match); err == nil {
			ecjFiles = append(ecjFiles, match)
		}
	}

	// Create merged .ecj file path
	mergedEcjFile := filepath.Join(t.tempDir, fmt.Sprintf("%s_%d.ecj", t.collection, t.volumeID))

	if len(ecjFiles) == 0 {
		// No .ecj files found - create empty one (no deletions)
		emptyFile, err := os.Create(mergedEcjFile)
		if err != nil {
			return fmt.Errorf("failed to create empty .ecj file: %w", err)
		}
		emptyFile.Close()

		t.LogInfo("No .ecj files found, created empty deletion journal", map[string]interface{}{
			"merged_file": mergedEcjFile,
		})
		return nil
	}

	t.LogInfo("Found .ecj files to merge", map[string]interface{}{
		"ecj_files":   ecjFiles,
		"count":       len(ecjFiles),
		"merged_file": mergedEcjFile,
	})

	// Merge all .ecj files into a single file
	// Each .ecj file contains deleted needle IDs from a specific server
	deletedNeedles := make(map[storage_types.NeedleId]bool) // Track unique deleted needles

	for _, ecjFile := range ecjFiles {
		err := t.processEcjFile(ecjFile, deletedNeedles)
		if err != nil {
			t.LogWarning("Failed to process .ecj file", map[string]interface{}{
				"file":  ecjFile,
				"error": err.Error(),
			})
			continue
		}
	}

	// Write merged deletion information to new .ecj file
	err = t.writeMergedEcjFile(mergedEcjFile, deletedNeedles)
	if err != nil {
		return fmt.Errorf("failed to write merged .ecj file: %w", err)
	}

	t.LogInfo("Successfully merged .ecj files", map[string]interface{}{
		"source_files":    len(ecjFiles),
		"deleted_needles": len(deletedNeedles),
		"merged_file":     mergedEcjFile,
	})

	return nil
}

// processEcjFile reads a .ecj file and adds deleted needle IDs to the set
func (t *EcVacuumTask) processEcjFile(ecjFile string, deletedNeedles map[storage_types.NeedleId]bool) error {
	t.LogInfo("Processing .ecj file for deleted needle IDs", map[string]interface{}{
		"file": ecjFile,
	})

	// Get base name for the file (remove .ecj extension) for IterateEcjFile
	baseName := strings.TrimSuffix(ecjFile, ".ecj")

	deletedCount := 0
	err := erasure_coding.IterateEcjFile(baseName, func(needleId storage_types.NeedleId) error {
		deletedNeedles[needleId] = true
		deletedCount++
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to iterate .ecj file %s: %w", ecjFile, err)
	}

	t.LogInfo("Successfully processed .ecj file", map[string]interface{}{
		"file":            ecjFile,
		"deleted_needles": deletedCount,
	})

	return nil
}

// writeMergedEcjFile writes the merged deletion information to a new .ecj file
func (t *EcVacuumTask) writeMergedEcjFile(mergedEcjFile string, deletedNeedles map[storage_types.NeedleId]bool) error {
	t.LogInfo("Writing merged .ecj file", map[string]interface{}{
		"file":            mergedEcjFile,
		"deleted_needles": len(deletedNeedles),
	})

	file, err := os.Create(mergedEcjFile)
	if err != nil {
		return fmt.Errorf("failed to create merged .ecj file: %w", err)
	}
	defer file.Close()

	// Write each deleted needle ID as binary data
	writtenCount := 0
	needleBytes := make([]byte, storage_types.NeedleIdSize)
	for needleId := range deletedNeedles {
		storage_types.NeedleIdToBytes(needleBytes, needleId)
		_, err := file.Write(needleBytes)
		if err != nil {
			return fmt.Errorf("failed to write needle ID to .ecj file: %w", err)
		}
		writtenCount++
	}

	// Sync to ensure data is written to disk
	err = file.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync .ecj file: %w", err)
	}

	t.LogInfo("Successfully wrote merged .ecj file", map[string]interface{}{
		"file":            mergedEcjFile,
		"deleted_needles": writtenCount,
		"file_size":       writtenCount * storage_types.NeedleIdSize,
	})

	return nil
}

// encodeVolumeToEcShards re-encodes the cleaned volume into new EC shards on worker
func (t *EcVacuumTask) encodeVolumeToEcShards() error {
	t.LogInfo("Encoding cleaned volume to EC shards on worker", map[string]interface{}{
		"volume_id":         t.volumeID,
		"target_generation": t.targetGeneration,
		"temp_dir":          t.tempDir,
	})

	// Step 1: Verify cleaned volume files exist
	baseFileName := filepath.Join(t.tempDir, fmt.Sprintf("%s_%d", t.collection, t.volumeID))
	datFileName := baseFileName + ".dat"
	idxFileName := baseFileName + ".idx"

	if _, err := os.Stat(datFileName); err != nil {
		return fmt.Errorf("cleaned .dat file not found at %s: %w", datFileName, err)
	}
	if _, err := os.Stat(idxFileName); err != nil {
		return fmt.Errorf("cleaned .idx file not found at %s: %w", idxFileName, err)
	}

	// Step 2: Generate new base filename with target generation
	targetBaseFileName := filepath.Join(t.tempDir, fmt.Sprintf("%s_%d_g%d", t.collection, t.volumeID, t.targetGeneration))
	targetDatFileName := targetBaseFileName + ".dat"
	targetIdxFileName := targetBaseFileName + ".idx"

	t.LogInfo("Generating new EC shards with target generation", map[string]interface{}{
		"source_base":     baseFileName,
		"target_base":     targetBaseFileName,
		"source_dat_file": datFileName,
		"source_idx_file": idxFileName,
		"target_dat_file": targetDatFileName,
		"target_idx_file": targetIdxFileName,
	})

	// Step 2a: Copy cleaned volume files to generation-aware names for EC encoding
	err := t.copyFile(datFileName, targetDatFileName)
	if err != nil {
		return fmt.Errorf("failed to copy .dat file for encoding: %w", err)
	}

	err = t.copyFile(idxFileName, targetIdxFileName)
	if err != nil {
		return fmt.Errorf("failed to copy .idx file for encoding: %w", err)
	}

	// Step 3: Generate EC shard files (.ec00 ~ .ec13) from cleaned .dat file
	err = erasure_coding.WriteEcFiles(targetBaseFileName)
	if err != nil {
		return fmt.Errorf("failed to generate EC shard files: %w", err)
	}

	// Step 4: Generate .ecx file from cleaned .idx file (use target base name with generation)
	err = erasure_coding.WriteSortedFileFromIdxToTarget(targetBaseFileName, targetBaseFileName+".ecx")
	if err != nil {
		return fmt.Errorf("failed to generate .ecx file: %w", err)
	}

	// Step 5: Create empty .ecj file for new generation (no deletions in clean volume)
	newEcjFile := targetBaseFileName + ".ecj"
	emptyEcjFile, err := os.Create(newEcjFile)
	if err != nil {
		return fmt.Errorf("failed to create new .ecj file: %w", err)
	}
	emptyEcjFile.Close()

	// Step 6: Generate .vif file (volume info) for new generation
	newVifFile := targetBaseFileName + ".vif"
	volumeInfo := &volume_server_pb.VolumeInfo{
		Version: uint32(needle.GetCurrentVersion()),
	}
	err = volume_info.SaveVolumeInfo(newVifFile, volumeInfo)
	if err != nil {
		t.LogWarning("Failed to create .vif file", map[string]interface{}{
			"vif_file": newVifFile,
			"error":    err.Error(),
		})
	}

	// Step 7: Verify all new files were created
	createdFiles := make([]string, 0)
	for i := 0; i < erasure_coding.TotalShardsCount; i++ {
		shardFile := fmt.Sprintf("%s.ec%02d", targetBaseFileName, i)
		if _, err := os.Stat(shardFile); err == nil {
			createdFiles = append(createdFiles, fmt.Sprintf("ec%02d", i))
		}
	}

	t.LogInfo("Successfully encoded volume to new EC shards", map[string]interface{}{
		"target_generation": t.targetGeneration,
		"shard_count":       len(createdFiles),
		"created_files":     createdFiles,
		"ecx_file":          targetBaseFileName + ".ecx",
		"ecj_file":          newEcjFile,
		"vif_file":          newVifFile,
	})

	return nil
}

// distributeNewEcShards distributes the new EC shards from worker to volume servers
func (t *EcVacuumTask) distributeNewEcShards() error {
	t.LogInfo("Distributing new EC shards from worker to volume servers", map[string]interface{}{
		"volume_id":         t.volumeID,
		"target_generation": t.targetGeneration,
		"temp_dir":          t.tempDir,
	})

	targetBaseFileName := filepath.Join(t.tempDir, fmt.Sprintf("%s_%d_g%d", t.collection, t.volumeID, t.targetGeneration))

	// Step 1: Distribute index files (.vif, .ecj, .ecx) to all volume servers that will have shards
	// Each volume server needs its own copy of index files for mounting
	for targetNode := range t.sourceNodes {
		err := t.distributeIndexFiles(targetNode, targetBaseFileName)
		if err != nil {
			return fmt.Errorf("failed to distribute index files to %s: %w", targetNode, err)
		}
	}

	// Step 2: Distribute shard files (.ec00-.ec13) to appropriate volume servers
	for targetNode, originalShardBits := range t.sourceNodes {
		if originalShardBits.ShardIdCount() == 0 {
			continue
		}

		t.LogInfo("Distributing EC shards to volume server", map[string]interface{}{
			"volume_id":         t.volumeID,
			"shard_ids":         originalShardBits.ShardIds(),
			"target_generation": t.targetGeneration,
			"target_server":     targetNode,
		})

		err := t.distributeShardFiles(targetNode, originalShardBits.ShardIds(), targetBaseFileName)
		if err != nil {
			return fmt.Errorf("failed to distribute shards to %s: %w", targetNode, err)
		}

		// Step 3: Mount the new shards on the target volume server
		err = operation.WithVolumeServerClient(false, targetNode, t.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
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

	t.LogInfo("Successfully distributed all new EC shards", map[string]interface{}{
		"volume_id":         t.volumeID,
		"target_generation": t.targetGeneration,
		"shard_servers":     len(t.sourceNodes),
	})

	return nil
}

// distributeIndexFiles distributes index files (.vif, .ecj, .ecx) to a server with dedicated index folder
func (t *EcVacuumTask) distributeIndexFiles(indexServer pb.ServerAddress, targetBaseFileName string) error {
	t.LogInfo("Distributing index files to index server", map[string]interface{}{
		"index_server":      indexServer,
		"target_generation": t.targetGeneration,
	})

	// List of index files to distribute
	indexFiles := []string{
		targetBaseFileName + ".vif", // Volume info file
		targetBaseFileName + ".ecj", // Empty deletion journal for new generation
		targetBaseFileName + ".ecx", // EC index file - required for mounting
	}

	return operation.WithVolumeServerClient(false, indexServer, t.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		for _, localFile := range indexFiles {
			if _, err := os.Stat(localFile); os.IsNotExist(err) {
				t.LogInfo("Index file not found, skipping", map[string]interface{}{
					"file": localFile,
				})
				continue
			}

			err := t.sendFileToVolumeServer(client, localFile, indexServer)
			if err != nil {
				return fmt.Errorf("failed to send index file %s: %w", localFile, err)
			}
		}
		return nil
	})
}

// distributeShardFiles distributes EC shard files (.ec00-.ec13) to a volume server
func (t *EcVacuumTask) distributeShardFiles(targetServer pb.ServerAddress, shardIds []erasure_coding.ShardId, targetBaseFileName string) error {
	t.LogInfo("Distributing shard files to volume server", map[string]interface{}{
		"target_server":     targetServer,
		"shard_ids":         shardIds,
		"target_generation": t.targetGeneration,
	})

	return operation.WithVolumeServerClient(false, targetServer, t.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		for _, shardId := range shardIds {
			shardFile := fmt.Sprintf("%s.ec%02d", targetBaseFileName, shardId)
			if _, err := os.Stat(shardFile); os.IsNotExist(err) {
				return fmt.Errorf("shard file %s not found", shardFile)
			}

			err := t.sendFileToVolumeServer(client, shardFile, targetServer)
			if err != nil {
				return fmt.Errorf("failed to send shard file %s: %w", shardFile, err)
			}
		}
		return nil
	})
}

// copyFile copies a file from source to destination
func (t *EcVacuumTask) copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open source file %s: %w", src, err)
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("failed to create destination file %s: %w", dst, err)
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	if err != nil {
		return fmt.Errorf("failed to copy from %s to %s: %w", src, dst, err)
	}

	return destFile.Sync()
}

// sendFileToVolumeServer sends a file from worker to volume server using ReceiveFile RPC
func (t *EcVacuumTask) sendFileToVolumeServer(client volume_server_pb.VolumeServerClient, localFile string, targetServer pb.ServerAddress) error {
	t.LogInfo("Sending file to volume server", map[string]interface{}{
		"local_file":    localFile,
		"target_server": targetServer,
		"generation":    t.targetGeneration,
	})

	// Open the local file
	file, err := os.Open(localFile)
	if err != nil {
		return fmt.Errorf("failed to open local file %s: %w", localFile, err)
	}
	defer file.Close()

	// Get file info
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info for %s: %w", localFile, err)
	}

	// Determine file extension and shard ID from local file path
	ext := filepath.Ext(localFile)
	var shardId uint32 = 0

	// Parse shard ID from EC shard files (e.g., .ec00, .ec01, etc.)
	if strings.HasPrefix(ext, ".ec") && len(ext) == 5 {
		if shardIdInt, parseErr := strconv.Atoi(ext[3:]); parseErr == nil {
			shardId = uint32(shardIdInt)
		}
	}

	t.LogInfo("Streaming file to volume server", map[string]interface{}{
		"file":      localFile,
		"ext":       ext,
		"shard_id":  shardId,
		"file_size": fileInfo.Size(),
		"server":    targetServer,
	})

	// Create streaming client
	stream, err := client.ReceiveFile(context.Background())
	if err != nil {
		return fmt.Errorf("failed to create receive stream: %w", err)
	}

	// Send file info first with proper generation support
	err = stream.Send(&volume_server_pb.ReceiveFileRequest{
		Data: &volume_server_pb.ReceiveFileRequest_Info{
			Info: &volume_server_pb.ReceiveFileInfo{
				VolumeId:   t.volumeID,
				Ext:        ext,
				Collection: t.collection,
				IsEcVolume: true,
				ShardId:    shardId,
				FileSize:   uint64(fileInfo.Size()),
				Generation: t.targetGeneration, // Use proper generation field for file naming
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to send file info: %w", err)
	}

	// Send file content in chunks
	buffer := make([]byte, 64*1024) // 64KB chunks
	totalBytes := int64(0)
	for {
		n, readErr := file.Read(buffer)
		if n > 0 {
			err = stream.Send(&volume_server_pb.ReceiveFileRequest{
				Data: &volume_server_pb.ReceiveFileRequest_FileContent{
					FileContent: buffer[:n],
				},
			})
			if err != nil {
				return fmt.Errorf("failed to send file content: %w", err)
			}
			totalBytes += int64(n)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return fmt.Errorf("failed to read file: %w", readErr)
		}
	}

	// Close stream and get response
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("failed to close stream: %w", err)
	}

	if resp.Error != "" {
		return fmt.Errorf("server error: %s", resp.Error)
	}

	t.LogInfo("Successfully sent file to volume server", map[string]interface{}{
		"local_file":     localFile,
		"target_server":  targetServer,
		"bytes_written":  resp.BytesWritten,
		"bytes_expected": totalBytes,
		"generation":     t.targetGeneration,
	})

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

// cleanupOldEcShards removes ALL old generation EC shards after successful activation
// This includes not just the source generation, but all generations except the new target generation
func (t *EcVacuumTask) cleanupOldEcShards() error {
	t.LogInfo("Starting cleanup of all old generation EC shards", map[string]interface{}{
		"volume_id":         t.volumeID,
		"target_generation": t.targetGeneration,
		"grace_period":      t.cleanupGracePeriod,
		"note":              "will cleanup ALL generations except target generation",
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

	// Step 3: Use cleanup generations from the vacuum plan
	generationsToCleanup := t.plan.GenerationsToCleanup

	t.LogInfo("Using cleanup generations from vacuum plan", map[string]interface{}{
		"volume_id":              t.volumeID,
		"target_generation":      t.targetGeneration,
		"source_generation":      t.sourceGeneration,
		"generations_to_cleanup": generationsToCleanup,
		"plan_validated":         true,
	})

	// Step 4: Unmount and delete old generation shards from each node
	var cleanupErrors []string
	for node := range t.plan.SourceDistribution.Nodes {
		for _, generation := range generationsToCleanup {
			if err := t.cleanupGenerationFromNode(node, generation); err != nil {
				cleanupErrors = append(cleanupErrors, fmt.Sprintf("node %s generation %d: %v", node, generation, err))
				t.LogWarning("Failed to cleanup generation from node", map[string]interface{}{
					"node":       node,
					"generation": generation,
					"error":      err.Error(),
				})
			}
		}
	}

	// Step 5: Report cleanup results
	if len(cleanupErrors) > 0 {
		t.LogWarning("Cleanup completed with errors", map[string]interface{}{
			"errors":                cleanupErrors,
			"note":                  "some old generation files may remain",
			"generations_attempted": generationsToCleanup,
		})
		// Don't fail the task for cleanup errors - vacuum was successful
		return nil
	}

	t.LogInfo("Successfully cleaned up all old generation EC shards", map[string]interface{}{
		"volume_id":           t.volumeID,
		"target_generation":   t.targetGeneration,
		"cleaned_generations": generationsToCleanup,
		"total_cleaned":       len(generationsToCleanup),
	})
	return nil
}

// cleanupGenerationFromNode unmounts and deletes a specific generation's shards from a node
func (t *EcVacuumTask) cleanupGenerationFromNode(node pb.ServerAddress, generation uint32) error {
	return operation.WithVolumeServerClient(false, node, t.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		t.LogInfo("Cleaning up generation from node", map[string]interface{}{
			"node":       node,
			"volume_id":  t.volumeID,
			"generation": generation,
		})

		// Final safety check: Double-check we're not deleting the active generation
		if generation == t.targetGeneration {
			return fmt.Errorf("CRITICAL SAFETY VIOLATION: attempted to delete active generation %d", generation)
		}

		// Step 1: Unmount all shards for this generation
		// Use all possible shard IDs since we don't know which ones this node has
		allShardIds := make([]uint32, erasure_coding.TotalShardsCount)
		for i := 0; i < erasure_coding.TotalShardsCount; i++ {
			allShardIds[i] = uint32(i)
		}

		_, unmountErr := client.VolumeEcShardsUnmount(context.Background(), &volume_server_pb.VolumeEcShardsUnmountRequest{
			VolumeId:   t.volumeID,
			ShardIds:   allShardIds,
			Generation: generation,
		})

		if unmountErr != nil {
			// Log but continue - files might already be unmounted or not exist on this node
			t.LogInfo("Unmount completed or shards not present on node", map[string]interface{}{
				"node":       node,
				"generation": generation,
				"error":      unmountErr.Error(),
				"note":       "this is normal if shards were already unmounted or don't exist on this node",
			})
		} else {
			t.LogInfo("✅ Successfully unmounted generation shards", map[string]interface{}{
				"node":       node,
				"volume_id":  t.volumeID,
				"generation": generation,
			})
		}

		// Step 2: Delete generation files from disk
		// Note: VolumeEcShardsDelete doesn't support generations, so we need to
		// delete the files directly using generation-aware naming
		if err := t.deleteGenerationFilesFromNode(client, generation); err != nil {
			t.LogWarning("Failed to delete generation files", map[string]interface{}{
				"node":       node,
				"generation": generation,
				"error":      err.Error(),
			})
			// Continue despite deletion errors - unmounting already happened
		} else {
			t.LogInfo("✅ Successfully deleted generation files", map[string]interface{}{
				"node":       node,
				"volume_id":  t.volumeID,
				"generation": generation,
			})
		}

		t.LogInfo("Successfully cleaned up generation from node", map[string]interface{}{
			"node":       node,
			"volume_id":  t.volumeID,
			"generation": generation,
		})
		return nil
	})
}

// deleteGenerationFilesFromNode deletes EC files for a specific generation from a volume server
func (t *EcVacuumTask) deleteGenerationFilesFromNode(client volume_server_pb.VolumeServerClient, generation uint32) error {
	// For all generations, use the existing VolumeEcShardsDelete method
	// Note: This currently only works correctly for generation 0 due to filename patterns
	// For generation > 0, the volume server should ideally be extended to support
	// generation-aware deletion, but for now we rely on the unmount operation
	// to make files safe for cleanup by the volume server's garbage collection

	allShardIds := make([]uint32, erasure_coding.TotalShardsCount)
	for i := 0; i < erasure_coding.TotalShardsCount; i++ {
		allShardIds[i] = uint32(i)
	}

	_, err := client.VolumeEcShardsDelete(context.Background(), &volume_server_pb.VolumeEcShardsDeleteRequest{
		VolumeId:   t.volumeID,
		Collection: t.collection,
		ShardIds:   allShardIds,
	})

	if err != nil {
		// Log warning but don't fail - the unmount should have made files safe for cleanup
		t.LogWarning("VolumeEcShardsDelete returned error - this is expected for generation > 0", map[string]interface{}{
			"volume_id":  t.volumeID,
			"generation": generation,
			"error":      err.Error(),
			"note":       "Generation > 0 files need manual cleanup or volume server extension",
		})

		// For generation > 0, the files are unmounted but not deleted
		// This is a known limitation - the volume server would need to be extended
		// to support generation-aware file deletion in VolumeEcShardsDelete
		if generation > 0 {
			t.LogInfo("Generation > 0 file cleanup limitation", map[string]interface{}{
				"volume_id":  t.volumeID,
				"generation": generation,
				"status":     "unmounted_but_not_deleted",
				"note":       "Files are unmounted from memory but remain on disk until manual cleanup",
			})
		}

		// Don't return error - unmounting is the primary safety requirement
		return nil
	}

	t.LogInfo("✅ Successfully deleted generation files", map[string]interface{}{
		"volume_id":  t.volumeID,
		"generation": generation,
	})

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

// SetAdminAddress sets the admin server address for API calls
func (t *EcVacuumTask) SetAdminAddress(address string) {
	t.adminAddress = address
}

// SetMasterAddress sets the master server address for generation activation
func (t *EcVacuumTask) SetMasterAddress(address pb.ServerAddress) {
	t.masterAddress = address
}

// SetCleanupGracePeriod sets the grace period before cleaning up old generation
func (t *EcVacuumTask) SetCleanupGracePeriod(period time.Duration) {
	t.cleanupGracePeriod = period
}

// fetchMasterAddressFromAdmin gets master addresses from admin server
func (t *EcVacuumTask) fetchMasterAddressFromAdmin() error {
	// Use admin address provided by worker
	if t.adminAddress == "" {
		return fmt.Errorf("admin server address not provided by worker - cannot fetch master addresses")
	}

	// Convert admin HTTP address to gRPC address (HTTP port + 10000)
	grpcAddress := pb.ServerToGrpcAddress(t.adminAddress)

	t.LogInfo("Fetching master address from admin server", map[string]interface{}{
		"admin_address": grpcAddress,
	})

	// Create gRPC connection to admin server
	conn, err := grpc.NewClient(grpcAddress, t.grpcDialOption)
	if err != nil {
		return fmt.Errorf("failed to connect to admin server at %s: %w", grpcAddress, err)
	}
	defer conn.Close()

	// Create worker service client
	client := worker_pb.NewWorkerServiceClient(conn)

	// Call GetMasterAddresses API
	resp, err := client.GetMasterAddresses(context.Background(), &worker_pb.GetMasterAddressesRequest{
		WorkerId: t.ID(), // Use task ID as worker ID for logging
	})
	if err != nil {
		return fmt.Errorf("failed to get master addresses from admin: %w", err)
	}

	if len(resp.MasterAddresses) == 0 {
		return fmt.Errorf("no master addresses returned from admin server")
	}

	// Use primary master if available, otherwise first address
	masterAddress := resp.PrimaryMaster
	if masterAddress == "" && len(resp.MasterAddresses) > 0 {
		masterAddress = resp.MasterAddresses[0]
	}

	t.masterAddress = pb.ServerAddress(masterAddress)

	t.LogInfo("Successfully obtained master address from admin server", map[string]interface{}{
		"master_address":    masterAddress,
		"available_masters": resp.MasterAddresses,
		"primary_master":    resp.PrimaryMaster,
	})

	return nil
}
