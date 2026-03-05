package erasure_coding

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"github.com/seaweedfs/seaweedfs/weed/worker/types/base"
	"google.golang.org/grpc"
)

// ErasureCodingTask implements the Task interface
type ErasureCodingTask struct {
	*base.BaseTask
	server         string
	volumeID       uint32
	collection     string
	workDir        string
	progress       float64
	grpcDialOption grpc.DialOption

	// EC parameters
	dataShards      int32
	parityShards    int32
	targets         []*worker_pb.TaskTarget // Unified targets for EC shards
	sources         []*worker_pb.TaskSource // Unified sources for cleanup
	shardAssignment map[string][]string     // destination -> assigned shard types
}

// NewErasureCodingTask creates a new unified EC task instance
func NewErasureCodingTask(id string, server string, volumeID uint32, collection string, grpcDialOption grpc.DialOption) *ErasureCodingTask {
	return &ErasureCodingTask{
		BaseTask:       base.NewBaseTask(id, types.TaskTypeErasureCoding),
		server:         server,
		volumeID:       volumeID,
		collection:     collection,
		dataShards:     erasure_coding.DataShardsCount,   // Default values
		parityShards:   erasure_coding.ParityShardsCount, // Default values
		grpcDialOption: grpcDialOption,
	}
}

// Execute implements the UnifiedTask interface
func (t *ErasureCodingTask) Execute(ctx context.Context, params *worker_pb.TaskParams) error {
	if params == nil {
		return fmt.Errorf("task parameters are required")
	}

	ecParams := params.GetErasureCodingParams()
	if ecParams == nil {
		return fmt.Errorf("erasure coding parameters are required")
	}

	t.dataShards = ecParams.DataShards
	t.parityShards = ecParams.ParityShards
	t.workDir = ecParams.WorkingDir
	t.targets = params.Targets // Get unified targets
	t.sources = params.Sources // Get unified sources

	// Log detailed task information
	t.GetLogger().WithFields(map[string]interface{}{
		"volume_id":     t.volumeID,
		"server":        t.server,
		"collection":    t.collection,
		"data_shards":   t.dataShards,
		"parity_shards": t.parityShards,
		"total_shards":  t.dataShards + t.parityShards,
		"targets":       len(t.targets),
		"sources":       len(t.sources),
	}).Info("Starting erasure coding task")

	// Log detailed target server assignments
	for i, target := range t.targets {
		t.GetLogger().WithFields(map[string]interface{}{
			"target_index": i,
			"server":       target.Node,
			"shard_ids":    target.ShardIds,
			"shard_count":  len(target.ShardIds),
		}).Info("Target server shard assignment")
	}

	// Log source information
	for i, source := range t.sources {
		t.GetLogger().WithFields(map[string]interface{}{
			"source_index": i,
			"server":       source.Node,
			"volume_id":    source.VolumeId,
			"disk_id":      source.DiskId,
			"rack":         source.Rack,
			"data_center":  source.DataCenter,
		}).Info("Source server information")
	}

	// Use the working directory from task parameters, or fall back to a default
	baseWorkDir := ecParams.WorkingDir
	if baseWorkDir == "" {
		baseWorkDir = t.GetWorkingDir()
	}
	taskWorkDir := filepath.Join(baseWorkDir, fmt.Sprintf("vol_%d_%d", t.volumeID, time.Now().Unix()))
	if err := os.MkdirAll(taskWorkDir, 0755); err != nil {
		return fmt.Errorf("failed to create task working directory %s: %v", taskWorkDir, err)
	}
	glog.V(1).Infof("Created working directory: %s", taskWorkDir)

	// Update the task's working directory to the specific instance directory
	t.workDir = taskWorkDir
	glog.V(1).Infof("Task working directory configured: %s (logs will be written here)", taskWorkDir)

	// Ensure cleanup of working directory
	defer func() {
		// Clean up volume files and EC shards
		patterns := []string{"*.dat", "*.idx", "*.ec*", "*.vif"}
		for _, pattern := range patterns {
			matches, err := filepath.Glob(filepath.Join(taskWorkDir, pattern))
			if err != nil {
				continue
			}
			for _, match := range matches {
				if err := os.Remove(match); err != nil {
					glog.V(2).Infof("Could not remove %s: %v", match, err)
				}
			}
		}
		// Remove the entire working directory
		if err := os.RemoveAll(taskWorkDir); err != nil {
			glog.V(2).Infof("Could not remove working directory %s: %v", taskWorkDir, err)
		} else {
			glog.V(1).Infof("Cleaned up working directory: %s", taskWorkDir)
		}
	}()

	// Step 1: Mark volume readonly
	t.ReportProgressWithStage(10.0, "Marking volume readonly")
	t.GetLogger().Info("Marking volume readonly")
	if err := t.markVolumeReadonly(); err != nil {
		return fmt.Errorf("failed to mark volume readonly: %v", err)
	}

	// Step 2: Copy volume files to worker
	t.ReportProgressWithStage(25.0, "Copying volume files to worker")
	t.GetLogger().Info("Copying volume files to worker")
	localFiles, err := t.copyVolumeFilesToWorker(taskWorkDir)
	if err != nil {
		return fmt.Errorf("failed to copy volume files: %v", err)
	}

	// Step 3: Generate EC shards locally
	t.ReportProgressWithStage(40.0, "Generating EC shards locally")
	t.GetLogger().Info("Generating EC shards locally")
	shardFiles, err := t.generateEcShardsLocally(localFiles, taskWorkDir)
	if err != nil {
		return fmt.Errorf("failed to generate EC shards: %v", err)
	}

	// Step 4: Distribute shards to destinations
	t.ReportProgressWithStage(60.0, "Distributing EC shards to destinations")
	t.GetLogger().Info("Distributing EC shards to destinations")
	if err := t.distributeEcShards(shardFiles); err != nil {
		return fmt.Errorf("failed to distribute EC shards: %v", err)
	}

	// Step 5: Mount EC shards
	t.ReportProgressWithStage(80.0, "Mounting EC shards")
	t.GetLogger().Info("Mounting EC shards")
	if err := t.mountEcShards(); err != nil {
		return fmt.Errorf("failed to mount EC shards: %v", err)
	}

	// Step 6: Delete original volume
	t.ReportProgressWithStage(90.0, "Deleting original volume")
	t.GetLogger().Info("Deleting original volume")
	if err := t.deleteOriginalVolume(); err != nil {
		return fmt.Errorf("failed to delete original volume: %v", err)
	}

	t.ReportProgressWithStage(100.0, "EC processing complete")
	glog.Infof("EC task completed successfully: volume %d from %s with %d shards distributed",
		t.volumeID, t.server, len(shardFiles))

	return nil
}

// Validate implements the UnifiedTask interface
func (t *ErasureCodingTask) Validate(params *worker_pb.TaskParams) error {
	if params == nil {
		return fmt.Errorf("task parameters are required")
	}

	ecParams := params.GetErasureCodingParams()
	if ecParams == nil {
		return fmt.Errorf("erasure coding parameters are required")
	}

	if params.VolumeId != t.volumeID {
		return fmt.Errorf("volume ID mismatch: expected %d, got %d", t.volumeID, params.VolumeId)
	}

	// Validate that at least one source matches our server
	found := false
	for _, source := range params.Sources {
		if source.Node == t.server {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("no source matches expected server %s", t.server)
	}

	if ecParams.DataShards < 1 {
		return fmt.Errorf("invalid data shards: %d (must be >= 1)", ecParams.DataShards)
	}

	if ecParams.ParityShards < 1 {
		return fmt.Errorf("invalid parity shards: %d (must be >= 1)", ecParams.ParityShards)
	}

	if len(params.Targets) < int(ecParams.DataShards+ecParams.ParityShards) {
		return fmt.Errorf("insufficient targets: got %d, need %d", len(params.Targets), ecParams.DataShards+ecParams.ParityShards)
	}

	return nil
}

// EstimateTime implements the UnifiedTask interface
func (t *ErasureCodingTask) EstimateTime(params *worker_pb.TaskParams) time.Duration {
	// Basic estimate based on simulated steps
	return 20 * time.Second // Sum of all step durations
}

// GetProgress returns current progress
func (t *ErasureCodingTask) GetProgress() float64 {
	return t.progress
}

// Helper methods for actual EC operations

// markVolumeReadonly marks the volume as readonly on the source server
func (t *ErasureCodingTask) markVolumeReadonly() error {
	return operation.WithVolumeServerClient(false, pb.ServerAddress(t.server), t.grpcDialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			_, err := client.VolumeMarkReadonly(context.Background(), &volume_server_pb.VolumeMarkReadonlyRequest{
				VolumeId: t.volumeID,
			})
			return err
		})
}

// copyVolumeFilesToWorker copies .dat and .idx files from source server to local worker
func (t *ErasureCodingTask) copyVolumeFilesToWorker(workDir string) (map[string]string, error) {
	localFiles := make(map[string]string)

	t.GetLogger().WithFields(map[string]interface{}{
		"volume_id":   t.volumeID,
		"source":      t.server,
		"working_dir": workDir,
	}).Info("Starting volume file copy from source server")

	// Copy .dat file
	datFile := filepath.Join(workDir, fmt.Sprintf("%d.dat", t.volumeID))
	if err := t.copyFileFromSource(".dat", datFile); err != nil {
		return nil, fmt.Errorf("failed to copy .dat file: %v", err)
	}
	localFiles["dat"] = datFile

	// Log .dat file size
	if info, err := os.Stat(datFile); err == nil {
		t.GetLogger().WithFields(map[string]interface{}{
			"file_type":  ".dat",
			"file_path":  datFile,
			"size_bytes": info.Size(),
			"size_mb":    float64(info.Size()) / (1024 * 1024),
		}).Info("Volume data file copied successfully")
	}

	// Copy .idx file
	idxFile := filepath.Join(workDir, fmt.Sprintf("%d.idx", t.volumeID))
	if err := t.copyFileFromSource(".idx", idxFile); err != nil {
		return nil, fmt.Errorf("failed to copy .idx file: %v", err)
	}
	localFiles["idx"] = idxFile

	// Log .idx file size
	if info, err := os.Stat(idxFile); err == nil {
		t.GetLogger().WithFields(map[string]interface{}{
			"file_type":  ".idx",
			"file_path":  idxFile,
			"size_bytes": info.Size(),
			"size_mb":    float64(info.Size()) / (1024 * 1024),
		}).Info("Volume index file copied successfully")
	}

	return localFiles, nil
}

// copyFileFromSource copies a file from source server to local path using gRPC streaming
func (t *ErasureCodingTask) copyFileFromSource(ext, localPath string) error {
	return operation.WithVolumeServerClient(false, pb.ServerAddress(t.server), t.grpcDialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			stream, err := client.CopyFile(context.Background(), &volume_server_pb.CopyFileRequest{
				VolumeId:   t.volumeID,
				Collection: t.collection,
				Ext:        ext,
				StopOffset: uint64(math.MaxInt64),
			})
			if err != nil {
				return fmt.Errorf("failed to initiate file copy: %v", err)
			}

			// Create local file
			localFile, err := os.Create(localPath)
			if err != nil {
				return fmt.Errorf("failed to create local file %s: %v", localPath, err)
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
					return fmt.Errorf("failed to receive file data: %v", err)
				}

				if len(resp.FileContent) > 0 {
					written, writeErr := localFile.Write(resp.FileContent)
					if writeErr != nil {
						return fmt.Errorf("failed to write to local file: %v", writeErr)
					}
					totalBytes += int64(written)
				}
			}

			glog.V(1).Infof("Successfully copied %s (%d bytes) from %s to %s", ext, totalBytes, t.server, localPath)
			return nil
		})
}

// generateEcShardsLocally generates EC shards from local volume files
func (t *ErasureCodingTask) generateEcShardsLocally(localFiles map[string]string, workDir string) (map[string]string, error) {
	datFile := localFiles["dat"]
	idxFile := localFiles["idx"]

	if datFile == "" || idxFile == "" {
		return nil, fmt.Errorf("missing required volume files: dat=%s, idx=%s", datFile, idxFile)
	}

	// Get base name without extension for EC operations
	baseName := strings.TrimSuffix(datFile, ".dat")
	shardFiles := make(map[string]string)

	glog.V(1).Infof("Generating EC shards from local files: dat=%s, idx=%s", datFile, idxFile)

	// Generate EC shard files (.ec00 ~ .ec13)
	if err := erasure_coding.WriteEcFiles(baseName); err != nil {
		return nil, fmt.Errorf("failed to generate EC shard files: %v", err)
	}

	// Generate .ecx file from .idx (use baseName, not full idx path)
	if err := erasure_coding.WriteSortedFileFromIdx(baseName, ".ecx"); err != nil {
		return nil, fmt.Errorf("failed to generate .ecx file: %v", err)
	}

	// Collect generated shard file paths and log details
	var generatedShards []string
	var totalShardSize int64

	// Check up to MaxShardCount (32) to support custom EC ratios
	for i := 0; i < erasure_coding.MaxShardCount; i++ {
		shardFile := fmt.Sprintf("%s.ec%02d", baseName, i)
		if info, err := os.Stat(shardFile); err == nil {
			shardKey := fmt.Sprintf("ec%02d", i)
			shardFiles[shardKey] = shardFile
			generatedShards = append(generatedShards, shardKey)
			totalShardSize += info.Size()

			// Log individual shard details
			t.GetLogger().WithFields(map[string]interface{}{
				"shard_id":   i,
				"shard_type": shardKey,
				"file_path":  shardFile,
				"size_bytes": info.Size(),
				"size_kb":    float64(info.Size()) / 1024,
			}).Info("EC shard generated")
		}
	}

	// Add metadata files
	ecxFile := baseName + ".ecx"
	if info, err := os.Stat(ecxFile); err == nil {
		shardFiles["ecx"] = ecxFile
		t.GetLogger().WithFields(map[string]interface{}{
			"file_type":  "ecx",
			"file_path":  ecxFile,
			"size_bytes": info.Size(),
		}).Info("EC index file generated")
	}

	ecjFile := baseName + ".ecj"
	if info, err := os.Stat(ecjFile); err == nil {
		shardFiles["ecj"] = ecjFile
		t.GetLogger().WithFields(map[string]interface{}{
			"file_type":  "ecj",
			"file_path":  ecjFile,
			"size_bytes": info.Size(),
		}).Info("EC journal file generated")
	}

	// Generate .vif file (volume info)
	vifFile := baseName + ".vif"
	volumeInfo := &volume_server_pb.VolumeInfo{
		Version: uint32(needle.GetCurrentVersion()),
	}
	if err := volume_info.SaveVolumeInfo(vifFile, volumeInfo); err != nil {
		glog.Warningf("Failed to create .vif file: %v", err)
	} else {
		shardFiles["vif"] = vifFile
		if info, err := os.Stat(vifFile); err == nil {
			t.GetLogger().WithFields(map[string]interface{}{
				"file_type":  "vif",
				"file_path":  vifFile,
				"size_bytes": info.Size(),
			}).Info("Volume info file generated")
		}
	}

	// Log summary of generation
	t.GetLogger().WithFields(map[string]interface{}{
		"total_files":         len(shardFiles),
		"ec_shards":           len(generatedShards),
		"generated_shards":    generatedShards,
		"total_shard_size_mb": float64(totalShardSize) / (1024 * 1024),
	}).Info("EC shard generation completed")
	return shardFiles, nil
}

// distributeEcShards distributes locally generated EC shards to destination servers
// using pre-assigned shard IDs from planning phase
func (t *ErasureCodingTask) distributeEcShards(shardFiles map[string]string) error {
	assignment, err := erasure_coding.DistributeEcShards(t.volumeID, t.collection, t.targets, shardFiles, t.grpcDialOption, t.GetLogger())
	if err != nil {
		return err
	}
	t.shardAssignment = assignment
	return nil
}

// mountEcShards mounts EC shards on destination servers
func (t *ErasureCodingTask) mountEcShards() error {
	return erasure_coding.MountEcShards(t.volumeID, t.collection, t.shardAssignment, t.grpcDialOption, t.GetLogger())
}

// deleteOriginalVolume deletes the original volume and all its replicas from all servers
func (t *ErasureCodingTask) deleteOriginalVolume() error {
	// Get replicas from task parameters (set during detection)
	replicas := t.getReplicas()

	if len(replicas) == 0 {
		glog.Warningf("No replicas found for volume %d, falling back to source server only", t.volumeID)
		replicas = []string{t.server}
	}

	t.GetLogger().WithFields(map[string]interface{}{
		"volume_id":       t.volumeID,
		"replica_count":   len(replicas),
		"replica_servers": replicas,
	}).Info("Starting original volume deletion from replica servers")

	// Delete volume from all replica locations
	var deleteErrors []string
	successCount := 0

	for i, replicaServer := range replicas {
		t.GetLogger().WithFields(map[string]interface{}{
			"replica_index":  i + 1,
			"total_replicas": len(replicas),
			"server":         replicaServer,
			"volume_id":      t.volumeID,
		}).Info("Deleting volume from replica server")

		err := operation.WithVolumeServerClient(false, pb.ServerAddress(replicaServer), t.grpcDialOption,
			func(client volume_server_pb.VolumeServerClient) error {
				_, err := client.VolumeDelete(context.Background(), &volume_server_pb.VolumeDeleteRequest{
					VolumeId:  t.volumeID,
					OnlyEmpty: false, // Force delete since we've created EC shards
				})
				return err
			})

		if err != nil {
			deleteErrors = append(deleteErrors, fmt.Sprintf("failed to delete volume %d from %s: %v", t.volumeID, replicaServer, err))
			t.GetLogger().WithFields(map[string]interface{}{
				"server":    replicaServer,
				"volume_id": t.volumeID,
				"error":     err.Error(),
			}).Error("Failed to delete volume from replica server")
		} else {
			successCount++
			t.GetLogger().WithFields(map[string]interface{}{
				"server":    replicaServer,
				"volume_id": t.volumeID,
			}).Info("Successfully deleted volume from replica server")
		}
	}

	// Report results
	if len(deleteErrors) > 0 {
		t.GetLogger().WithFields(map[string]interface{}{
			"volume_id":      t.volumeID,
			"successful":     successCount,
			"failed":         len(deleteErrors),
			"total_replicas": len(replicas),
			"success_rate":   float64(successCount) / float64(len(replicas)) * 100,
			"errors":         deleteErrors,
		}).Warning("Some volume deletions failed")
		// Don't return error - EC task should still be considered successful if shards are mounted
	} else {
		t.GetLogger().WithFields(map[string]interface{}{
			"volume_id":       t.volumeID,
			"replica_count":   len(replicas),
			"replica_servers": replicas,
		}).Info("Successfully deleted volume from all replica servers")
	}

	return nil
}

// getReplicas extracts replica servers from unified sources
func (t *ErasureCodingTask) getReplicas() []string {
	var replicas []string
	for _, source := range t.sources {
		// Only include volume replica sources (not EC shard sources)
		// Assumption: VolumeId == 0 is considered invalid and should be excluded.
		// If volume ID 0 is valid in some contexts, update this check accordingly.
		if source.VolumeId > 0 {
			replicas = append(replicas, source.Node)
		}
	}
	return replicas
}
