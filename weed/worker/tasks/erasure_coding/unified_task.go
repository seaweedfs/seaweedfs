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
	server     string
	volumeID   uint32
	collection string
	workDir    string
	progress   float64

	// EC parameters
	dataShards   int32
	parityShards int32
	destinations []*worker_pb.ECDestination
}

// NewErasureCodingTask creates a new unified EC task instance
func NewErasureCodingTask(id string, server string, volumeID uint32, collection string) *ErasureCodingTask {
	return &ErasureCodingTask{
		BaseTask:     base.NewBaseTask(id, types.TaskTypeErasureCoding),
		server:       server,
		volumeID:     volumeID,
		collection:   collection,
		dataShards:   10, // Default values
		parityShards: 4,  // Default values
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
	t.destinations = ecParams.Destinations

	t.GetLogger().WithFields(map[string]interface{}{
		"volume_id":     t.volumeID,
		"server":        t.server,
		"collection":    t.collection,
		"data_shards":   t.dataShards,
		"parity_shards": t.parityShards,
		"destinations":  len(t.destinations),
	}).Info("Starting erasure coding task")

	// Use the working directory from task parameters, or fall back to a default
	baseWorkDir := t.workDir
	if baseWorkDir == "" {
		baseWorkDir = "/tmp/seaweedfs_ec_work"
	}

	// Create unique working directory for this task
	taskWorkDir := filepath.Join(baseWorkDir, fmt.Sprintf("vol_%d_%d", t.volumeID, time.Now().Unix()))
	if err := os.MkdirAll(taskWorkDir, 0755); err != nil {
		return fmt.Errorf("failed to create task working directory %s: %v", taskWorkDir, err)
	}
	glog.V(1).Infof("Created working directory: %s", taskWorkDir)

	// Update the task's working directory to the specific instance directory
	t.workDir = taskWorkDir
	glog.V(1).Infof("Task working directory configured: %s (logs will be written here)", taskWorkDir)

	// Ensure cleanup of working directory (but preserve logs)
	defer func() {
		// Clean up volume files and EC shards, but preserve the directory structure and any logs
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
		glog.V(1).Infof("Cleaned up volume files from working directory: %s (logs preserved)", taskWorkDir)
	}()

	// Step 1: Mark volume readonly
	t.ReportProgress(10.0)
	t.GetLogger().Info("Marking volume readonly")
	if err := t.markVolumeReadonly(); err != nil {
		return fmt.Errorf("failed to mark volume readonly: %v", err)
	}

	// Step 2: Copy volume files to worker
	t.ReportProgress(25.0)
	t.GetLogger().Info("Copying volume files to worker")
	localFiles, err := t.copyVolumeFilesToWorker(taskWorkDir)
	if err != nil {
		return fmt.Errorf("failed to copy volume files: %v", err)
	}

	// Step 3: Generate EC shards locally
	t.ReportProgress(40.0)
	t.GetLogger().Info("Generating EC shards locally")
	shardFiles, err := t.generateEcShardsLocally(localFiles, taskWorkDir)
	if err != nil {
		return fmt.Errorf("failed to generate EC shards: %v", err)
	}

	// Step 4: Distribute shards to destinations
	t.ReportProgress(60.0)
	t.GetLogger().Info("Distributing EC shards to destinations")
	if err := t.distributeEcShards(); err != nil {
		return fmt.Errorf("failed to distribute EC shards: %v", err)
	}

	// Step 5: Mount EC shards
	t.ReportProgress(80.0)
	t.GetLogger().Info("Mounting EC shards")
	if err := t.mountEcShards(); err != nil {
		return fmt.Errorf("failed to mount EC shards: %v", err)
	}

	// Step 6: Delete original volume
	t.ReportProgress(90.0)
	t.GetLogger().Info("Deleting original volume")
	if err := t.deleteOriginalVolume(); err != nil {
		return fmt.Errorf("failed to delete original volume: %v", err)
	}

	t.ReportProgress(100.0)
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

	if params.Server != t.server {
		return fmt.Errorf("source server mismatch: expected %s, got %s", t.server, params.Server)
	}

	if ecParams.DataShards < 1 {
		return fmt.Errorf("invalid data shards: %d (must be >= 1)", ecParams.DataShards)
	}

	if ecParams.ParityShards < 1 {
		return fmt.Errorf("invalid parity shards: %d (must be >= 1)", ecParams.ParityShards)
	}

	if len(ecParams.Destinations) < int(ecParams.DataShards+ecParams.ParityShards) {
		return fmt.Errorf("insufficient destinations: got %d, need %d", len(ecParams.Destinations), ecParams.DataShards+ecParams.ParityShards)
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
	return operation.WithVolumeServerClient(false, pb.ServerAddress(t.server), grpc.WithInsecure(),
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

	// Copy .dat file
	datFile := filepath.Join(workDir, fmt.Sprintf("%d.dat", t.volumeID))
	if err := t.copyFileFromSource(".dat", datFile); err != nil {
		return nil, fmt.Errorf("failed to copy .dat file: %v", err)
	}
	localFiles["dat"] = datFile

	// Copy .idx file
	idxFile := filepath.Join(workDir, fmt.Sprintf("%d.idx", t.volumeID))
	if err := t.copyFileFromSource(".idx", idxFile); err != nil {
		return nil, fmt.Errorf("failed to copy .idx file: %v", err)
	}
	localFiles["idx"] = idxFile

	return localFiles, nil
}

// copyFileFromSource copies a file from source server to local path using gRPC streaming
func (t *ErasureCodingTask) copyFileFromSource(ext, localPath string) error {
	return operation.WithVolumeServerClient(false, pb.ServerAddress(t.server), grpc.WithInsecure(),
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

	// Generate .ecx file from .idx
	if err := erasure_coding.WriteSortedFileFromIdx(idxFile, ".ecx"); err != nil {
		return nil, fmt.Errorf("failed to generate .ecx file: %v", err)
	}

	// Collect generated shard file paths
	for i := 0; i < erasure_coding.TotalShardsCount; i++ {
		shardFile := fmt.Sprintf("%s.ec%02d", baseName, i)
		if _, err := os.Stat(shardFile); err == nil {
			shardFiles[fmt.Sprintf("ec%02d", i)] = shardFile
		}
	}

	// Add metadata files
	ecxFile := idxFile + ".ecx"
	if _, err := os.Stat(ecxFile); err == nil {
		shardFiles["ecx"] = ecxFile
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
	}

	glog.V(1).Infof("Generated %d EC files locally", len(shardFiles))
	return shardFiles, nil
}

// distributeEcShards distributes EC shards to destination servers
func (t *ErasureCodingTask) distributeEcShards() error {
	if len(t.destinations) == 0 {
		return fmt.Errorf("no destinations specified for EC shard distribution")
	}

	// Use VolumeEcShardsCopy to copy shards from source to destinations
	for _, dest := range t.destinations {
		err := operation.WithVolumeServerClient(false, pb.ServerAddress(dest.Node), grpc.WithInsecure(),
			func(client volume_server_pb.VolumeServerClient) error {
				_, copyErr := client.VolumeEcShardsCopy(context.Background(), &volume_server_pb.VolumeEcShardsCopyRequest{
					VolumeId:       t.volumeID,
					Collection:     t.collection,
					ShardIds:       []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}, // All shards
					CopyEcxFile:    true,
					CopyEcjFile:    false,
					CopyVifFile:    true,
					SourceDataNode: t.server,
					DiskId:         dest.DiskId, // Pass target disk ID
				})
				return copyErr
			})

		if err != nil {
			return fmt.Errorf("failed to copy shards to %s: %v", dest.Node, err)
		}
	}

	return nil
}

// mountEcShards mounts EC shards on destination servers
func (t *ErasureCodingTask) mountEcShards() error {
	// Prepare all shard IDs (0-13)
	var allShardIds []uint32
	for i := 0; i < erasure_coding.TotalShardsCount; i++ {
		allShardIds = append(allShardIds, uint32(i))
	}

	for _, dest := range t.destinations {
		err := operation.WithVolumeServerClient(false, pb.ServerAddress(dest.Node), grpc.WithInsecure(),
			func(client volume_server_pb.VolumeServerClient) error {
				_, mountErr := client.VolumeEcShardsMount(context.Background(), &volume_server_pb.VolumeEcShardsMountRequest{
					VolumeId:   t.volumeID,
					Collection: t.collection,
					ShardIds:   allShardIds,
				})
				return mountErr
			})

		if err != nil {
			glog.Warningf("Failed to mount shards on %s (this may be normal): %v", dest.Node, err)
		} else {
			glog.V(1).Infof("Successfully mounted EC shards on %s", dest.Node)
		}
	}

	return nil
}

// deleteOriginalVolume deletes the original volume from the source server
func (t *ErasureCodingTask) deleteOriginalVolume() error {
	return operation.WithVolumeServerClient(false, pb.ServerAddress(t.server), grpc.WithInsecure(),
		func(client volume_server_pb.VolumeServerClient) error {
			_, err := client.VolumeDelete(context.Background(), &volume_server_pb.VolumeDeleteRequest{
				VolumeId:  t.volumeID,
				OnlyEmpty: false, // Force delete since we've created EC shards
			})
			return err
		})
}
