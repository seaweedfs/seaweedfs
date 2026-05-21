package erasure_coding

import (
	"bytes"
	"context"
	"fmt"
	"io"
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
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	storagetypes "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_replica"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
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
	dataShards       int32
	parityShards     int32
	sourceDiskType   string                  // source volume's disk type, forwarded to Mount RPC (#9423)
	targets          []*worker_pb.TaskTarget // Unified targets for EC shards
	sources          []*worker_pb.TaskSource // Unified sources for cleanup
	shardAssignment  map[string][]string     // destination -> assigned shard types
	readonlyReplicas []pb.ServerAddress      // replicas marked readonly, for rollback
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
	t.sourceDiskType = ecParams.SourceDiskType
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

	// Step 1: Mark all replicas readonly, then reconcile them and select the most
	// complete replica as the encode source. Encoding a stale replica and then
	// deleting the originals would silently lose entries that exist only on another
	// replica; SyncAndSelectBestReplica builds the union onto the best replica first
	// (mirrors the shell ec.encode best-replica selection).
	t.ReportProgressWithStage(10.0, "Marking volume readonly")
	t.GetLogger().Info("Marking volume readonly")
	if err := t.markReplicasReadonly(ctx); err != nil {
		return fmt.Errorf("failed to mark volume readonly: %v", err)
	}
	if err := t.syncAndSelectSourceReplica(); err != nil {
		t.rollbackReadonly(ctx)
		return fmt.Errorf("failed to sync and select source replica: %v", err)
	}

	// Step 2: Copy volume files to worker
	// The .idx and .dat are copied as separate network transfers, with .idx
	// copied first. If a write lands on the source after the .idx copy, the
	// .dat will include extra data not referenced by .idx (harmless).
	// verifyDatIdxConsistency() in generateEcShardsLocally catches the reverse
	// case where .idx references data past .dat.
	t.ReportProgressWithStage(25.0, "Copying volume files to worker")
	t.GetLogger().Info("Copying volume files to worker")
	localFiles, err := t.copyVolumeFilesToWorker(ctx, taskWorkDir)
	if err != nil {
		t.rollbackReadonly(ctx)
		return fmt.Errorf("failed to copy volume files: %v", err)
	}

	// Step 3: Generate EC shards locally
	t.ReportProgressWithStage(40.0, "Generating EC shards locally")
	t.GetLogger().Info("Generating EC shards locally")
	shardFiles, err := t.generateEcShardsLocally(localFiles, taskWorkDir)
	if err != nil {
		t.rollbackReadonly(ctx)
		return fmt.Errorf("failed to generate EC shards: %v", err)
	}

	// Clear partial EC shards left over on destinations from a prior failed
	// encode so distributeEcShards' ReceiveFile is not refused by the
	// mounted-volume guard.
	t.ReportProgressWithStage(55.0, "Clearing stale EC shards on destinations")
	t.GetLogger().Info("Clearing stale EC shards on destinations")
	if err := t.cleanupStaleEcShards(ctx); err != nil {
		t.rollbackReadonly(ctx)
		return fmt.Errorf("failed to clear stale EC shards on destinations: %v", err)
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

	// Without this gate, a partial distribute/mount lets the next step
	// zero the only intact .dat while the cluster is missing shards.
	t.ReportProgressWithStage(85.0, "Verifying EC shards across destinations")
	t.GetLogger().Info("Verifying EC shards across destinations")
	if err := t.verifyEcShardsBeforeDelete(ctx); err != nil {
		return fmt.Errorf("EC shard verification failed; refusing to delete source volume %d: %w", t.volumeID, err)
	}

	// Step 7: Delete original volume
	t.ReportProgressWithStage(90.0, "Deleting original volume")
	t.GetLogger().Info("Deleting original volume")
	if err := t.deleteOriginalVolume(ctx); err != nil {
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

// replicaLocations returns the regular (non-EC) volume replica locations from the
// task sources. EC-shard sources carry shard ids; regular replicas do not. Falls
// back to the assigned source server when no replica sources are present.
func (t *ErasureCodingTask) replicaLocations() []wdclient.Location {
	var locs []wdclient.Location
	for _, s := range t.sources {
		if s == nil || len(s.ShardIds) > 0 || s.Node == "" {
			continue
		}
		locs = append(locs, wdclient.Location{Url: s.Node, DataCenter: s.DataCenter})
	}
	if len(locs) == 0 {
		locs = append(locs, wdclient.Location{Url: t.server})
	}
	return locs
}

// markReplicasReadonly marks every regular replica readonly so no writes land
// during encoding, recording them so rollbackReadonly can restore them all.
func (t *ErasureCodingTask) markReplicasReadonly(ctx context.Context) error {
	t.readonlyReplicas = t.readonlyReplicas[:0]
	for _, loc := range t.replicaLocations() {
		addr := loc.ServerAddress()
		err := operation.WithVolumeServerClient(false, addr, t.grpcDialOption,
			func(client volume_server_pb.VolumeServerClient) error {
				_, e := client.VolumeMarkReadonly(ctx, &volume_server_pb.VolumeMarkReadonlyRequest{VolumeId: t.volumeID})
				return e
			})
		if err != nil {
			return fmt.Errorf("mark volume %d readonly on %s: %w", t.volumeID, addr, err)
		}
		t.readonlyReplicas = append(t.readonlyReplicas, addr)
	}
	return nil
}

// syncAndSelectSourceReplica reconciles the volume's replicas (building the union
// of all live entries onto the most complete one) and switches the encode source
// to that replica, so a stale replica is never the basis of the encode.
func (t *ErasureCodingTask) syncAndSelectSourceReplica() error {
	locs := t.replicaLocations()
	if len(locs) <= 1 {
		return nil // single replica: nothing to reconcile
	}
	var buf bytes.Buffer
	best, err := volume_replica.SyncAndSelectBestReplica(t.grpcDialOption, needle.VolumeId(t.volumeID), t.collection, locs, "", &buf)
	if out := strings.TrimSpace(buf.String()); out != "" {
		glog.Infof("EC encode replica sync for volume %d:\n%s", t.volumeID, out)
	}
	if err != nil {
		return err
	}
	if best.Url != "" && best.Url != t.server {
		glog.Infof("EC encode: using best replica %s as source for volume %d (was %s)", best.Url, t.volumeID, t.server)
		t.server = best.Url
	}
	return nil
}

// rollbackReadonly is a best-effort restore of every replica markReplicasReadonly
// touched, used when the EC task fails before the originals are deleted. Logs but
// does not return errors; uses a fresh context since the caller's may be cancelled.
func (t *ErasureCodingTask) rollbackReadonly(_ context.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	servers := t.readonlyReplicas
	if len(servers) == 0 {
		servers = []pb.ServerAddress{pb.ServerAddress(t.server)}
	}
	for _, addr := range servers {
		err := operation.WithVolumeServerClient(false, addr, t.grpcDialOption,
			func(client volume_server_pb.VolumeServerClient) error {
				_, e := client.VolumeMarkWritable(ctx, &volume_server_pb.VolumeMarkWritableRequest{VolumeId: t.volumeID})
				return e
			})
		if err != nil {
			glog.Warningf("failed to restore volume %d to writable on %s after EC task failure: %v", t.volumeID, addr, err)
		} else {
			glog.V(0).Infof("restored volume %d to writable on %s after EC task failure", t.volumeID, addr)
		}
	}
}

// copyVolumeFilesToWorker copies .idx and .dat files from source server to local worker.
// The .idx is copied first, then .dat. Both copies are capped to the sizes reported by
// ReadVolumeFileStatus. If a write lands after .idx is copied, .dat may include extra
// data not referenced by .idx (harmless). The reverse (idx referencing data past .dat)
// is caught by verifyDatIdxConsistency in generateEcShardsLocally.
func (t *ErasureCodingTask) copyVolumeFilesToWorker(ctx context.Context, workDir string) (map[string]string, error) {
	localFiles := make(map[string]string)

	fileStatus, err := t.readSourceVolumeFileStatus(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read source volume file status: %v", err)
	}

	t.GetLogger().WithFields(map[string]interface{}{
		"volume_id":           t.volumeID,
		"source":              t.server,
		"working_dir":         workDir,
		"compaction_revision": fileStatus.GetCompactionRevision(),
		"dat_file_size_bytes": fileStatus.GetDatFileSize(),
		"idx_file_size_bytes": fileStatus.GetIdxFileSize(),
	}).Info("Starting volume file copy from source server")

	// Copy .idx file FIRST — if a write lands on the source after this copy,
	// the .dat copy will include the new data but .idx won't reference it.
	idxFile := filepath.Join(workDir, fmt.Sprintf("%d.idx", t.volumeID))
	if err := t.copyFileFromSource(ctx, ".idx", idxFile, fileStatus.GetCompactionRevision(), fileStatus.GetIdxFileSize()); err != nil {
		return nil, fmt.Errorf("failed to copy .idx file: %v", err)
	}
	localFiles["idx"] = idxFile

	if info, err := os.Stat(idxFile); err == nil {
		t.GetLogger().WithFields(map[string]interface{}{
			"file_type":  ".idx",
			"file_path":  idxFile,
			"size_bytes": info.Size(),
			"size_mb":    float64(info.Size()) / (1024 * 1024),
		}).Info("Volume index file copied successfully")
	}

	// Copy .dat file SECOND — guaranteed to have at least as much data as .idx references.
	datFile := filepath.Join(workDir, fmt.Sprintf("%d.dat", t.volumeID))
	if err := t.copyFileFromSource(ctx, ".dat", datFile, fileStatus.GetCompactionRevision(), fileStatus.GetDatFileSize()); err != nil {
		return nil, fmt.Errorf("failed to copy .dat file: %v", err)
	}
	localFiles["dat"] = datFile

	if info, err := os.Stat(datFile); err == nil {
		t.GetLogger().WithFields(map[string]interface{}{
			"file_type":  ".dat",
			"file_path":  datFile,
			"size_bytes": info.Size(),
			"size_mb":    float64(info.Size()) / (1024 * 1024),
		}).Info("Volume data file copied successfully")
	}

	return localFiles, nil
}

func (t *ErasureCodingTask) readSourceVolumeFileStatus(ctx context.Context) (*volume_server_pb.ReadVolumeFileStatusResponse, error) {
	var statusResp *volume_server_pb.ReadVolumeFileStatusResponse
	err := operation.WithVolumeServerClient(false, pb.ServerAddress(t.server), t.grpcDialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			var readErr error
			statusResp, readErr = client.ReadVolumeFileStatus(ctx, &volume_server_pb.ReadVolumeFileStatusRequest{
				VolumeId: t.volumeID,
			})
			return readErr
		})
	if err != nil {
		return nil, err
	}
	if statusResp.GetDatFileSize() == 0 {
		return nil, fmt.Errorf("volume %d on %s reports zero dat file size", t.volumeID, t.server)
	}
	if statusResp.GetIdxFileSize() == 0 {
		return nil, fmt.Errorf("volume %d on %s reports zero idx file size with non-empty dat", t.volumeID, t.server)
	}
	return statusResp, nil
}

// copyFileFromSource copies a file from source server to local path using gRPC streaming
func (t *ErasureCodingTask) copyFileFromSource(ctx context.Context, ext, localPath string, compactionRevision uint32, stopOffset uint64) error {
	return operation.WithVolumeServerClient(false, pb.ServerAddress(t.server), t.grpcDialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			stream, err := client.CopyFile(ctx, &volume_server_pb.CopyFileRequest{
				VolumeId:           t.volumeID,
				Collection:         t.collection,
				Ext:                ext,
				CompactionRevision: compactionRevision,
				StopOffset:         stopOffset,
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

			if totalBytes != int64(stopOffset) {
				return fmt.Errorf("short copy of %s: got %d bytes, expected %d", ext, totalBytes, stopOffset)
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

	// Verify .dat and .idx are consistent before EC encoding.
	// Since they were copied as separate network transfers, the .idx may have
	// entries pointing past the end of .dat if a write landed between the copies.
	if err := verifyDatIdxConsistency(datFile, idxFile); err != nil {
		return nil, fmt.Errorf("dat/idx consistency check failed: %v", err)
	}

	// Generate .ecx file from .idx BEFORE EC shards to prevent inconsistency.
	if err := erasure_coding.WriteSortedFileFromIdx(baseName, ".ecx"); err != nil {
		return nil, fmt.Errorf("failed to generate .ecx file: %v", err)
	}

	// Generate EC shard files (.ec00 ~ .ec13)
	if err := erasure_coding.WriteEcFiles(baseName); err != nil {
		return nil, fmt.Errorf("failed to generate EC shard files: %v", err)
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
	return erasure_coding.MountEcShards(t.volumeID, t.collection, t.shardAssignment, t.sourceDiskType, t.grpcDialOption, t.GetLogger())
}

func (t *ErasureCodingTask) verifyEcShardsBeforeDelete(ctx context.Context) error {
	servers := make([]string, 0, len(t.shardAssignment))
	for node := range t.shardAssignment {
		servers = append(servers, node)
	}
	if len(servers) == 0 {
		return fmt.Errorf("no destinations to verify; shardAssignment is empty")
	}

	totalShards := int(t.dataShards + t.parityShards)
	union, perServer := erasure_coding.VerifyShardsAcrossServers(ctx, t.volumeID, servers, t.grpcDialOption)

	summary := erasure_coding.SummarizeShardInventory(perServer)
	t.GetLogger().WithFields(map[string]interface{}{
		"volume_id":     t.volumeID,
		"shards_seen":   union.Count(),
		"shards_needed": totalShards,
		"per_server":    summary,
	}).Info("EC shard inventory before source deletion")

	if err := erasure_coding.RequireFullShardSet(t.volumeID, union, totalShards); err != nil {
		t.GetLogger().WithFields(map[string]interface{}{
			"volume_id":  t.volumeID,
			"per_server": summary,
			"error":      err.Error(),
		}).Error("EC shard verification failed — source volume will be kept")
		return err
	}
	return nil
}

// deleteOriginalVolume deletes the original volume and all its replicas from all servers
func (t *ErasureCodingTask) deleteOriginalVolume(ctx context.Context) error {
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
				_, err := client.VolumeDelete(ctx, &volume_server_pb.VolumeDeleteRequest{
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

	if len(deleteErrors) > 0 {
		t.GetLogger().WithFields(map[string]interface{}{
			"volume_id":      t.volumeID,
			"successful":     successCount,
			"failed":         len(deleteErrors),
			"total_replicas": len(replicas),
			"success_rate":   float64(successCount) / float64(len(replicas)) * 100,
			"errors":         deleteErrors,
		}).Error("Failed to delete some original volume replicas after EC encoding")
		// A surviving source replica lets a later detection scan re-propose
		// EC on the same volume, which retries over mounted shards.
		return fmt.Errorf("failed to delete %d of %d original volume replicas for volume %d: %s",
			len(deleteErrors), len(replicas), t.volumeID, strings.Join(deleteErrors, "; "))
	}

	t.GetLogger().WithFields(map[string]interface{}{
		"volume_id":       t.volumeID,
		"replica_count":   len(replicas),
		"replica_servers": replicas,
	}).Info("Successfully deleted volume from all replica servers")

	return nil
}

// getReplicas extracts regular .dat replica servers from unified sources.
// Sources with ShardIds set are EC-shard cleanup targets and must be skipped.
// Per-disk source rows are deduped to one server entry — VolumeDelete is a
// server-wide call.
func (t *ErasureCodingTask) getReplicas() []string {
	var replicas []string
	seen := make(map[string]struct{})
	for _, source := range t.sources {
		if source.VolumeId == 0 || len(source.ShardIds) > 0 {
			continue
		}
		if _, ok := seen[source.Node]; ok {
			continue
		}
		seen[source.Node] = struct{}{}
		replicas = append(replicas, source.Node)
	}
	return replicas
}

// cleanupStaleEcShards unmounts and deletes any EC shards still mounted on
// destinations from a previous failed encode of this volume. Targets every
// node we plan to write to (t.targets) plus every node detection saw EC
// shards on (t.sources with ShardIds set), and issues the cleanup over the
// full shard range so a stale topology snapshot — or shards landed by a
// prior attempt that haven't heartbeated yet — cannot leave the
// mounted-volume guard tripped during distributeEcShards. Safe by ordering:
// runs after the source .dat is in the worker's workdir and a full local
// shard set is generated. Per-destination errors are aggregated, not
// short-circuited.
func (t *ErasureCodingTask) cleanupStaleEcShards(ctx context.Context) error {
	nodes := make(map[string]struct{})
	for _, source := range t.sources {
		if source == nil || source.Node == "" || len(source.ShardIds) == 0 {
			continue
		}
		nodes[source.Node] = struct{}{}
	}
	for _, target := range t.targets {
		if target == nil || target.Node == "" {
			continue
		}
		nodes[target.Node] = struct{}{}
	}
	if len(nodes) == 0 {
		return nil
	}

	allShards := fullShardIdRange(t.dataShards, t.parityShards)

	var cleanupErrors []string
	for node := range nodes {
		t.GetLogger().WithFields(map[string]interface{}{
			"volume_id":   t.volumeID,
			"destination": node,
			"shard_ids":   allShards,
		}).Info("Clearing stale EC shards on destination before re-distribute")

		if err := unmountAndDeleteEcShards(ctx, t.grpcDialOption, node, t.volumeID, t.collection, allShards); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Sprintf("%s: %v", node, err))
			t.GetLogger().WithFields(map[string]interface{}{
				"volume_id":   t.volumeID,
				"destination": node,
				"error":       err.Error(),
			}).Error("Failed to clear stale EC shards on destination")
		}
	}

	if len(cleanupErrors) > 0 {
		return fmt.Errorf("stale EC shard cleanup failed on %d destination(s): %s",
			len(cleanupErrors), strings.Join(cleanupErrors, "; "))
	}
	return nil
}

// fullShardIdRange builds [0..total-1] for unmount/delete RPCs. Falls back
// to erasure_coding.TotalShardsCount when the task's ratio is unset (early
// callers, tests); the helper never returns an empty slice.
func fullShardIdRange(dataShards, parityShards int32) []uint32 {
	total := int(dataShards + parityShards)
	if total <= 0 {
		total = erasure_coding.TotalShardsCount
	}
	if total > erasure_coding.MaxShardCount {
		total = erasure_coding.MaxShardCount
	}
	ids := make([]uint32, total)
	for i := range ids {
		ids[i] = uint32(i)
	}
	return ids
}

// unmountAndDeleteEcShards unmounts then deletes the named shards on one
// destination. Unmount must precede delete (delete requires the shard be
// unmounted); both RPCs are idempotent against missing shards.
func unmountAndDeleteEcShards(
	ctx context.Context,
	dialOption grpc.DialOption,
	destination string,
	volumeID uint32,
	collection string,
	shardIds []uint32,
) error {
	return operation.WithVolumeServerClient(false, pb.ServerAddress(destination), dialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			if _, err := client.VolumeEcShardsUnmount(ctx, &volume_server_pb.VolumeEcShardsUnmountRequest{
				VolumeId: volumeID,
				ShardIds: shardIds,
			}); err != nil {
				return fmt.Errorf("unmount: %w", err)
			}
			if _, err := client.VolumeEcShardsDelete(ctx, &volume_server_pb.VolumeEcShardsDeleteRequest{
				VolumeId:   volumeID,
				Collection: collection,
				ShardIds:   shardIds,
			}); err != nil {
				return fmt.Errorf("delete: %w", err)
			}
			return nil
		})
}

// verifyDatIdxConsistency checks that all .idx entries reference data within the
// .dat file. Since .dat and .idx are copied as separate network transfers, the
// .idx may have entries from writes that landed after the .dat was copied.
func verifyDatIdxConsistency(datFile, idxFile string) error {
	datInfo, err := os.Stat(datFile)
	if err != nil {
		return fmt.Errorf("stat dat file: %v", err)
	}
	datSize := datInfo.Size()

	// Read volume version from superblock to compute actual needle sizes
	df, err := os.Open(datFile)
	if err != nil {
		return fmt.Errorf("open dat file: %v", err)
	}
	defer df.Close()

	versionBytes := make([]byte, 1)
	if _, err := df.ReadAt(versionBytes, 0); err != nil {
		return fmt.Errorf("read version byte: %v", err)
	}
	version := needle.Version(versionBytes[0])

	idxF, err := os.Open(idxFile)
	if err != nil {
		return fmt.Errorf("open idx file: %v", err)
	}
	defer idxF.Close()

	var maxEnd int64
	var maxEndNeedleId storagetypes.NeedleId
	var entryCount int64
	err = idx.WalkIndexFile(idxF, 0, func(key storagetypes.NeedleId, offset storagetypes.Offset, size storagetypes.Size) error {
		entryCount++
		if size.IsDeleted() {
			return nil
		}
		end := offset.ToActualOffset() + needle.GetActualSize(size, version)
		if end > maxEnd {
			maxEnd = end
			maxEndNeedleId = key
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk idx file: %v", err)
	}

	if maxEnd > datSize {
		return fmt.Errorf(
			"idx references data beyond dat file: needle %d ends at offset %d but dat file is only %d bytes (%d entries total)",
			maxEndNeedleId, maxEnd, datSize, entryCount,
		)
	}

	glog.V(1).Infof("dat/idx consistency check passed: %d entries, max offset %d, dat size %d", entryCount, maxEnd, datSize)
	return nil
}
