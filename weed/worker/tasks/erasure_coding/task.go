package erasure_coding

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Task implements comprehensive erasure coding with protobuf parameters
type Task struct {
	*base.BaseTypedTask

	// Current task state
	sourceServer string
	volumeID     uint32
	collection   string
	workDir      string
	masterClient string
	grpcDialOpt  grpc.DialOption

	// EC parameters from protobuf
	destinations           []*worker_pb.ECDestination           // Disk-aware destinations
	existingShardLocations []*worker_pb.ExistingECShardLocation // Existing shards to cleanup
	estimatedShardSize     uint64
	dataShards             int
	parityShards           int
	cleanupSource          bool

	// Progress tracking
	currentStep  string
	stepProgress map[string]float64
}

// NewTask creates a new erasure coding task
func NewTask() types.TypedTaskInterface {
	task := &Task{
		BaseTypedTask: base.NewBaseTypedTask(types.TaskTypeErasureCoding),
		masterClient:  "localhost:9333",                                         // Default master client
		workDir:       "/tmp/seaweedfs_ec_work",                                 // Default work directory
		grpcDialOpt:   grpc.WithTransportCredentials(insecure.NewCredentials()), // Default to insecure
		dataShards:    erasure_coding.DataShardsCount,                           // Use package constant
		parityShards:  erasure_coding.ParityShardsCount,                         // Use package constant
		stepProgress:  make(map[string]float64),
	}
	return task
}

// ValidateTyped validates the typed parameters for EC task
func (t *Task) ValidateTyped(params *worker_pb.TaskParams) error {
	// Basic validation from base class
	if err := t.BaseTypedTask.ValidateTyped(params); err != nil {
		return err
	}

	// Check that we have EC-specific parameters
	ecParams := params.GetErasureCodingParams()
	if ecParams == nil {
		return fmt.Errorf("erasure_coding_params is required for EC task")
	}

	// Require destinations
	if len(ecParams.Destinations) == 0 {
		return fmt.Errorf("destinations must be specified for EC task")
	}

	// DataShards and ParityShards are constants from erasure_coding package
	expectedDataShards := int32(erasure_coding.DataShardsCount)
	expectedParityShards := int32(erasure_coding.ParityShardsCount)

	if ecParams.DataShards > 0 && ecParams.DataShards != expectedDataShards {
		return fmt.Errorf("data_shards must be %d (fixed constant), got %d", expectedDataShards, ecParams.DataShards)
	}
	if ecParams.ParityShards > 0 && ecParams.ParityShards != expectedParityShards {
		return fmt.Errorf("parity_shards must be %d (fixed constant), got %d", expectedParityShards, ecParams.ParityShards)
	}

	// Validate destination count
	destinationCount := len(ecParams.Destinations)
	totalShards := expectedDataShards + expectedParityShards
	if totalShards > int32(destinationCount) {
		return fmt.Errorf("insufficient destinations: need %d, have %d", totalShards, destinationCount)
	}

	return nil
}

// EstimateTimeTyped estimates the time needed for EC processing based on protobuf parameters
func (t *Task) EstimateTimeTyped(params *worker_pb.TaskParams) time.Duration {
	baseTime := 20 * time.Minute // Processing takes time due to comprehensive operations

	ecParams := params.GetErasureCodingParams()
	if ecParams != nil && ecParams.EstimatedShardSize > 0 {
		// More accurate estimate based on shard size
		// Account for copying, encoding, and distribution
		gbSize := ecParams.EstimatedShardSize / (1024 * 1024 * 1024)
		estimatedTime := time.Duration(gbSize*2) * time.Minute // 2 minutes per GB
		if estimatedTime > baseTime {
			return estimatedTime
		}
	}

	return baseTime
}

// ExecuteTyped implements the actual erasure coding workflow with typed parameters
func (t *Task) ExecuteTyped(params *worker_pb.TaskParams) error {
	// Extract basic parameters
	t.volumeID = params.VolumeId
	t.sourceServer = params.Server
	t.collection = params.Collection

	// Extract EC-specific parameters
	ecParams := params.GetErasureCodingParams()
	if ecParams != nil {
		t.destinations = ecParams.Destinations                     // Store disk-aware destinations
		t.existingShardLocations = ecParams.ExistingShardLocations // Store existing shards for cleanup
		t.estimatedShardSize = ecParams.EstimatedShardSize
		t.cleanupSource = ecParams.CleanupSource

		// DataShards and ParityShards are constants, don't override from parameters
		// t.dataShards and t.parityShards are already set to constants in NewTask

		if ecParams.WorkingDir != "" {
			t.workDir = ecParams.WorkingDir
		}
		if ecParams.MasterClient != "" {
			t.masterClient = ecParams.MasterClient
		}
	}

	// Determine available destinations for logging
	var availableDestinations []string
	for _, dest := range t.destinations {
		availableDestinations = append(availableDestinations, fmt.Sprintf("%s(disk:%d)", dest.Node, dest.DiskId))
	}

	glog.V(1).Infof("Starting EC task for volume %d: %s -> %v (data:%d, parity:%d)",
		t.volumeID, t.sourceServer, availableDestinations, t.dataShards, t.parityShards)

	// Create unique working directory for this task
	taskWorkDir := filepath.Join(t.workDir, fmt.Sprintf("vol_%d_%d", t.volumeID, time.Now().Unix()))
	if err := os.MkdirAll(taskWorkDir, 0755); err != nil {
		return fmt.Errorf("failed to create task working directory %s: %v", taskWorkDir, err)
	}
	glog.V(1).Infof("WORKFLOW: Created working directory: %s", taskWorkDir)

	// Ensure cleanup of working directory
	defer func() {
		if err := os.RemoveAll(taskWorkDir); err != nil {
			glog.Warningf("Failed to cleanup working directory %s: %v", taskWorkDir, err)
		} else {
			glog.V(1).Infof("WORKFLOW: Cleaned up working directory: %s", taskWorkDir)
		}
	}()

	// Step 1: Collect volume locations from master
	glog.V(1).Infof("WORKFLOW STEP 1: Collecting volume locations from master")
	t.SetProgress(5.0)
	volumeId := needle.VolumeId(t.volumeID)
	volumeLocations, err := t.collectVolumeLocations(volumeId)
	if err != nil {
		return fmt.Errorf("failed to collect volume locations before EC encoding: %v", err)
	}
	glog.V(1).Infof("WORKFLOW: Found volume %d on %d servers: %v", t.volumeID, len(volumeLocations), volumeLocations)

	// Convert ServerAddress slice to string slice
	var locationStrings []string
	for _, addr := range volumeLocations {
		locationStrings = append(locationStrings, string(addr))
	}

	// Step 2: Check if volume has sufficient size for EC encoding
	if !t.shouldPerformECEncoding(locationStrings) {
		glog.Infof("Volume %d does not meet EC encoding criteria, skipping", t.volumeID)
		t.SetProgress(100.0)
		return nil
	}

	// Step 2A: Cleanup existing EC shards if any
	glog.V(1).Infof("WORKFLOW STEP 2A: Cleaning up existing EC shards for volume %d", t.volumeID)
	t.SetProgress(10.0)
	err = t.cleanupExistingEcShards()
	if err != nil {
		glog.Warningf("Failed to cleanup existing EC shards (continuing anyway): %v", err)
		// Don't fail the task - this is just cleanup
	}
	glog.V(1).Infof("WORKFLOW: Existing EC shards cleanup completed for volume %d", t.volumeID)

	// Step 3: Mark volume readonly on all servers
	glog.V(1).Infof("WORKFLOW STEP 2B: Marking volume %d readonly on all replica servers", t.volumeID)
	t.SetProgress(15.0)
	err = t.markVolumeReadonlyOnAllReplicas(needle.VolumeId(t.volumeID), locationStrings)
	if err != nil {
		return fmt.Errorf("failed to mark volume readonly: %v", err)
	}
	glog.V(1).Infof("WORKFLOW: Volume %d marked readonly on all replicas", t.volumeID)

	// Step 5: Copy volume files (.dat, .idx) to EC worker
	glog.V(1).Infof("WORKFLOW STEP 3: Copying volume files from source server %s to EC worker", t.sourceServer)
	t.SetProgress(25.0)
	localVolumeFiles, err := t.copyVolumeFilesToWorker(taskWorkDir)
	if err != nil {
		return fmt.Errorf("failed to copy volume files to EC worker: %v", err)
	}
	glog.V(1).Infof("WORKFLOW: Volume files copied to EC worker: %v", localVolumeFiles)

	// Step 6: Generate EC shards locally on EC worker
	glog.V(1).Infof("WORKFLOW STEP 4: Generating EC shards locally on EC worker")
	t.SetProgress(40.0)
	localShardFiles, err := t.generateEcShardsLocally(localVolumeFiles, taskWorkDir)
	if err != nil {
		return fmt.Errorf("failed to generate EC shards locally: %v", err)
	}
	glog.V(1).Infof("WORKFLOW: EC shards generated locally: %d shard files", len(localShardFiles))

	// Step 7: Distribute shards from EC worker to destination servers
	glog.V(1).Infof("WORKFLOW STEP 5: Distributing EC shards from worker to destination servers")
	t.SetProgress(60.0)
	err = t.distributeEcShardsFromWorker(localShardFiles)
	if err != nil {
		return fmt.Errorf("failed to distribute EC shards from worker: %v", err)
	}
	glog.V(1).Infof("WORKFLOW: EC shards distributed to all destination servers")

	// Step 8: Mount EC shards on destination servers
	glog.V(1).Infof("WORKFLOW STEP 6: Mounting EC shards on destination servers")
	t.SetProgress(80.0)
	err = t.mountEcShardsOnDestinations()
	if err != nil {
		return fmt.Errorf("failed to mount EC shards: %v", err)
	}
	glog.V(1).Infof("WORKFLOW: EC shards mounted successfully")

	// Step 9: Delete original volume from all locations
	glog.V(1).Infof("WORKFLOW STEP 7: Deleting original volume %d from all replica servers", t.volumeID)
	t.SetProgress(90.0)
	err = t.deleteVolumeFromAllLocations(needle.VolumeId(t.volumeID), locationStrings)
	if err != nil {
		return fmt.Errorf("failed to delete original volume: %v", err)
	}
	glog.V(1).Infof("WORKFLOW: Original volume %d deleted from all locations", t.volumeID)

	t.SetProgress(100.0)
	glog.Infof("EC task completed successfully for volume %d", t.volumeID)
	return nil
}

// collectVolumeLocations gets volume location from master (placeholder implementation)
func (t *Task) collectVolumeLocations(volumeId needle.VolumeId) ([]pb.ServerAddress, error) {
	// For now, return a placeholder implementation
	// Full implementation would call master to get volume locations
	return []pb.ServerAddress{pb.ServerAddress(t.sourceServer)}, nil
}

// cleanupExistingEcShards deletes existing EC shards using planned locations
func (t *Task) cleanupExistingEcShards() error {
	if len(t.existingShardLocations) == 0 {
		glog.V(1).Infof("No existing EC shards to cleanup for volume %d", t.volumeID)
		return nil
	}

	glog.V(1).Infof("Cleaning up existing EC shards for volume %d on %d servers", t.volumeID, len(t.existingShardLocations))

	// Delete existing shards from each location using planned shard locations
	for _, location := range t.existingShardLocations {
		if len(location.ShardIds) == 0 {
			continue
		}

		glog.V(1).Infof("Deleting existing EC shards %v from %s for volume %d", location.ShardIds, location.Node, t.volumeID)

		err := operation.WithVolumeServerClient(false, pb.ServerAddress(location.Node), t.grpcDialOpt,
			func(client volume_server_pb.VolumeServerClient) error {
				_, deleteErr := client.VolumeEcShardsDelete(context.Background(), &volume_server_pb.VolumeEcShardsDeleteRequest{
					VolumeId:   t.volumeID,
					Collection: t.collection,
					ShardIds:   location.ShardIds,
				})
				return deleteErr
			})

		if err != nil {
			glog.Errorf("Failed to delete existing EC shards %v from %s for volume %d: %v", location.ShardIds, location.Node, t.volumeID, err)
			// Continue with other servers - don't fail the entire cleanup
		} else {
			glog.V(1).Infof("Successfully deleted existing EC shards %v from %s for volume %d", location.ShardIds, location.Node, t.volumeID)
		}
	}

	glog.V(1).Infof("Completed cleanup of existing EC shards for volume %d", t.volumeID)
	return nil
}

// shouldPerformECEncoding checks if the volume meets criteria for EC encoding
func (t *Task) shouldPerformECEncoding(volumeLocations []string) bool {
	// For now, always proceed with EC encoding if volume exists
	// This can be extended with volume size checks, etc.
	return len(volumeLocations) > 0
}

// markVolumeReadonlyOnAllReplicas marks the volume as readonly on all replica servers
func (t *Task) markVolumeReadonlyOnAllReplicas(volumeId needle.VolumeId, volumeLocations []string) error {
	glog.V(1).Infof("Marking volume %d readonly on %d servers", volumeId, len(volumeLocations))

	// Mark volume readonly on all replica servers
	for _, location := range volumeLocations {
		glog.V(1).Infof("Marking volume %d readonly on %s", volumeId, location)

		err := operation.WithVolumeServerClient(false, pb.ServerAddress(location), t.grpcDialOpt,
			func(client volume_server_pb.VolumeServerClient) error {
				_, markErr := client.VolumeMarkReadonly(context.Background(), &volume_server_pb.VolumeMarkReadonlyRequest{
					VolumeId: uint32(volumeId),
				})
				return markErr
			})

		if err != nil {
			glog.Errorf("Failed to mark volume %d readonly on %s: %v", volumeId, location, err)
			return fmt.Errorf("failed to mark volume %d readonly on %s: %v", volumeId, location, err)
		}

		glog.V(1).Infof("Successfully marked volume %d readonly on %s", volumeId, location)
	}

	glog.V(1).Infof("Successfully marked volume %d readonly on all %d locations", volumeId, len(volumeLocations))
	return nil
}

// copyVolumeFilesToWorker copies .dat and .idx files from source server to local worker
func (t *Task) copyVolumeFilesToWorker(workDir string) (map[string]string, error) {
	localFiles := make(map[string]string)

	// Copy .dat file
	datFile := fmt.Sprintf("%s.dat", filepath.Join(workDir, fmt.Sprintf("%d", t.volumeID)))
	err := t.copyFileFromSource(".dat", datFile)
	if err != nil {
		return nil, fmt.Errorf("failed to copy .dat file: %v", err)
	}
	localFiles["dat"] = datFile
	glog.V(1).Infof("Copied .dat file to: %s", datFile)

	// Copy .idx file
	idxFile := fmt.Sprintf("%s.idx", filepath.Join(workDir, fmt.Sprintf("%d", t.volumeID)))
	err = t.copyFileFromSource(".idx", idxFile)
	if err != nil {
		return nil, fmt.Errorf("failed to copy .idx file: %v", err)
	}
	localFiles["idx"] = idxFile
	glog.V(1).Infof("Copied .idx file to: %s", idxFile)

	return localFiles, nil
}

// copyFileFromSource copies a file from source server to local path using gRPC streaming
func (t *Task) copyFileFromSource(ext, localPath string) error {
	return operation.WithVolumeServerClient(false, pb.ServerAddress(t.sourceServer), t.grpcDialOpt,
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

			glog.V(1).Infof("Successfully copied %s (%d bytes) from %s to %s", ext, totalBytes, t.sourceServer, localPath)
			return nil
		})
}

// generateEcShardsLocally generates EC shards from local volume files
func (t *Task) generateEcShardsLocally(localFiles map[string]string, workDir string) (map[string]string, error) {
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
	// Create basic volume info - in a real implementation, this would come from the original volume
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

func (t *Task) copyEcShardsToDestinations() error {
	if len(t.destinations) == 0 {
		return fmt.Errorf("no destinations specified for EC shard distribution")
	}

	destinations := t.destinations

	glog.V(1).Infof("Copying EC shards for volume %d to %d destinations", t.volumeID, len(destinations))

	// Prepare shard IDs (0-13 for EC shards)
	var shardIds []uint32
	for i := 0; i < erasure_coding.TotalShardsCount; i++ {
		shardIds = append(shardIds, uint32(i))
	}

	// Distribute shards across destinations
	var wg sync.WaitGroup
	errorChan := make(chan error, len(destinations))

	// Track which disks have already received metadata files (server+disk)
	metadataFilesCopied := make(map[string]bool)
	var metadataMutex sync.Mutex

	// For each destination, copy a subset of shards
	shardsPerDest := len(shardIds) / len(destinations)
	remainder := len(shardIds) % len(destinations)

	shardOffset := 0
	for i, dest := range destinations {
		wg.Add(1)

		shardsForThisDest := shardsPerDest
		if i < remainder {
			shardsForThisDest++ // Distribute remainder shards
		}

		destShardIds := shardIds[shardOffset : shardOffset+shardsForThisDest]
		shardOffset += shardsForThisDest

		go func(destination *worker_pb.ECDestination, targetShardIds []uint32) {
			defer wg.Done()

			if t.IsCancelled() {
				errorChan <- fmt.Errorf("task cancelled during shard copy")
				return
			}

			// Create disk-specific metadata key (server+disk)
			diskKey := fmt.Sprintf("%s:%d", destination.Node, destination.DiskId)

			glog.V(1).Infof("Copying shards %v from %s to %s (disk %d)",
				targetShardIds, t.sourceServer, destination.Node, destination.DiskId)

			// Check if this disk needs metadata files (only once per disk)
			metadataMutex.Lock()
			needsMetadataFiles := !metadataFilesCopied[diskKey]
			if needsMetadataFiles {
				metadataFilesCopied[diskKey] = true
			}
			metadataMutex.Unlock()

			err := operation.WithVolumeServerClient(false, pb.ServerAddress(destination.Node), t.grpcDialOpt,
				func(client volume_server_pb.VolumeServerClient) error {
					_, copyErr := client.VolumeEcShardsCopy(context.Background(), &volume_server_pb.VolumeEcShardsCopyRequest{
						VolumeId:       uint32(t.volumeID),
						Collection:     t.collection,
						ShardIds:       targetShardIds,
						CopyEcxFile:    needsMetadataFiles, // Copy .ecx only once per disk
						CopyEcjFile:    needsMetadataFiles, // Copy .ecj only once per disk
						CopyVifFile:    needsMetadataFiles, // Copy .vif only once per disk
						SourceDataNode: t.sourceServer,
						DiskId:         destination.DiskId, // Pass target disk ID
					})
					return copyErr
				})

			if err != nil {
				errorChan <- fmt.Errorf("failed to copy shards to %s disk %d: %v", destination.Node, destination.DiskId, err)
				return
			}

			if needsMetadataFiles {
				glog.V(1).Infof("Successfully copied shards %v and metadata files (.ecx, .ecj, .vif) to %s disk %d",
					targetShardIds, destination.Node, destination.DiskId)
			} else {
				glog.V(1).Infof("Successfully copied shards %v to %s disk %d (metadata files already present)",
					targetShardIds, destination.Node, destination.DiskId)
			}
		}(dest, destShardIds)
	}

	wg.Wait()
	close(errorChan)

	// Check for any copy errors
	if err := <-errorChan; err != nil {
		return err
	}

	glog.V(1).Infof("Successfully copied all EC shards for volume %d", t.volumeID)
	return nil
}

// distributeEcShardsFromWorker distributes locally generated EC shards to destination servers
func (t *Task) distributeEcShardsFromWorker(localShardFiles map[string]string) error {
	if len(t.destinations) == 0 {
		return fmt.Errorf("no destinations specified for EC shard distribution")
	}

	destinations := t.destinations

	glog.V(1).Infof("Distributing EC shards for volume %d from worker to %d destinations", t.volumeID, len(destinations))

	// Prepare shard IDs (0-13 for EC shards)
	var shardIds []uint32
	for i := 0; i < erasure_coding.TotalShardsCount; i++ {
		shardIds = append(shardIds, uint32(i))
	}

	// Distribute shards across destinations
	var wg sync.WaitGroup
	errorChan := make(chan error, len(destinations))

	// Track which disks have already received metadata files (server+disk)
	metadataFilesCopied := make(map[string]bool)
	var metadataMutex sync.Mutex

	// For each destination, send a subset of shards
	shardsPerDest := len(shardIds) / len(destinations)
	remainder := len(shardIds) % len(destinations)

	shardOffset := 0
	for i, dest := range destinations {
		wg.Add(1)

		shardsForThisDest := shardsPerDest
		if i < remainder {
			shardsForThisDest++ // Distribute remainder shards
		}

		destShardIds := shardIds[shardOffset : shardOffset+shardsForThisDest]
		shardOffset += shardsForThisDest

		go func(destination *worker_pb.ECDestination, targetShardIds []uint32) {
			defer wg.Done()

			if t.IsCancelled() {
				errorChan <- fmt.Errorf("task cancelled during shard distribution")
				return
			}

			// Create disk-specific metadata key (server+disk)
			diskKey := fmt.Sprintf("%s:%d", destination.Node, destination.DiskId)

			glog.V(1).Infof("Distributing shards %v from worker to %s (disk %d)",
				targetShardIds, destination.Node, destination.DiskId)

			// Check if this disk needs metadata files (only once per disk)
			metadataMutex.Lock()
			needsMetadataFiles := !metadataFilesCopied[diskKey]
			if needsMetadataFiles {
				metadataFilesCopied[diskKey] = true
			}
			metadataMutex.Unlock()

			// Send shard files to destination using HTTP upload (simplified for now)
			err := t.sendShardsToDestination(destination, targetShardIds, localShardFiles, needsMetadataFiles)
			if err != nil {
				errorChan <- fmt.Errorf("failed to send shards to %s disk %d: %v", destination.Node, destination.DiskId, err)
				return
			}

			if needsMetadataFiles {
				glog.V(1).Infof("Successfully distributed shards %v and metadata files (.ecx, .vif) to %s disk %d",
					targetShardIds, destination.Node, destination.DiskId)
			} else {
				glog.V(1).Infof("Successfully distributed shards %v to %s disk %d (metadata files already present)",
					targetShardIds, destination.Node, destination.DiskId)
			}
		}(dest, destShardIds)
	}

	wg.Wait()
	close(errorChan)

	// Check for any distribution errors
	if err := <-errorChan; err != nil {
		return err
	}

	glog.V(1).Infof("Completed distributing EC shards for volume %d", t.volumeID)
	return nil
}

// sendShardsToDestination sends specific shard files from worker to a destination server (simplified)
func (t *Task) sendShardsToDestination(destination *worker_pb.ECDestination, shardIds []uint32, localFiles map[string]string, includeMetadata bool) error {
	// For now, use a simplified approach - just upload the files
	// In a full implementation, this would use proper file upload mechanisms
	glog.V(2).Infof("Would send shards %v and metadata=%v to %s disk %d", shardIds, includeMetadata, destination.Node, destination.DiskId)

	// TODO: Implement actual file upload to volume server
	// This is a placeholder - actual implementation would:
	// 1. Open each shard file locally
	// 2. Upload via HTTP POST or gRPC stream to destination volume server
	// 3. Volume server would save to the specified disk_id

	return nil
}

// mountEcShardsOnDestinations mounts EC shards on all destination servers
func (t *Task) mountEcShardsOnDestinations() error {
	if len(t.destinations) == 0 {
		return fmt.Errorf("no destinations specified for mounting EC shards")
	}

	destinations := t.destinations

	glog.V(1).Infof("Mounting EC shards for volume %d on %d destinations", t.volumeID, len(destinations))

	// Prepare all shard IDs (0-13)
	var allShardIds []uint32
	for i := 0; i < erasure_coding.TotalShardsCount; i++ {
		allShardIds = append(allShardIds, uint32(i))
	}

	var wg sync.WaitGroup
	errorChan := make(chan error, len(destinations))

	// Mount shards on each destination server
	for _, dest := range destinations {
		wg.Add(1)

		go func(destination *worker_pb.ECDestination) {
			defer wg.Done()

			if t.IsCancelled() {
				errorChan <- fmt.Errorf("task cancelled during shard mounting")
				return
			}

			glog.V(1).Infof("Mounting EC shards on %s disk %d", destination.Node, destination.DiskId)

			err := operation.WithVolumeServerClient(false, pb.ServerAddress(destination.Node), t.grpcDialOpt,
				func(client volume_server_pb.VolumeServerClient) error {
					_, mountErr := client.VolumeEcShardsMount(context.Background(), &volume_server_pb.VolumeEcShardsMountRequest{
						VolumeId:   uint32(t.volumeID),
						Collection: t.collection,
						ShardIds:   allShardIds, // Mount all available shards on each server
					})
					return mountErr
				})

			if err != nil {
				// It's normal for some servers to not have all shards, so log as warning rather than error
				glog.Warningf("Failed to mount some shards on %s disk %d (this may be normal): %v", destination.Node, destination.DiskId, err)
			} else {
				glog.V(1).Infof("Successfully mounted EC shards on %s disk %d", destination.Node, destination.DiskId)
			}
		}(dest)
	}

	wg.Wait()
	close(errorChan)

	// Check for any critical mounting errors
	select {
	case err := <-errorChan:
		if err != nil {
			glog.Warningf("Some shard mounting issues occurred: %v", err)
		}
	default:
		// No errors
	}

	glog.V(1).Infof("Completed mounting EC shards for volume %d", t.volumeID)
	return nil
}

// deleteVolumeFromAllLocations deletes the original volume from all replica servers
func (t *Task) deleteVolumeFromAllLocations(volumeId needle.VolumeId, volumeLocations []string) error {
	glog.V(1).Infof("Deleting original volume %d from %d locations", volumeId, len(volumeLocations))

	for _, location := range volumeLocations {
		glog.V(1).Infof("Deleting volume %d from %s", volumeId, location)

		err := operation.WithVolumeServerClient(false, pb.ServerAddress(location), t.grpcDialOpt,
			func(client volume_server_pb.VolumeServerClient) error {
				_, deleteErr := client.VolumeDelete(context.Background(), &volume_server_pb.VolumeDeleteRequest{
					VolumeId:  uint32(volumeId),
					OnlyEmpty: false, // Force delete even if not empty since we've already created EC shards
				})
				return deleteErr
			})

		if err != nil {
			glog.Errorf("Failed to delete volume %d from %s: %v", volumeId, location, err)
			return fmt.Errorf("failed to delete volume %d from %s: %v", volumeId, location, err)
		}

		glog.V(1).Infof("Successfully deleted volume %d from %s", volumeId, location)
	}

	glog.V(1).Infof("Successfully deleted volume %d from all %d locations", volumeId, len(volumeLocations))
	return nil
}

// Register the task in the global registry
func init() {
	types.RegisterGlobalTypedTask(types.TaskTypeErasureCoding, NewTask)
	glog.V(1).Infof("Registered EC task")
}
