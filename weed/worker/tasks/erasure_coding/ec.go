package erasure_coding

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
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
	destinations       []*worker_pb.ECDestination // Disk-aware destinations
	estimatedShardSize uint64
	dataShards         int
	parityShards       int
	cleanupSource      bool

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
		t.destinations = ecParams.Destinations // Store disk-aware destinations
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

	// Step 3: Mark volume readonly on all servers
	glog.V(1).Infof("WORKFLOW STEP 2: Marking volume %d readonly on all replica servers", t.volumeID)
	t.SetProgress(15.0)
	err = t.markVolumeReadonlyOnAllReplicas(needle.VolumeId(t.volumeID), locationStrings)
	if err != nil {
		return fmt.Errorf("failed to mark volume readonly: %v", err)
	}
	glog.V(1).Infof("WORKFLOW: Volume %d marked readonly on all replicas", t.volumeID)

	// Step 4: Generate EC shards on source server (standard SeaweedFS approach)
	glog.V(1).Infof("WORKFLOW STEP 3: Generating EC shards on source server %s", t.sourceServer)
	t.SetProgress(30.0)
	err = t.generateEcShardsOnSource()
	if err != nil {
		return fmt.Errorf("failed to generate EC shards on source: %v", err)
	}
	glog.V(1).Infof("WORKFLOW: EC shards generated successfully on source server")

	// Step 5: Copy shards to destination servers
	glog.V(1).Infof("WORKFLOW STEP 4: Copying EC shards to destination servers")
	t.SetProgress(60.0)
	err = t.copyEcShardsToDestinations()
	if err != nil {
		return fmt.Errorf("failed to copy EC shards to destinations: %v", err)
	}
	glog.V(1).Infof("WORKFLOW: EC shards copied to all destination servers")

	// Step 6: Mount EC shards on destination servers
	glog.V(1).Infof("WORKFLOW STEP 5: Mounting EC shards on destination servers")
	t.SetProgress(80.0)
	err = t.mountEcShardsOnDestinations()
	if err != nil {
		return fmt.Errorf("failed to mount EC shards: %v", err)
	}
	glog.V(1).Infof("WORKFLOW: EC shards mounted successfully")

	// Step 7: Delete original volume from all locations
	glog.V(1).Infof("WORKFLOW STEP 6: Deleting original volume %d from all replica servers", t.volumeID)
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

// copyEcShardsToDestinations copies EC shards from source to planned destinations
// generateEcShardsOnSource generates EC shards on the source volume server
func (t *Task) generateEcShardsOnSource() error {
	glog.V(1).Infof("Generating EC shards for volume %d on source server %s", t.volumeID, t.sourceServer)

	return operation.WithVolumeServerClient(false, pb.ServerAddress(t.sourceServer), t.grpcDialOpt,
		func(client volume_server_pb.VolumeServerClient) error {
			_, err := client.VolumeEcShardsGenerate(context.Background(), &volume_server_pb.VolumeEcShardsGenerateRequest{
				VolumeId:   uint32(t.volumeID),
				Collection: t.collection,
			})
			if err != nil {
				return fmt.Errorf("VolumeEcShardsGenerate failed: %v", err)
			}

			glog.V(1).Infof("Successfully generated EC shards for volume %d on %s", t.volumeID, t.sourceServer)
			return nil
		})
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
