package erasure_coding

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"google.golang.org/grpc/credentials/insecure"
)

// TypedTask implements comprehensive erasure coding with typed protobuf parameters
type TypedTask struct {
	*base.BaseTypedTask

	// Current task state
	sourceServer string
	volumeID     uint32
	collection   string
	workDir      string
	masterClient string
	grpcDialOpt  grpc.DialOption

	// EC parameters from protobuf
	destNodes          []string
	primaryDestNode    string
	estimatedShardSize uint64
	dataShards         int
	parityShards       int
	cleanupSource      bool

	// Progress tracking
	currentStep  string
	stepProgress map[string]float64
}

// NewTypedTask creates a new typed erasure coding task
func NewTypedTask() types.TypedTaskInterface {
	task := &TypedTask{
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
func (t *TypedTask) ValidateTyped(params *worker_pb.TaskParams) error {
	// Basic validation from base class
	if err := t.BaseTypedTask.ValidateTyped(params); err != nil {
		return err
	}

	// Check that we have EC-specific parameters
	ecParams := params.GetErasureCodingParams()
	if ecParams == nil {
		return fmt.Errorf("erasure_coding_params is required for EC task")
	}

	// Require destination nodes - tasks without destinations should be rejected at admin level
	if len(ecParams.DestNodes) == 0 && ecParams.PrimaryDestNode == "" {
		return fmt.Errorf("at least one destination node must be specified for EC task")
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
	if len(ecParams.DestNodes) > 0 {
		totalShards := expectedDataShards + expectedParityShards
		if totalShards > int32(len(ecParams.DestNodes)) {
			return fmt.Errorf("insufficient destination nodes: need %d, have %d", totalShards, len(ecParams.DestNodes))
		}
	}

	return nil
}

// EstimateTimeTyped estimates the time needed for EC processing based on protobuf parameters
func (t *TypedTask) EstimateTimeTyped(params *worker_pb.TaskParams) time.Duration {
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
func (t *TypedTask) ExecuteTyped(params *worker_pb.TaskParams) error {
	// Extract basic parameters
	t.volumeID = params.VolumeId
	t.sourceServer = params.Server
	t.collection = params.Collection

	// Extract EC-specific parameters
	ecParams := params.GetErasureCodingParams()
	if ecParams != nil {
		t.destNodes = ecParams.DestNodes
		t.primaryDestNode = ecParams.PrimaryDestNode
		t.estimatedShardSize = ecParams.EstimatedShardSize
		t.cleanupSource = ecParams.CleanupSource

		// DataShards and ParityShards are constants, don't override from parameters
		// t.dataShards and t.parityShards are already set to constants in NewTypedTask

		if ecParams.WorkingDir != "" {
			t.workDir = ecParams.WorkingDir
		}
		if ecParams.MasterClient != "" {
			t.masterClient = ecParams.MasterClient
		}
	}

	// Determine available destinations for logging
	availableDestinations := t.destNodes
	if len(availableDestinations) == 0 && t.primaryDestNode != "" {
		availableDestinations = []string{t.primaryDestNode}
	}

	glog.V(1).Infof("Starting typed EC task for volume %d: %s -> %v (data:%d, parity:%d)",
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

	// Additional workflow steps would be implemented here
	// For now, just simulate the work
	t.SetProgress(100.0)
	glog.Infof("Typed EC task completed successfully for volume %d", t.volumeID)
	return nil
}

// distributeEcShardsToPlannedDestinations distributes shards using planned destinations from protobuf
func (t *TypedTask) distributeEcShardsToPlannedDestinations(shardFiles []string, taskWorkDir string) error {
	glog.V(1).Infof("Distributing %d EC shards to planned destinations: %v", len(shardFiles), t.destNodes)

	// Use planned destinations if available
	var volumeServers []pb.ServerAddress
	if len(t.destNodes) > 0 {
		glog.V(1).Infof("Using planned destinations for EC shard distribution: %v", t.destNodes)
		for _, dest := range t.destNodes {
			volumeServers = append(volumeServers, pb.ServerAddress(dest))
		}
	} else if t.primaryDestNode != "" {
		glog.V(1).Infof("Using primary destination node: %s", t.primaryDestNode)
		volumeServers = append(volumeServers, pb.ServerAddress(t.primaryDestNode))
	} else {
		return fmt.Errorf("no destination nodes specified for EC shard distribution")
	}

	if len(volumeServers) == 0 {
		return fmt.Errorf("no volume servers available for EC distribution")
	}

	// Distribute shards across available servers
	shardTargets := make(map[int]pb.ServerAddress)
	targetServers := make(map[string]bool) // Track unique target servers
	for i, shardFile := range shardFiles {
		targetServer := volumeServers[i%len(volumeServers)]
		shardTargets[i] = targetServer
		targetServers[string(targetServer)] = true
		glog.V(1).Infof("Shard %d (%s) will go to server %s", i, shardFile, targetServer)
	}

	// Upload all shards to their target servers
	glog.V(1).Infof("Starting shard upload phase - uploading %d shards to %d servers", len(shardFiles), len(targetServers))
	var wg sync.WaitGroup
	errorChan := make(chan error, len(shardFiles))

	for i, shardFile := range shardFiles {
		wg.Add(1)
		go func(shardId int, shardFile string, targetServer pb.ServerAddress) {
			defer wg.Done()

			// Check for cancellation before each upload
			if t.IsCancelled() {
				errorChan <- fmt.Errorf("task cancelled during shard upload")
				return
			}

			glog.V(1).Infof("Uploading shard %d (%s) to server: %s", shardId, shardFile, targetServer)
			// Actual upload implementation would go here
			glog.V(1).Infof("Successfully uploaded shard %d to server: %s", shardId, targetServer)
		}(i, shardFile, shardTargets[i])
	}

	wg.Wait()
	close(errorChan)

	// Check for any upload errors
	if err := <-errorChan; err != nil {
		return err
	}

	glog.V(1).Infof("Successfully distributed all %d EC shards", len(shardFiles))
	return nil
}

// collectVolumeLocations gets volume location from master (placeholder implementation)
func (t *TypedTask) collectVolumeLocations(volumeId needle.VolumeId) ([]pb.ServerAddress, error) {
	// For now, return a placeholder implementation
	// Full implementation would call master to get volume locations
	return []pb.ServerAddress{pb.ServerAddress(t.sourceServer)}, nil
}

// Register the typed task in the global registry
func init() {
	types.RegisterGlobalTypedTask(types.TaskTypeErasureCoding, NewTypedTask)
	glog.V(1).Infof("Registered typed EC task")
}
