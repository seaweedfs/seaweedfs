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

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/admin/config"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"google.golang.org/grpc/credentials/insecure"
)

// Task implements comprehensive erasure coding with local processing and smart distribution
type Task struct {
	*tasks.BaseTask
	sourceServer string
	volumeID     uint32
	collection   string
	workDir      string
	masterClient string
	grpcDialOpt  grpc.DialOption

	// EC parameters
	dataShards   int // Default: 10
	parityShards int // Default: 4
	totalShards  int // Default: 14

	// Progress tracking
	currentStep  string
	stepProgress map[string]float64
}

// NewTask creates a new erasure coding task
func NewTask(sourceServer string, volumeID uint32) *Task {
	task := &Task{
		BaseTask:     tasks.NewBaseTask(types.TaskTypeErasureCoding),
		sourceServer: sourceServer,
		volumeID:     volumeID,
		masterClient: "localhost:9333",                                         // Default master client
		workDir:      "/tmp/seaweedfs_ec_work",                                 // Default work directory
		grpcDialOpt:  grpc.WithTransportCredentials(insecure.NewCredentials()), // Default to insecure
		dataShards:   10,
		parityShards: 4,
		totalShards:  14,
		stepProgress: make(map[string]float64),
	}
	return task
}

// NewTaskWithParams creates a new erasure coding task with custom parameters
func NewTaskWithParams(sourceServer string, volumeID uint32, masterClient string, workDir string) *Task {
	task := &Task{
		BaseTask:     tasks.NewBaseTask(types.TaskTypeErasureCoding),
		sourceServer: sourceServer,
		volumeID:     volumeID,
		masterClient: masterClient,
		workDir:      workDir,
		grpcDialOpt:  grpc.WithTransportCredentials(insecure.NewCredentials()), // Default to insecure
		dataShards:   10,
		parityShards: 4,
		totalShards:  14,
		stepProgress: make(map[string]float64),
	}
	return task
}

// SetDialOption allows setting a custom gRPC dial option
func (t *Task) SetDialOption(dialOpt grpc.DialOption) {
	t.grpcDialOpt = dialOpt
}

// Execute performs the EC operation following command_ec_encode.go pattern but with local processing
func (t *Task) Execute(params types.TaskParams) error {
	glog.V(1).Infof("Starting erasure coding for volume %d from server %s (download → ec → distribute)", t.volumeID, t.sourceServer)

	// Extract parameters - use the actual collection from task params
	t.collection = params.Collection

	// Override defaults with parameters if provided
	if mc, ok := params.Parameters["master_client"].(string); ok && mc != "" {
		t.masterClient = mc
	}
	if wd, ok := params.Parameters["work_dir"].(string); ok && wd != "" {
		t.workDir = wd
	}

	// Create unique working directory for this task to avoid conflicts
	// Use volume ID and timestamp to ensure uniqueness
	taskWorkDir := filepath.Join(t.workDir, fmt.Sprintf("vol_%d_%d", t.volumeID, time.Now().Unix()))

	// Create the task-specific working directory
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
	volumeId := needle.VolumeId(t.volumeID)
	volumeLocations, err := t.collectVolumeLocations(volumeId)
	if err != nil {
		return fmt.Errorf("failed to collect volume locations before EC encoding: %v", err)
	}
	glog.V(1).Infof("WORKFLOW: Found volume %d on %d servers: %v", t.volumeID, len(volumeLocations), volumeLocations)

	// Step 2: Mark volume readonly on all servers
	glog.V(1).Infof("WORKFLOW STEP 2: Marking volume %d readonly on all replica servers", t.volumeID)
	err = t.markVolumeReadonlyOnAllReplicas(volumeId, volumeLocations)
	if err != nil {
		return fmt.Errorf("failed to mark volume readonly: %v", err)
	}
	glog.V(1).Infof("WORKFLOW: Volume %d marked readonly on all replicas", t.volumeID)

	// Step 3: Copy volume data to local worker
	glog.V(1).Infof("WORKFLOW STEP 3: Downloading volume %d data to worker", t.volumeID)
	err = t.copyVolumeDataLocally(taskWorkDir)
	if err != nil {
		return fmt.Errorf("failed to copy volume data locally: %v", err)
	}
	glog.V(1).Infof("WORKFLOW: Volume %d data downloaded successfully", t.volumeID)

	// Step 4: Perform local EC encoding
	glog.V(1).Infof("WORKFLOW STEP 4: Performing local EC encoding for volume %d", t.volumeID)
	shardFiles, err := t.performLocalECEncoding(taskWorkDir)
	if err != nil {
		return fmt.Errorf("failed to generate EC shards locally: %v", err)
	}
	glog.V(1).Infof("WORKFLOW: Generated %d EC shards for volume %d", len(shardFiles), t.volumeID)

	// Step 5: Distribute shards across servers
	glog.V(1).Infof("WORKFLOW STEP 5: Distributing EC shards across cluster")
	err = t.distributeEcShardsAcrossServers(shardFiles, taskWorkDir)
	if err != nil {
		return fmt.Errorf("failed to distribute EC shards: %v", err)
	}
	glog.V(1).Infof("WORKFLOW: EC shards distributed and mounted successfully")

	// Step 6: Delete original volume from ALL replica locations (following command_ec_encode.go pattern)
	glog.V(1).Infof("WORKFLOW STEP 6: Deleting original volume %d from all replica servers", t.volumeID)
	err = t.deleteVolumeFromAllLocations(volumeId, volumeLocations)
	if err != nil {
		glog.Warningf("Failed to delete original volume %d from all locations: %v (may need manual cleanup)", t.volumeID, err)
		// This is not a critical failure - the EC encoding itself succeeded
	} else {
		glog.V(1).Infof("WORKFLOW: Original volume %d deleted from all replicas", t.volumeID)
	}

	// Step 7: Final success
	t.SetProgress(100.0)
	glog.V(1).Infof("WORKFLOW COMPLETE: Successfully completed erasure coding for volume %d", t.volumeID)
	return nil
}

// copyVolumeDataLocally downloads .dat and .idx files from source server to local working directory
func (t *Task) copyVolumeDataLocally(workDir string) error {
	t.currentStep = "copying"
	t.SetProgress(10.0)
	glog.V(1).Infof("Copying volume %d data from server %s to local directory %s", t.volumeID, t.sourceServer, workDir)

	// Connect to source volume server
	grpcAddress := pb.ServerToGrpcAddress(string(t.sourceServer))
	conn, err := grpc.NewClient(grpcAddress, t.grpcDialOpt)
	if err != nil {
		glog.Errorf("COPY ERROR: Failed to connect to source server %s: %v", t.sourceServer, err)
		return fmt.Errorf("failed to connect to source server %s: %v", t.sourceServer, err)
	}
	defer conn.Close()
	glog.V(1).Infof("COPY GRPC: Connected to source server %s", t.sourceServer)

	client := volume_server_pb.NewVolumeServerClient(conn)
	ctx := context.Background()

	// Copy .dat file
	datFile := filepath.Join(workDir, fmt.Sprintf("%d.dat", t.volumeID))
	glog.V(1).Infof("COPY START: Downloading .dat file for volume %d to %s", t.volumeID, datFile)
	err = t.copyVolumeFile(client, ctx, t.volumeID, ".dat", datFile, 0)
	if err != nil {
		glog.Errorf("COPY ERROR: Failed to copy .dat file: %v", err)
		return fmt.Errorf("failed to copy .dat file: %v", err)
	}

	// Verify .dat file was copied
	if datInfo, err := os.Stat(datFile); err != nil {
		glog.Errorf("COPY ERROR: .dat file not found after copy: %v", err)
		return fmt.Errorf(".dat file not found after copy: %v", err)
	} else {
		glog.V(1).Infof("COPY SUCCESS: .dat file copied successfully (%d bytes)", datInfo.Size())
	}

	// Copy .idx file
	idxFile := filepath.Join(workDir, fmt.Sprintf("%d.idx", t.volumeID))
	glog.V(1).Infof("COPY START: Downloading .idx file for volume %d to %s", t.volumeID, idxFile)
	err = t.copyVolumeFile(client, ctx, t.volumeID, ".idx", idxFile, 0)
	if err != nil {
		glog.Errorf("COPY ERROR: Failed to copy .idx file: %v", err)
		return fmt.Errorf("failed to copy .idx file: %v", err)
	}

	// Verify .idx file was copied
	if idxInfo, err := os.Stat(idxFile); err != nil {
		glog.Errorf("COPY ERROR: .idx file not found after copy: %v", err)
		return fmt.Errorf(".idx file not found after copy: %v", err)
	} else {
		glog.V(1).Infof("COPY SUCCESS: .idx file copied successfully (%d bytes)", idxInfo.Size())
	}

	t.SetProgress(15.0)
	glog.V(1).Infof("COPY COMPLETED: Successfully copied volume %d files (.dat and .idx) to %s", t.volumeID, workDir)
	return nil
}

// copyVolumeFile copies a specific volume file from source server
func (t *Task) copyVolumeFile(client volume_server_pb.VolumeServerClient, ctx context.Context,
	volumeID uint32, extension string, localPath string, expectedSize uint64) error {

	glog.V(2).Infof("FILE COPY START: Copying volume %d%s from source server", volumeID, extension)

	// Stream volume file data using CopyFile API with proper parameters (following shell implementation)
	stream, err := client.CopyFile(ctx, &volume_server_pb.CopyFileRequest{
		VolumeId:                 volumeID,
		Ext:                      extension,
		CompactionRevision:       math.MaxUint32, // Copy latest revision
		StopOffset:               math.MaxInt64,  // Copy entire file
		Collection:               t.collection,
		IsEcVolume:               false, // Regular volume, not EC volume
		IgnoreSourceFileNotFound: false, // Fail if source file not found
	})
	if err != nil {
		glog.Errorf("FILE COPY ERROR: Failed to start file copy stream for %s: %v", extension, err)
		return fmt.Errorf("failed to start file copy stream: %v", err)
	}
	glog.V(2).Infof("FILE COPY GRPC: Created copy stream for %s", extension)

	// Create local file
	file, err := os.Create(localPath)
	if err != nil {
		glog.Errorf("FILE COPY ERROR: Failed to create local file %s: %v", localPath, err)
		return fmt.Errorf("failed to create local file %s: %v", localPath, err)
	}
	defer file.Close()
	glog.V(2).Infof("FILE COPY LOCAL: Created local file %s", localPath)

	// Copy data with progress tracking
	var totalBytes int64
	chunkCount := 0
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			glog.V(2).Infof("FILE COPY COMPLETE: Finished streaming %s (%d bytes in %d chunks)", extension, totalBytes, chunkCount)
			break
		}
		if err != nil {
			glog.Errorf("FILE COPY ERROR: Failed to receive data for %s: %v", extension, err)
			return fmt.Errorf("failed to receive stream data: %v", err)
		}

		if len(resp.FileContent) > 0 {
			n, err := file.Write(resp.FileContent)
			if err != nil {
				glog.Errorf("FILE COPY ERROR: Failed to write to local file %s: %v", localPath, err)
				return fmt.Errorf("failed to write to local file: %v", err)
			}
			totalBytes += int64(n)
			chunkCount++
			glog.V(3).Infof("FILE COPY CHUNK: %s chunk %d written (%d bytes, total: %d)", extension, chunkCount, n, totalBytes)
		}
	}

	// Sync to disk
	err = file.Sync()
	if err != nil {
		glog.Warningf("FILE COPY WARNING: Failed to sync %s to disk: %v", localPath, err)
	}

	glog.V(2).Infof("FILE COPY SUCCESS: Successfully copied %s (%d bytes total)", extension, totalBytes)
	return nil
}

// performLocalECEncoding performs Reed-Solomon encoding on local volume files
func (t *Task) performLocalECEncoding(workDir string) ([]string, error) {
	t.currentStep = "encoding"
	t.SetProgress(30.0)
	glog.V(1).Infof("Performing local EC encoding for volume %d", t.volumeID)

	datFile := filepath.Join(workDir, fmt.Sprintf("%d.dat", t.volumeID))
	idxFile := filepath.Join(workDir, fmt.Sprintf("%d.idx", t.volumeID))

	// Check if files exist and get their sizes
	datInfo, err := os.Stat(datFile)
	if err != nil {
		return nil, fmt.Errorf("failed to stat dat file: %v", err)
	}

	idxInfo, err := os.Stat(idxFile)
	if err != nil {
		return nil, fmt.Errorf("failed to stat idx file: %v", err)
	}

	glog.V(1).Infof("Encoding files: %s (%d bytes), %s (%d bytes)",
		datFile, datInfo.Size(), idxFile, idxInfo.Size())

	// Handle empty volumes - this is a valid case that should not be EC encoded
	if datInfo.Size() == 0 {
		glog.Infof("Volume %d is empty (0 bytes), skipping EC encoding", t.volumeID)
		return nil, fmt.Errorf("volume %d is empty and cannot be EC encoded", t.volumeID)
	}

	// Use the existing volume files directly with the SeaweedFS EC library
	// The SeaweedFS EC library expects baseFileName without extension
	baseFileName := filepath.Join(workDir, fmt.Sprintf("%d", t.volumeID))

	glog.V(1).Infof("Starting EC encoding with base filename: %s", baseFileName)

	// Generate EC shards using SeaweedFS erasure coding library
	glog.V(1).Infof("Starting EC shard generation for volume %d", t.volumeID)
	err = erasure_coding.WriteEcFiles(baseFileName)
	if err != nil {
		return nil, fmt.Errorf("failed to write EC files: %v", err)
	}
	glog.V(1).Infof("Completed EC shard generation for volume %d", t.volumeID)

	// Generate .ecx file from .idx file
	glog.V(1).Infof("Creating .ecx index file for volume %d", t.volumeID)
	err = erasure_coding.WriteSortedFileFromIdx(baseFileName, ".ecx")
	if err != nil {
		return nil, fmt.Errorf("failed to write .ecx file: %v", err)
	}
	glog.V(1).Infof("Successfully created .ecx index file for volume %d", t.volumeID)

	// Create .ecj file (EC journal file) - initially empty for new EC volumes
	ecjFile := baseFileName + ".ecj"
	glog.V(1).Infof("Creating .ecj journal file: %s", ecjFile)
	ecjFileHandle, err := os.Create(ecjFile)
	if err != nil {
		return nil, fmt.Errorf("failed to create .ecj file: %v", err)
	}
	ecjFileHandle.Close()
	glog.V(1).Infof("Successfully created .ecj journal file: %s", ecjFile)

	// Create .vif file (volume info file) with basic volume information
	vifFile := baseFileName + ".vif"
	glog.V(1).Infof("Creating .vif volume info file: %s", vifFile)
	volumeInfo := &volume_server_pb.VolumeInfo{
		Version:     3,              // needle.Version3
		DatFileSize: datInfo.Size(), // int64
	}

	// Save volume info to .vif file using the standard SeaweedFS function
	err = volume_info.SaveVolumeInfo(vifFile, volumeInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to create .vif file: %v", err)
	}
	glog.V(1).Infof("Successfully created .vif volume info file: %s", vifFile)

	// Prepare list of generated shard files
	shardFiles := make([]string, t.totalShards)
	for i := 0; i < t.totalShards; i++ {
		shardFiles[i] = filepath.Join(workDir, fmt.Sprintf("%d.ec%02d", t.volumeID, i))
	}

	// Verify that ALL shards were created and log each one
	glog.V(1).Infof("Verifying all %d EC shards were created for volume %d", t.totalShards, t.volumeID)
	for i, shardFile := range shardFiles {
		if info, err := os.Stat(shardFile); err != nil {
			glog.Errorf("MISSING SHARD: Shard %d file %s not found: %v", i, shardFile, err)
			return nil, fmt.Errorf("shard %d was not created: %v", i, err)
		} else {
			glog.V(1).Infof("SHARD CREATED: Shard %d: %s (%d bytes)", i, shardFile, info.Size())
		}
	}

	// Verify auxiliary files were created
	auxFiles := []string{baseFileName + ".ecx", baseFileName + ".ecj", baseFileName + ".vif"}
	for _, auxFile := range auxFiles {
		if info, err := os.Stat(auxFile); err != nil {
			glog.Errorf("MISSING AUX FILE: %s not found: %v", auxFile, err)
		} else {
			glog.V(1).Infof("AUX FILE CREATED: %s (%d bytes)", auxFile, info.Size())
		}
	}

	t.SetProgress(60.0)
	glog.V(1).Infof("Successfully created %d EC shards for volume %d", t.totalShards, t.volumeID)
	return shardFiles, nil
}

// Validate validates the task parameters
func (t *Task) Validate(params types.TaskParams) error {
	if params.VolumeID == 0 {
		return fmt.Errorf("volume_id is required")
	}
	if params.Server == "" {
		return fmt.Errorf("server is required")
	}
	if t.masterClient == "" {
		return fmt.Errorf("master_client is required")
	}
	if t.workDir == "" {
		return fmt.Errorf("work_dir is required")
	}
	return nil
}

// EstimateTime estimates the time needed for EC processing
func (t *Task) EstimateTime(params types.TaskParams) time.Duration {
	baseTime := 20 * time.Minute // Processing takes time due to comprehensive operations

	if size, ok := params.Parameters["volume_size"].(int64); ok {
		// More accurate estimate based on volume size
		// Account for copying, encoding, and distribution
		gbSize := size / (1024 * 1024 * 1024)
		estimatedTime := time.Duration(gbSize*2) * time.Minute // 2 minutes per GB
		if estimatedTime > baseTime {
			return estimatedTime
		}
	}

	return baseTime
}

// GetProgress returns current progress with detailed step information
func (t *Task) GetProgress() float64 {
	return t.BaseTask.GetProgress()
}

// GetCurrentStep returns the current processing step
func (t *Task) GetCurrentStep() string {
	return t.currentStep
}

// SetEstimatedDuration sets the estimated duration for the task
func (t *Task) SetEstimatedDuration(duration time.Duration) {
	// This can be implemented to store the estimated duration if needed
	// For now, we'll use the dynamic estimation from EstimateTime
}

// Cancel cancels the task
func (t *Task) Cancel() error {
	return t.BaseTask.Cancel()
}

// collectVolumeLocations collects all server locations where a volume has replicas (following command_ec_encode.go pattern)
func (t *Task) collectVolumeLocations(volumeId needle.VolumeId) ([]pb.ServerAddress, error) {
	// Connect to master client to get volume locations
	grpcAddress := pb.ServerToGrpcAddress(t.masterClient)
	conn, err := grpc.NewClient(grpcAddress, t.grpcDialOpt)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to master %s: %v", t.masterClient, err)
	}
	defer conn.Close()

	client := master_pb.NewSeaweedClient(conn)
	volumeListResp, err := client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to get volume list from master: %v", err)
	}

	var locations []pb.ServerAddress
	// Search through all data centers, racks, and volume servers
	for _, dc := range volumeListResp.TopologyInfo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, dataNode := range rack.DataNodeInfos {
				for _, diskInfo := range dataNode.DiskInfos {
					for _, volumeInfo := range diskInfo.VolumeInfos {
						if volumeInfo.Id == uint32(volumeId) {
							locations = append(locations, pb.ServerAddress(dataNode.Id))
							goto nextDataNode // Found volume on this server, move to next server
						}
					}
				}
			nextDataNode:
			}
		}
	}

	if len(locations) == 0 {
		return nil, fmt.Errorf("volume %d not found on any server", volumeId)
	}

	return locations, nil
}

// markVolumeReadonlyOnAllReplicas marks volume as readonly on all replicas (following command_ec_encode.go pattern)
func (t *Task) markVolumeReadonlyOnAllReplicas(volumeId needle.VolumeId, locations []pb.ServerAddress) error {
	// Use parallel processing like command_ec_encode.go
	var wg sync.WaitGroup
	errorChan := make(chan error, len(locations))

	for _, location := range locations {
		wg.Add(1)
		go func(addr pb.ServerAddress) {
			defer wg.Done()

			grpcAddress := pb.ServerToGrpcAddress(string(addr))
			conn, err := grpc.NewClient(grpcAddress, t.grpcDialOpt)
			if err != nil {
				errorChan <- fmt.Errorf("failed to connect to %s: %v", addr, err)
				return
			}
			defer conn.Close()

			client := volume_server_pb.NewVolumeServerClient(conn)
			_, err = client.VolumeMarkReadonly(context.Background(), &volume_server_pb.VolumeMarkReadonlyRequest{
				VolumeId: uint32(volumeId),
			})
			if err != nil {
				errorChan <- fmt.Errorf("failed to mark volume %d readonly on %s: %v", volumeId, addr, err)
			}
		}(location)
	}

	wg.Wait()
	close(errorChan)

	// Check for errors
	for err := range errorChan {
		if err != nil {
			return err
		}
	}

	return nil
}

// distributeEcShardsAcrossServers distributes EC shards across volume servers
func (t *Task) distributeEcShardsAcrossServers(shardFiles []string, taskWorkDir string) error {
	t.currentStep = "distributing"
	t.SetProgress(70.0)
	glog.V(1).Infof("Distributing %d EC shards across servers", len(shardFiles))

	// Get volume servers from topology
	var volumeServers []pb.ServerAddress
	err := operation.WithMasterServerClient(false, pb.ServerAddress(t.masterClient), t.grpcDialOpt, func(masterClient master_pb.SeaweedClient) error {
		topologyResp, err := masterClient.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		if err != nil {
			return err
		}

		// Extract unique volume server addresses
		serverSet := make(map[string]bool)
		for _, dc := range topologyResp.TopologyInfo.DataCenterInfos {
			for _, rack := range dc.RackInfos {
				for _, node := range rack.DataNodeInfos {
					serverAddr := pb.NewServerAddressFromDataNode(node)
					serverKey := string(serverAddr)
					if !serverSet[serverKey] {
						serverSet[serverKey] = true
						volumeServers = append(volumeServers, serverAddr)
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to get volume servers: %v", err)
	}

	if len(volumeServers) == 0 {
		return fmt.Errorf("no volume servers available for EC distribution")
	}

	// Distribute shards in round-robin fashion
	shardTargets := make(map[int]pb.ServerAddress)
	targetServers := make(map[string]bool) // Track unique target servers
	for i, shardFile := range shardFiles {
		targetServer := volumeServers[i%len(volumeServers)]
		shardTargets[i] = targetServer
		targetServers[string(targetServer)] = true
		glog.V(1).Infof("Shard %d (%s) will go to server %s", i, shardFile, targetServer)
	}

	// Upload auxiliary files (.ecx, .ecj, .vif) to ALL servers that will receive shards
	// These files are needed for EC volume mounting on each server
	glog.V(1).Infof("Uploading auxiliary files to %d target servers", len(targetServers))
	for serverAddr := range targetServers {
		targetServer := pb.ServerAddress(serverAddr)
		glog.V(1).Infof("Starting auxiliary file upload to server: %s", targetServer)
		err = t.uploadAuxiliaryFiles(taskWorkDir, targetServer)
		if err != nil {
			return fmt.Errorf("failed to upload auxiliary files to %s: %v", targetServer, err)
		}
		glog.V(1).Infof("Completed auxiliary file upload to server: %s", targetServer)
	}

	// Upload all shards to their target servers
	glog.V(1).Infof("Starting shard upload phase - uploading %d shards to %d servers", len(shardFiles), len(targetServers))
	var wg sync.WaitGroup
	errorChan := make(chan error, len(shardFiles))

	for i, shardFile := range shardFiles {
		wg.Add(1)
		go func(shardId int, shardFile string, targetServer pb.ServerAddress) {
			defer wg.Done()
			glog.V(1).Infof("SHARD UPLOAD START: Uploading shard %d (%s) to server %s", shardId, shardFile, targetServer)
			if err := t.uploadShardToTargetServer(shardFile, targetServer, uint32(shardId)); err != nil {
				glog.Errorf("SHARD UPLOAD FAILED: Shard %d to %s failed: %v", shardId, targetServer, err)
				errorChan <- fmt.Errorf("failed to upload shard %d to %s: %v", shardId, targetServer, err)
			} else {
				glog.V(1).Infof("SHARD UPLOAD SUCCESS: Shard %d successfully uploaded to %s", shardId, targetServer)
			}
		}(i, shardFile, shardTargets[i])
	}

	wg.Wait()
	close(errorChan)

	// Check for upload errors
	for err := range errorChan {
		return err // Return first error encountered
	}
	glog.V(1).Infof("All %d shards uploaded successfully", len(shardFiles))

	// Mount all shards on their respective servers
	glog.V(1).Infof("Starting shard mounting phase - mounting %d shards", len(shardTargets))
	for shardId, targetServer := range shardTargets {
		glog.V(1).Infof("SHARD MOUNT START: Mounting shard %d on server %s", shardId, targetServer)
		err = t.mountShardOnServer(targetServer, uint32(shardId))
		if err != nil {
			glog.Errorf("SHARD MOUNT FAILED: Shard %d on %s failed: %v", shardId, targetServer, err)
			return fmt.Errorf("failed to mount shard %d on %s: %v", shardId, targetServer, err)
		}
		glog.V(1).Infof("SHARD MOUNT SUCCESS: Shard %d successfully mounted on %s", shardId, targetServer)
	}

	t.SetProgress(90.0)
	glog.V(1).Infof("Successfully distributed and mounted all EC shards")

	// Log final distribution summary for debugging
	glog.V(1).Infof("EC DISTRIBUTION SUMMARY for volume %d:", t.volumeID)
	glog.V(1).Infof("  - Total shards created: %d", len(shardFiles))
	glog.V(1).Infof("  - Target servers: %d", len(targetServers))
	glog.V(1).Infof("  - Shard distribution:")
	for shardId, targetServer := range shardTargets {
		glog.V(1).Infof("    Shard %d → %s", shardId, targetServer)
	}
	glog.V(1).Infof("  - Auxiliary files (.ecx, .ecj, .vif) uploaded to all %d servers", len(targetServers))
	glog.V(1).Infof("EC ENCODING COMPLETED for volume %d", t.volumeID)

	return nil
}

// uploadAuxiliaryFiles uploads the .ecx, .ecj, and .vif files needed for EC volume mounting
func (t *Task) uploadAuxiliaryFiles(workDir string, targetServer pb.ServerAddress) error {
	baseFileName := filepath.Join(workDir, fmt.Sprintf("%d", t.volumeID))

	// List of auxiliary files to upload
	auxFiles := []struct {
		ext  string
		desc string
	}{
		{".ecx", "index file"},
		{".ecj", "journal file"},
		{".vif", "volume info file"},
	}

	glog.V(1).Infof("Uploading auxiliary files for volume %d to server %s", t.volumeID, targetServer)

	for _, auxFile := range auxFiles {
		filePath := baseFileName + auxFile.ext

		// Check if file exists (some may be optional)
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			glog.V(1).Infof("Auxiliary file %s does not exist, skipping", filePath)
			continue
		}

		// Upload the auxiliary file
		err := t.uploadAuxiliaryFile(filePath, targetServer, auxFile.ext)
		if err != nil {
			return fmt.Errorf("failed to upload %s: %v", auxFile.desc, err)
		}
		glog.V(1).Infof("Successfully uploaded %s (%s) for volume %d to %s", auxFile.desc, auxFile.ext, t.volumeID, targetServer)
	}

	glog.V(1).Infof("Completed uploading auxiliary files for volume %d to %s", t.volumeID, targetServer)
	return nil
}

// uploadAuxiliaryFile uploads a single auxiliary file (.ecx, .ecj, .vif) to the target server
func (t *Task) uploadAuxiliaryFile(filePath string, targetServer pb.ServerAddress, ext string) error {
	glog.V(1).Infof("AUX UPLOAD START: Uploading auxiliary file %s to %s", ext, targetServer)

	// Open the auxiliary file
	file, err := os.Open(filePath)
	if err != nil {
		glog.Errorf("AUX UPLOAD ERROR: Failed to open auxiliary file %s: %v", filePath, err)
		return fmt.Errorf("failed to open auxiliary file %s: %v", filePath, err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		glog.Errorf("AUX UPLOAD ERROR: Failed to stat auxiliary file %s: %v", filePath, err)
		return fmt.Errorf("failed to stat auxiliary file: %v", err)
	}

	glog.V(1).Infof("AUX UPLOAD DETAILS: File %s size: %d bytes", filePath, fileInfo.Size())

	// Connect to target volume server
	grpcAddress := pb.ServerToGrpcAddress(string(targetServer))
	conn, err := grpc.NewClient(grpcAddress, t.grpcDialOpt)
	if err != nil {
		glog.Errorf("AUX UPLOAD ERROR: Failed to connect to %s: %v", targetServer, err)
		return fmt.Errorf("failed to connect to %s: %v", targetServer, err)
	}
	defer conn.Close()
	glog.V(2).Infof("AUX UPLOAD GRPC: Connected to %s for %s", targetServer, ext)

	client := volume_server_pb.NewVolumeServerClient(conn)
	ctx := context.Background()

	// Create streaming client for auxiliary file upload
	stream, err := client.ReceiveFile(ctx)
	if err != nil {
		glog.Errorf("AUX UPLOAD ERROR: Failed to create receive stream for %s: %v", ext, err)
		return fmt.Errorf("failed to create receive stream: %v", err)
	}
	glog.V(2).Infof("AUX UPLOAD GRPC: Created stream for %s", ext)

	// Send file info first
	err = stream.Send(&volume_server_pb.ReceiveFileRequest{
		Data: &volume_server_pb.ReceiveFileRequest_Info{
			Info: &volume_server_pb.ReceiveFileInfo{
				VolumeId:   t.volumeID,
				Ext:        ext,
				Collection: t.collection,
				IsEcVolume: true,
				ShardId:    0, // Not applicable for auxiliary files
				FileSize:   uint64(fileInfo.Size()),
			},
		},
	})
	if err != nil {
		glog.Errorf("AUX UPLOAD ERROR: Failed to send auxiliary file info for %s: %v", ext, err)
		return fmt.Errorf("failed to send auxiliary file info: %v", err)
	}
	glog.V(2).Infof("AUX UPLOAD GRPC: Sent file info for %s", ext)

	// Stream file content in chunks
	buffer := make([]byte, 64*1024) // 64KB chunks
	totalSent := int64(0)
	chunkCount := 0
	for {
		n, err := file.Read(buffer)
		if err != nil && err != io.EOF {
			glog.Errorf("AUX UPLOAD ERROR: Failed to read auxiliary file %s: %v", filePath, err)
			return fmt.Errorf("failed to read auxiliary file: %v", err)
		}

		// Send data if we read any
		if n > 0 {
			err = stream.Send(&volume_server_pb.ReceiveFileRequest{
				Data: &volume_server_pb.ReceiveFileRequest_FileContent{
					FileContent: buffer[:n],
				},
			})
			if err != nil {
				glog.Errorf("AUX UPLOAD ERROR: Failed to send chunk %d for %s: %v", chunkCount, ext, err)
				return fmt.Errorf("failed to send auxiliary file chunk: %v", err)
			}
			totalSent += int64(n)
			chunkCount++
			glog.V(3).Infof("AUX UPLOAD CHUNK: %s chunk %d sent (%d bytes, total: %d)", ext, chunkCount, n, totalSent)
		}

		// Break if we reached EOF
		if err == io.EOF {
			break
		}
	}
	glog.V(2).Infof("AUX UPLOAD GRPC: Completed streaming %s (%d bytes in %d chunks)", ext, totalSent, chunkCount)

	// Close stream and get response
	resp, err := stream.CloseAndRecv()
	if err != nil {
		glog.Errorf("AUX UPLOAD ERROR: Failed to close stream for %s: %v", ext, err)
		return fmt.Errorf("failed to close auxiliary file stream: %v", err)
	}

	if resp.Error != "" {
		glog.Errorf("AUX UPLOAD ERROR: Server error uploading %s: %s", ext, resp.Error)
		return fmt.Errorf("server error uploading auxiliary file: %s", resp.Error)
	}

	glog.V(1).Infof("AUX UPLOAD SUCCESS: %s (%d bytes) successfully uploaded to %s", ext, resp.BytesWritten, targetServer)
	return nil
}

// deleteVolumeFromAllLocations deletes volume from all replica locations (following command_ec_encode.go pattern)
func (t *Task) deleteVolumeFromAllLocations(volumeId needle.VolumeId, locations []pb.ServerAddress) error {
	if len(locations) == 0 {
		glog.Warningf("No locations found for volume %d, skipping deletion", volumeId)
		return nil
	}

	glog.V(1).Infof("Deleting volume %d from %d locations: %v", volumeId, len(locations), locations)

	// Use parallel processing like command_ec_encode.go
	var wg sync.WaitGroup
	errorChan := make(chan error, len(locations))

	for _, location := range locations {
		wg.Add(1)
		go func(addr pb.ServerAddress) {
			defer wg.Done()

			grpcAddress := pb.ServerToGrpcAddress(string(addr))
			conn, err := grpc.NewClient(grpcAddress, t.grpcDialOpt)
			if err != nil {
				errorChan <- fmt.Errorf("failed to connect to %s: %v", addr, err)
				return
			}
			defer conn.Close()

			client := volume_server_pb.NewVolumeServerClient(conn)
			_, err = client.VolumeDelete(context.Background(), &volume_server_pb.VolumeDeleteRequest{
				VolumeId: uint32(volumeId),
			})
			if err != nil {
				errorChan <- fmt.Errorf("failed to delete volume %d from %s: %v", volumeId, addr, err)
				return
			}

			glog.V(1).Infof("Successfully deleted volume %d from %s", volumeId, addr)
		}(location)
	}

	wg.Wait()
	close(errorChan)

	// Check for errors (but don't fail the whole operation for deletion errors)
	errorCount := 0
	for err := range errorChan {
		if err != nil {
			glog.Errorf("Volume deletion error: %v", err)
			errorCount++
		}
	}

	if errorCount > 0 {
		return fmt.Errorf("failed to delete volume from %d locations", errorCount)
	}

	glog.Infof("Successfully deleted volume %d from all %d replica locations", volumeId, len(locations))
	return nil
}

// uploadShardToTargetServer streams shard data to target server using gRPC ReceiveFile
func (t *Task) uploadShardToTargetServer(shardFile string, targetServer pb.ServerAddress, shardId uint32) error {
	glog.V(1).Infof("UPLOAD START: Streaming shard %d to server %s via gRPC", shardId, targetServer)

	// Read the shard file
	file, err := os.Open(shardFile)
	if err != nil {
		glog.Errorf("UPLOAD ERROR: Failed to open shard file %s: %v", shardFile, err)
		return fmt.Errorf("failed to open shard file %s: %v", shardFile, err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		glog.Errorf("UPLOAD ERROR: Failed to stat shard file %s: %v", shardFile, err)
		return fmt.Errorf("failed to stat shard file: %v", err)
	}

	if fileInfo.Size() == 0 {
		glog.Errorf("UPLOAD ERROR: Shard file %s is empty", shardFile)
		return fmt.Errorf("shard file %s is empty", shardFile)
	}

	glog.V(1).Infof("UPLOAD DETAILS: Shard %d file %s size: %d bytes", shardId, shardFile, fileInfo.Size())

	// Connect to target volume server
	grpcAddress := pb.ServerToGrpcAddress(string(targetServer))
	conn, err := grpc.NewClient(grpcAddress, t.grpcDialOpt)
	if err != nil {
		glog.Errorf("UPLOAD ERROR: Failed to connect to %s: %v", targetServer, err)
		return fmt.Errorf("failed to connect to %s: %v", targetServer, err)
	}
	defer conn.Close()
	glog.V(2).Infof("UPLOAD GRPC: Connected to %s for shard %d", targetServer, shardId)

	client := volume_server_pb.NewVolumeServerClient(conn)
	ctx := context.Background()

	// Create streaming client
	stream, err := client.ReceiveFile(ctx)
	if err != nil {
		glog.Errorf("UPLOAD ERROR: Failed to create receive stream for shard %d: %v", shardId, err)
		return fmt.Errorf("failed to create receive stream: %v", err)
	}
	glog.V(2).Infof("UPLOAD GRPC: Created stream for shard %d to %s", shardId, targetServer)

	// Send file info first
	err = stream.Send(&volume_server_pb.ReceiveFileRequest{
		Data: &volume_server_pb.ReceiveFileRequest_Info{
			Info: &volume_server_pb.ReceiveFileInfo{
				VolumeId:   t.volumeID,
				Ext:        fmt.Sprintf(".ec%02d", shardId),
				Collection: t.collection,
				IsEcVolume: true,
				ShardId:    shardId,
				FileSize:   uint64(fileInfo.Size()),
			},
		},
	})
	if err != nil {
		glog.Errorf("UPLOAD ERROR: Failed to send file info for shard %d: %v", shardId, err)
		return fmt.Errorf("failed to send file info: %v", err)
	}
	glog.V(2).Infof("UPLOAD GRPC: Sent file info for shard %d", shardId)

	// Stream file content in chunks
	buffer := make([]byte, 64*1024) // 64KB chunks
	totalSent := int64(0)
	chunkCount := 0
	for {
		n, err := file.Read(buffer)
		if err != nil && err != io.EOF {
			glog.Errorf("UPLOAD ERROR: Failed to read shard file %s: %v", shardFile, err)
			return fmt.Errorf("failed to read file: %v", err)
		}

		// Send data if we read any
		if n > 0 {
			err = stream.Send(&volume_server_pb.ReceiveFileRequest{
				Data: &volume_server_pb.ReceiveFileRequest_FileContent{
					FileContent: buffer[:n],
				},
			})
			if err != nil {
				glog.Errorf("UPLOAD ERROR: Failed to send chunk %d for shard %d: %v", chunkCount, shardId, err)
				return fmt.Errorf("failed to send file chunk: %v", err)
			}
			totalSent += int64(n)
			chunkCount++
			glog.V(3).Infof("UPLOAD CHUNK: Shard %d chunk %d sent (%d bytes, total: %d)", shardId, chunkCount, n, totalSent)
		}

		// Break if we reached EOF
		if err == io.EOF {
			break
		}
	}
	glog.V(2).Infof("UPLOAD GRPC: Completed streaming shard %d (%d bytes in %d chunks)", shardId, totalSent, chunkCount)

	// Close stream and get response
	resp, err := stream.CloseAndRecv()
	if err != nil {
		glog.Errorf("UPLOAD ERROR: Failed to close stream for shard %d: %v", shardId, err)
		return fmt.Errorf("failed to close stream: %v", err)
	}

	if resp.Error != "" {
		glog.Errorf("UPLOAD ERROR: Server error for shard %d: %s", shardId, resp.Error)
		return fmt.Errorf("server error: %s", resp.Error)
	}

	glog.V(1).Infof("UPLOAD SUCCESS: Shard %d (%d bytes) successfully uploaded to %s", shardId, resp.BytesWritten, targetServer)
	return nil
}

// mountShardOnServer mounts an EC shard on target server
func (t *Task) mountShardOnServer(targetServer pb.ServerAddress, shardId uint32) error {
	glog.V(1).Infof("MOUNT START: Mounting shard %d on server %s", shardId, targetServer)

	// Connect to target server
	grpcAddress := pb.ServerToGrpcAddress(string(targetServer))
	conn, err := grpc.NewClient(grpcAddress, t.grpcDialOpt)
	if err != nil {
		glog.Errorf("MOUNT ERROR: Failed to connect to %s for shard %d: %v", targetServer, shardId, err)
		return fmt.Errorf("failed to connect to %s: %v", targetServer, err)
	}
	defer conn.Close()
	glog.V(2).Infof("MOUNT GRPC: Connected to %s for shard %d", targetServer, shardId)

	client := volume_server_pb.NewVolumeServerClient(conn)
	ctx := context.Background()

	// Mount the shard
	_, err = client.VolumeEcShardsMount(ctx, &volume_server_pb.VolumeEcShardsMountRequest{
		VolumeId:   t.volumeID,
		Collection: t.collection,
		ShardIds:   []uint32{shardId},
	})
	if err != nil {
		glog.Errorf("MOUNT ERROR: Failed to mount shard %d on %s: %v", shardId, targetServer, err)
		return fmt.Errorf("failed to mount shard %d: %v", shardId, err)
	}

	glog.V(1).Infof("MOUNT SUCCESS: Shard %d successfully mounted on %s", shardId, targetServer)
	return nil
}

// ErasureCodingConfigV2 extends BaseConfig with erasure coding specific settings
type ErasureCodingConfigV2 struct {
	base.BaseConfig
	QuietForSeconds  int     `json:"quiet_for_seconds"`
	FullnessRatio    float64 `json:"fullness_ratio"`
	CollectionFilter string  `json:"collection_filter"`
}

// ecDetection implements the detection logic for erasure coding tasks
func ecDetection(metrics []*types.VolumeHealthMetrics, clusterInfo *types.ClusterInfo, config base.TaskConfig) ([]*types.TaskDetectionResult, error) {
	if !config.IsEnabled() {
		return nil, nil
	}

	ecConfig := config.(*ErasureCodingConfigV2)
	var results []*types.TaskDetectionResult
	now := time.Now()
	quietThreshold := time.Duration(ecConfig.QuietForSeconds) * time.Second
	minSizeBytes := uint64(100) * 1024 * 1024 // 100MB minimum

	for _, metric := range metrics {
		// Skip if already EC volume
		if metric.IsECVolume {
			continue
		}

		// Check minimum size requirement
		if metric.Size < minSizeBytes {
			continue
		}

		// Check collection filter if specified
		if ecConfig.CollectionFilter != "" {
			// Parse comma-separated collections
			allowedCollections := make(map[string]bool)
			for _, collection := range strings.Split(ecConfig.CollectionFilter, ",") {
				allowedCollections[strings.TrimSpace(collection)] = true
			}
			// Skip if volume's collection is not in the allowed list
			if !allowedCollections[metric.Collection] {
				continue
			}
		}

		// Check quiet duration and fullness criteria
		if metric.Age >= quietThreshold && metric.FullnessRatio >= ecConfig.FullnessRatio {
			result := &types.TaskDetectionResult{
				TaskType:   types.TaskTypeErasureCoding,
				VolumeID:   metric.VolumeID,
				Server:     metric.Server,
				Collection: metric.Collection,
				Priority:   types.TaskPriorityLow, // EC is not urgent
				Reason: fmt.Sprintf("Volume meets EC criteria: quiet for %.1fs (>%ds), fullness=%.1f%% (>%.1f%%), size=%.1fMB (>100MB)",
					metric.Age.Seconds(), ecConfig.QuietForSeconds, metric.FullnessRatio*100, ecConfig.FullnessRatio*100,
					float64(metric.Size)/(1024*1024)),
				Parameters: map[string]interface{}{
					"age_seconds":    int(metric.Age.Seconds()),
					"fullness_ratio": metric.FullnessRatio,
					"size_mb":        int(metric.Size / (1024 * 1024)),
				},
				ScheduleAt: now,
			}
			results = append(results, result)
		}
	}

	return results, nil
}

// ecScheduling implements the scheduling logic for erasure coding tasks
func ecScheduling(task *types.Task, runningTasks []*types.Task, availableWorkers []*types.Worker, config base.TaskConfig) bool {
	ecConfig := config.(*ErasureCodingConfigV2)

	// Check if we have available workers
	if len(availableWorkers) == 0 {
		return false
	}

	// Count running EC tasks
	runningCount := 0
	for _, runningTask := range runningTasks {
		if runningTask.Type == types.TaskTypeErasureCoding {
			runningCount++
		}
	}

	// Check concurrency limit
	if runningCount >= ecConfig.MaxConcurrent {
		return false
	}

	// Check if any worker can handle EC tasks
	for _, worker := range availableWorkers {
		for _, capability := range worker.Capabilities {
			if capability == types.TaskTypeErasureCoding {
				return true
			}
		}
	}

	return false
}

// createErasureCodingTask creates an erasure coding task instance
func createErasureCodingTask(params types.TaskParams) (types.TaskInterface, error) {
	// Validate parameters
	if params.VolumeID == 0 {
		return nil, fmt.Errorf("volume_id is required")
	}
	if params.Server == "" {
		return nil, fmt.Errorf("server is required")
	}

	// Extract additional parameters for comprehensive EC
	masterClient := "localhost:9333"    // Default master client
	workDir := "/tmp/seaweedfs_ec_work" // Default work directory

	if mc, ok := params.Parameters["master_client"].(string); ok && mc != "" {
		masterClient = mc
	}
	if wd, ok := params.Parameters["work_dir"].(string); ok && wd != "" {
		workDir = wd
	}

	// Create EC task with comprehensive capabilities
	task := NewTaskWithParams(params.Server, params.VolumeID, masterClient, workDir)

	// Set gRPC dial option if provided
	if params.GrpcDialOption != nil {
		task.SetDialOption(params.GrpcDialOption)
	}

	task.SetEstimatedDuration(task.EstimateTime(params))
	return task, nil
}

// getErasureCodingConfigSpec returns the configuration schema for erasure coding tasks
func getErasureCodingConfigSpec() base.ConfigSpec {
	return base.ConfigSpec{
		Fields: []*config.Field{
			{
				Name:         "enabled",
				JSONName:     "enabled",
				Type:         config.FieldTypeBool,
				DefaultValue: true,
				Required:     false,
				DisplayName:  "Enable Erasure Coding Tasks",
				Description:  "Whether erasure coding tasks should be automatically created",
				InputType:    "checkbox",
				CSSClasses:   "form-check-input",
			},
			{
				Name:         "quiet_for_seconds",
				JSONName:     "quiet_for_seconds",
				Type:         config.FieldTypeInterval,
				DefaultValue: 7 * 24 * 60 * 60,  // 7 days
				MinValue:     1 * 24 * 60 * 60,  // 1 day
				MaxValue:     30 * 24 * 60 * 60, // 30 days
				Required:     true,
				DisplayName:  "Quiet For Duration",
				Description:  "Only apply erasure coding to volumes that have not been modified for this duration",
				Unit:         config.UnitDays,
				InputType:    "interval",
				CSSClasses:   "form-control",
			},
			{
				Name:         "scan_interval_seconds",
				JSONName:     "scan_interval_seconds",
				Type:         config.FieldTypeInterval,
				DefaultValue: 12 * 60 * 60, // 12 hours
				MinValue:     2 * 60 * 60,  // 2 hours
				MaxValue:     24 * 60 * 60, // 24 hours
				Required:     true,
				DisplayName:  "Scan Interval",
				Description:  "How often to scan for volumes needing erasure coding",
				Unit:         config.UnitHours,
				InputType:    "interval",
				CSSClasses:   "form-control",
			},
			{
				Name:         "max_concurrent",
				JSONName:     "max_concurrent",
				Type:         config.FieldTypeInt,
				DefaultValue: 1,
				MinValue:     1,
				MaxValue:     3,
				Required:     true,
				DisplayName:  "Max Concurrent Tasks",
				Description:  "Maximum number of erasure coding tasks that can run simultaneously",
				Unit:         config.UnitCount,
				InputType:    "number",
				CSSClasses:   "form-control",
			},
			{
				Name:         "fullness_ratio",
				JSONName:     "fullness_ratio",
				Type:         config.FieldTypeFloat,
				DefaultValue: 0.9, // 90%
				MinValue:     0.5,
				MaxValue:     1.0,
				Required:     true,
				DisplayName:  "Fullness Ratio",
				Description:  "Only apply erasure coding to volumes with fullness ratio above this threshold",
				Placeholder:  "0.90 (90%)",
				Unit:         config.UnitNone,
				InputType:    "number",
				CSSClasses:   "form-control",
			},
			{
				Name:         "collection_filter",
				JSONName:     "collection_filter",
				Type:         config.FieldTypeString,
				DefaultValue: "",
				Required:     false,
				DisplayName:  "Collection Filter",
				Description:  "Only apply erasure coding to volumes in these collections (comma-separated, leave empty for all)",
				Placeholder:  "collection1,collection2",
				InputType:    "text",
				CSSClasses:   "form-control",
			},
		},
	}
}

// initErasureCodingV2 registers the refactored erasure coding task
func initErasureCodingV2() {
	// Create configuration instance
	config := &ErasureCodingConfigV2{
		BaseConfig: base.BaseConfig{
			Enabled:             false,        // Conservative default - enable via configuration
			ScanIntervalSeconds: 12 * 60 * 60, // 12 hours
			MaxConcurrent:       1,            // Conservative default
		},
		QuietForSeconds:  7 * 24 * 60 * 60, // 7 days quiet period
		FullnessRatio:    0.90,             // 90% full threshold
		CollectionFilter: "",               // No collection filter by default
	}

	// Create complete task definition
	taskDef := &base.TaskDefinition{
		Type:         types.TaskTypeErasureCoding,
		Name:         "erasure_coding",
		DisplayName:  "Erasure Coding",
		Description:  "Converts volumes to erasure coded format for improved data durability",
		Icon:         "fas fa-shield-alt text-info",
		Capabilities: []string{"erasure_coding", "storage", "durability"},

		Config:         config,
		ConfigSpec:     getErasureCodingConfigSpec(),
		CreateTask:     createErasureCodingTask,
		DetectionFunc:  ecDetection,
		ScanInterval:   12 * time.Hour,
		SchedulingFunc: ecScheduling,
		MaxConcurrent:  1,
		RepeatInterval: 24 * time.Hour,
	}

	// Register everything with a single function call!
	base.RegisterTask(taskDef)
}
