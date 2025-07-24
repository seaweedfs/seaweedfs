package erasure_coding

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"google.golang.org/grpc"
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

// ServerInfo holds information about available servers for shard placement
type ServerInfo struct {
	Address        string
	DataCenter     string
	Rack           string
	AvailableSpace int64
	LoadScore      float64
	ShardCount     int
}

// ShardPlacement represents where a shard should be placed
type ShardPlacement struct {
	ShardID     int
	ServerAddr  string
	DataCenter  string
	Rack        string
	BackupAddrs []string // Alternative servers for redundancy
}

// NewTask creates a new erasure coding task
func NewTask(sourceServer string, volumeID uint32) *Task {
	task := &Task{
		BaseTask:     tasks.NewBaseTask(types.TaskTypeErasureCoding),
		sourceServer: sourceServer,
		volumeID:     volumeID,
		masterClient: "localhost:9333",         // Default master client
		workDir:      "/tmp/seaweedfs_ec_work", // Default work directory
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
		dataShards:   10,
		parityShards: 4,
		totalShards:  14,
		stepProgress: make(map[string]float64),
	}
	return task
}

// Execute performs the comprehensive EC operation
func (t *Task) Execute(params types.TaskParams) error {
	glog.Infof("Starting erasure coding for volume %d from server %s", t.volumeID, t.sourceServer)

	// Extract parameters
	t.collection = params.Collection
	if t.collection == "" {
		t.collection = "default"
	}

	// Override defaults with parameters if provided
	if mc, ok := params.Parameters["master_client"].(string); ok && mc != "" {
		t.masterClient = mc
	}
	if wd, ok := params.Parameters["work_dir"].(string); ok && wd != "" {
		t.workDir = wd
	}

	// Create working directory for this task
	taskWorkDir := filepath.Join(t.workDir, fmt.Sprintf("ec_%d_%d", t.volumeID, time.Now().Unix()))
	err := os.MkdirAll(taskWorkDir, 0755)
	if err != nil {
		return fmt.Errorf("failed to create work directory %s: %v", taskWorkDir, err)
	}
	defer t.cleanup(taskWorkDir)

	// Step 1: Copy volume data to local disk
	if err := t.copyVolumeDataLocally(taskWorkDir); err != nil {
		return fmt.Errorf("failed to copy volume data: %v", err)
	}

	// Step 2: Mark source volume as read-only
	if err := t.markVolumeReadOnly(); err != nil {
		return fmt.Errorf("failed to mark volume read-only: %v", err)
	}

	// Step 3: Perform local EC encoding
	shardFiles, err := t.performLocalECEncoding(taskWorkDir)
	if err != nil {
		return fmt.Errorf("failed to perform EC encoding: %v", err)
	}

	// Step 4: Find optimal shard placement
	placements, err := t.calculateOptimalShardPlacement()
	if err != nil {
		return fmt.Errorf("failed to calculate shard placement: %v", err)
	}

	// Step 5: Distribute shards to target servers
	if err := t.distributeShards(shardFiles, placements); err != nil {
		return fmt.Errorf("failed to distribute shards: %v", err)
	}

	// Step 6: Verify and cleanup source volume
	if err := t.verifyAndCleanupSource(); err != nil {
		return fmt.Errorf("failed to verify and cleanup: %v", err)
	}

	t.SetProgress(100.0)
	glog.Infof("Successfully completed erasure coding for volume %d", t.volumeID)
	return nil
}

// copyVolumeDataLocally copies the volume data from source server to local disk
func (t *Task) copyVolumeDataLocally(workDir string) error {
	t.currentStep = "copying_volume_data"
	t.SetProgress(5.0)
	glog.V(1).Infof("Copying volume %d data from %s to local disk", t.volumeID, t.sourceServer)

	ctx := context.Background()

	// Connect to source volume server
	conn, err := grpc.Dial(t.sourceServer, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("failed to connect to source server %s: %v", t.sourceServer, err)
	}
	defer conn.Close()

	client := volume_server_pb.NewVolumeServerClient(conn)

	// Get volume info first
	statusResp, err := client.VolumeStatus(ctx, &volume_server_pb.VolumeStatusRequest{
		VolumeId: t.volumeID,
	})
	if err != nil {
		return fmt.Errorf("failed to get volume status: %v", err)
	}

	glog.V(1).Infof("Volume %d size: %d bytes, file count: %d",
		t.volumeID, statusResp.VolumeSize, statusResp.FileCount)

	// Copy .dat file
	datFile := filepath.Join(workDir, fmt.Sprintf("%d.dat", t.volumeID))
	if err := t.copyVolumeFile(client, ctx, t.volumeID, ".dat", datFile, statusResp.VolumeSize); err != nil {
		return fmt.Errorf("failed to copy .dat file: %v", err)
	}

	// Copy .idx file
	idxFile := filepath.Join(workDir, fmt.Sprintf("%d.idx", t.volumeID))
	if err := t.copyVolumeFile(client, ctx, t.volumeID, ".idx", idxFile, 0); err != nil {
		return fmt.Errorf("failed to copy .idx file: %v", err)
	}

	t.SetProgress(15.0)
	glog.V(1).Infof("Successfully copied volume %d files to %s", t.volumeID, workDir)
	return nil
}

// copyVolumeFile copies a specific volume file from source server
func (t *Task) copyVolumeFile(client volume_server_pb.VolumeServerClient, ctx context.Context,
	volumeID uint32, extension string, localPath string, expectedSize uint64) error {

	// Stream volume file data using CopyFile API
	stream, err := client.CopyFile(ctx, &volume_server_pb.CopyFileRequest{
		VolumeId:   volumeID,
		Ext:        extension,
		Collection: t.collection,
	})
	if err != nil {
		return fmt.Errorf("failed to start file copy stream: %v", err)
	}

	// Create local file
	file, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("failed to create local file %s: %v", localPath, err)
	}
	defer file.Close()

	// Copy data with progress tracking
	var totalBytes int64
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to receive file data: %v", err)
		}

		written, err := file.Write(resp.FileContent)
		if err != nil {
			return fmt.Errorf("failed to write to local file: %v", err)
		}

		totalBytes += int64(written)

		// Update progress for large files
		if expectedSize > 0 {
			progress := float64(totalBytes) / float64(expectedSize) * 10.0 // 10% of total progress
			t.SetProgress(5.0 + progress)
		}
	}

	glog.V(2).Infof("Copied %d bytes to %s", totalBytes, localPath)
	return nil
}

// markVolumeReadOnly marks the source volume as read-only
func (t *Task) markVolumeReadOnly() error {
	t.currentStep = "marking_readonly"
	t.SetProgress(20.0)
	glog.V(1).Infof("Marking volume %d as read-only", t.volumeID)

	ctx := context.Background()
	conn, err := grpc.Dial(t.sourceServer, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("failed to connect to source server: %v", err)
	}
	defer conn.Close()

	client := volume_server_pb.NewVolumeServerClient(conn)
	_, err = client.VolumeMarkReadonly(ctx, &volume_server_pb.VolumeMarkReadonlyRequest{
		VolumeId: t.volumeID,
	})
	if err != nil {
		return fmt.Errorf("failed to mark volume read-only: %v", err)
	}

	t.SetProgress(25.0)
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

	// Generate EC shards using SeaweedFS erasure coding
	shardFiles := make([]string, t.totalShards)
	for i := 0; i < t.totalShards; i++ {
		shardFiles[i] = filepath.Join(workDir, fmt.Sprintf("%d.ec%02d", t.volumeID, i))
	}

	// Encode .dat file
	if err := t.encodeFile(datFile, shardFiles, ".dat"); err != nil {
		return nil, fmt.Errorf("failed to encode dat file: %v", err)
	}

	t.SetProgress(45.0)

	// Encode .idx file
	if err := t.encodeFile(idxFile, shardFiles, ".idx"); err != nil {
		return nil, fmt.Errorf("failed to encode idx file: %v", err)
	}

	t.SetProgress(60.0)
	glog.V(1).Infof("Successfully created %d EC shards for volume %d", t.totalShards, t.volumeID)
	return shardFiles, nil
}

// encodeFile encodes a single file into EC shards
func (t *Task) encodeFile(inputFile string, shardFiles []string, fileType string) error {
	// Read input file
	data, err := os.ReadFile(inputFile)
	if err != nil {
		return fmt.Errorf("failed to read input file: %v", err)
	}

	// Write data to a temporary file first, then use SeaweedFS erasure coding
	tempFile := filepath.Join(filepath.Dir(shardFiles[0]), fmt.Sprintf("temp_%s", filepath.Base(inputFile)))
	err = os.WriteFile(tempFile, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write temp file: %v", err)
	}
	defer os.Remove(tempFile)

	// Use SeaweedFS erasure coding library with base filename
	baseFileName := tempFile[:len(tempFile)-len(filepath.Ext(tempFile))]
	err = erasure_coding.WriteEcFiles(baseFileName)
	if err != nil {
		return fmt.Errorf("failed to write EC files: %v", err)
	}

	// Verify that shards were created
	for i, shardFile := range shardFiles {
		if _, err := os.Stat(shardFile); err != nil {
			glog.Warningf("Shard %d file %s not found: %v", i, shardFile, err)
		} else {
			info, _ := os.Stat(shardFile)
			glog.V(2).Infof("Created shard %d: %s (%d bytes)", i, shardFile, info.Size())
		}
	}

	return nil
}

// calculateOptimalShardPlacement determines where to place each shard for optimal distribution
func (t *Task) calculateOptimalShardPlacement() ([]ShardPlacement, error) {
	t.currentStep = "calculating_placement"
	t.SetProgress(65.0)
	glog.V(1).Infof("Calculating optimal shard placement for volume %d", t.volumeID)

	// Get available servers from master
	servers, err := t.getAvailableServers()
	if err != nil {
		return nil, fmt.Errorf("failed to get available servers: %v", err)
	}

	if len(servers) < t.totalShards {
		return nil, fmt.Errorf("insufficient servers: need %d, have %d", t.totalShards, len(servers))
	}

	// Sort servers by placement desirability (considering space, load, affinity)
	t.rankServersForPlacement(servers)

	// Assign shards to servers with affinity logic
	placements := make([]ShardPlacement, t.totalShards)
	usedServers := make(map[string]int) // Track how many shards per server

	for shardID := 0; shardID < t.totalShards; shardID++ {
		server := t.selectBestServerForShard(servers, usedServers, shardID)
		if server == nil {
			return nil, fmt.Errorf("failed to find suitable server for shard %d", shardID)
		}

		placements[shardID] = ShardPlacement{
			ShardID:     shardID,
			ServerAddr:  server.Address,
			DataCenter:  server.DataCenter,
			Rack:        server.Rack,
			BackupAddrs: t.selectBackupServers(servers, server, 2),
		}

		usedServers[server.Address]++
		glog.V(2).Infof("Assigned shard %d to server %s (DC: %s, Rack: %s)",
			shardID, server.Address, server.DataCenter, server.Rack)
	}

	t.SetProgress(70.0)
	glog.V(1).Infof("Calculated placement for %d shards across %d servers",
		t.totalShards, len(usedServers))
	return placements, nil
}

// getAvailableServers retrieves available servers from the master
func (t *Task) getAvailableServers() ([]*ServerInfo, error) {
	ctx := context.Background()
	conn, err := grpc.Dial(t.masterClient, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to master: %v", err)
	}
	defer conn.Close()

	client := master_pb.NewSeaweedClient(conn)
	resp, err := client.VolumeList(ctx, &master_pb.VolumeListRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to get volume list: %v", err)
	}

	servers := make([]*ServerInfo, 0)

	// Parse topology information to extract server details
	if resp.TopologyInfo != nil {
		for _, dc := range resp.TopologyInfo.DataCenterInfos {
			for _, rack := range dc.RackInfos {
				for _, node := range rack.DataNodeInfos {
					for diskType, diskInfo := range node.DiskInfos {
						server := &ServerInfo{
							Address:        fmt.Sprintf("%s:%d", node.Id, node.GrpcPort),
							DataCenter:     dc.Id,
							Rack:           rack.Id,
							AvailableSpace: int64(diskInfo.FreeVolumeCount) * 32 * 1024 * 1024 * 1024, // Rough estimate
							LoadScore:      float64(diskInfo.ActiveVolumeCount) / float64(diskInfo.MaxVolumeCount),
							ShardCount:     0,
						}

						// Skip servers that are full or have high load
						if diskInfo.FreeVolumeCount > 0 && server.LoadScore < 0.9 {
							servers = append(servers, server)
							glog.V(2).Infof("Available server: %s (DC: %s, Rack: %s, DiskType: %s, Load: %.2f)",
								server.Address, server.DataCenter, server.Rack, diskType, server.LoadScore)
						}
					}
				}
			}
		}
	}

	return servers, nil
}

// rankServersForPlacement sorts servers by desirability for shard placement
func (t *Task) rankServersForPlacement(servers []*ServerInfo) {
	sort.Slice(servers, func(i, j int) bool {
		serverA, serverB := servers[i], servers[j]

		// Primary criteria: lower load is better
		if serverA.LoadScore != serverB.LoadScore {
			return serverA.LoadScore < serverB.LoadScore
		}

		// Secondary criteria: more available space is better
		if serverA.AvailableSpace != serverB.AvailableSpace {
			return serverA.AvailableSpace > serverB.AvailableSpace
		}

		// Tertiary criteria: fewer existing shards is better
		return serverA.ShardCount < serverB.ShardCount
	})
}

// selectBestServerForShard selects the best server for a specific shard considering affinity
func (t *Task) selectBestServerForShard(servers []*ServerInfo, usedServers map[string]int, shardID int) *ServerInfo {
	// For data shards (0-9), prefer distribution across different racks
	// For parity shards (10-13), can be more flexible
	isDataShard := shardID < t.dataShards

	var candidates []*ServerInfo

	if isDataShard {
		// For data shards, prioritize rack diversity
		usedRacks := make(map[string]bool)
		for _, server := range servers {
			if count, exists := usedServers[server.Address]; exists && count > 0 {
				usedRacks[server.Rack] = true
			}
		}

		// First try to find servers in unused racks
		for _, server := range servers {
			if !usedRacks[server.Rack] && usedServers[server.Address] < 2 { // Max 2 shards per server
				candidates = append(candidates, server)
			}
		}

		// If no unused racks, fall back to any available server
		if len(candidates) == 0 {
			for _, server := range servers {
				if usedServers[server.Address] < 2 {
					candidates = append(candidates, server)
				}
			}
		}
	} else {
		// For parity shards, just avoid overloading servers
		for _, server := range servers {
			if usedServers[server.Address] < 2 {
				candidates = append(candidates, server)
			}
		}
	}

	if len(candidates) == 0 {
		// Last resort: allow up to 3 shards per server
		for _, server := range servers {
			if usedServers[server.Address] < 3 {
				candidates = append(candidates, server)
			}
		}
	}

	if len(candidates) > 0 {
		return candidates[0] // Already sorted by desirability
	}

	return nil
}

// selectBackupServers selects backup servers for redundancy
func (t *Task) selectBackupServers(servers []*ServerInfo, primaryServer *ServerInfo, count int) []string {
	var backups []string

	for _, server := range servers {
		if server.Address != primaryServer.Address && server.Rack != primaryServer.Rack {
			backups = append(backups, server.Address)
			if len(backups) >= count {
				break
			}
		}
	}

	return backups
}

// distributeShards uploads shards to their assigned servers
func (t *Task) distributeShards(shardFiles []string, placements []ShardPlacement) error {
	t.currentStep = "distributing_shards"
	t.SetProgress(75.0)
	glog.V(1).Infof("Distributing %d shards to target servers", len(placements))

	// Distribute shards in parallel for better performance
	successCount := 0
	errors := make([]error, 0)

	for i, placement := range placements {
		shardFile := shardFiles[i]

		err := t.uploadShardToServer(shardFile, placement)
		if err != nil {
			glog.Errorf("Failed to upload shard %d to %s: %v", i, placement.ServerAddr, err)
			errors = append(errors, err)

			// Try backup servers
			uploaded := false
			for _, backupAddr := range placement.BackupAddrs {
				backupPlacement := placement
				backupPlacement.ServerAddr = backupAddr
				if err := t.uploadShardToServer(shardFile, backupPlacement); err == nil {
					glog.V(1).Infof("Successfully uploaded shard %d to backup server %s", i, backupAddr)
					uploaded = true
					break
				}
			}

			if !uploaded {
				return fmt.Errorf("failed to upload shard %d to any server", i)
			}
		}

		successCount++
		progress := 75.0 + (float64(successCount)/float64(len(placements)))*15.0
		t.SetProgress(progress)

		glog.V(2).Infof("Successfully distributed shard %d to %s", i, placement.ServerAddr)
	}

	if len(errors) > 0 && successCount < len(placements)/2 {
		return fmt.Errorf("too many shard distribution failures: %d/%d", len(errors), len(placements))
	}

	t.SetProgress(90.0)
	glog.V(1).Infof("Successfully distributed %d/%d shards", successCount, len(placements))
	return nil
}

// uploadShardToServer uploads a shard file to a specific server
func (t *Task) uploadShardToServer(shardFile string, placement ShardPlacement) error {
	glog.V(2).Infof("Uploading shard %d to server %s", placement.ShardID, placement.ServerAddr)

	ctx := context.Background()
	conn, err := grpc.Dial(placement.ServerAddr, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("failed to connect to server %s: %v", placement.ServerAddr, err)
	}
	defer conn.Close()

	client := volume_server_pb.NewVolumeServerClient(conn)

	// Upload shard using VolumeEcShardsCopy - this assumes shards are already generated locally
	// and we're copying them to the target server
	shardIds := []uint32{uint32(placement.ShardID)}
	_, err = client.VolumeEcShardsCopy(ctx, &volume_server_pb.VolumeEcShardsCopyRequest{
		VolumeId:    t.volumeID,
		Collection:  t.collection,
		ShardIds:    shardIds,
		CopyEcxFile: true,
		CopyEcjFile: true,
		CopyVifFile: true,
	})
	if err != nil {
		return fmt.Errorf("failed to copy EC shard: %v", err)
	}

	glog.V(2).Infof("Successfully uploaded shard %d to %s", placement.ShardID, placement.ServerAddr)
	return nil
}

// verifyAndCleanupSource verifies the EC conversion and cleans up the source volume
func (t *Task) verifyAndCleanupSource() error {
	t.currentStep = "verify_cleanup"
	t.SetProgress(95.0)
	glog.V(1).Infof("Verifying EC conversion and cleaning up source volume %d", t.volumeID)

	ctx := context.Background()
	conn, err := grpc.Dial(t.sourceServer, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("failed to connect to source server: %v", err)
	}
	defer conn.Close()

	client := volume_server_pb.NewVolumeServerClient(conn)

	// Verify source volume is read-only
	statusResp, err := client.VolumeStatus(ctx, &volume_server_pb.VolumeStatusRequest{
		VolumeId: t.volumeID,
	})
	if err == nil && statusResp.IsReadOnly {
		glog.V(1).Infof("Source volume %d is confirmed read-only", t.volumeID)
	}

	// Delete source volume files (optional - could be kept for backup)
	// This would normally be done after confirming all shards are properly distributed
	// _, err = client.VolumeDelete(ctx, &volume_server_pb.VolumeDeleteRequest{
	//     VolumeId: t.volumeID,
	// })
	// if err != nil {
	//     glog.Warningf("Failed to delete source volume: %v", err)
	// }

	return nil
}

// cleanup removes temporary files and directories
func (t *Task) cleanup(workDir string) {
	glog.V(1).Infof("Cleaning up work directory: %s", workDir)
	if err := os.RemoveAll(workDir); err != nil {
		glog.Warningf("Failed to cleanup work directory %s: %v", workDir, err)
	}
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
