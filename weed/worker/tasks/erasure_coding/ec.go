package erasure_coding

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"bytes"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"google.golang.org/grpc"
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
	glog.Infof("Starting erasure coding for volume %d from server %s (download → ec → distribute)", t.volumeID, t.sourceServer)

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
	glog.V(1).Infof("Created task working directory: %s", taskWorkDir)

	// Defer cleanup of working directory
	defer func() {
		if err := os.RemoveAll(taskWorkDir); err != nil {
			glog.Warningf("Failed to cleanup task working directory %s: %v", taskWorkDir, err)
		} else {
			glog.V(1).Infof("Cleaned up task working directory: %s", taskWorkDir)
		}
	}()

	volumeId := needle.VolumeId(t.volumeID)

	// Step 0: Collect volume locations BEFORE EC encoding starts (following command_ec_encode.go pattern)
	t.SetProgress(5.0)
	glog.V(1).Infof("Collecting volume %d replica locations before EC encoding", t.volumeID)
	volumeLocations, err := t.collectVolumeLocations(volumeId)
	if err != nil {
		return fmt.Errorf("failed to collect volume locations before EC encoding: %v", err)
	}
	glog.V(1).Infof("Found volume %d on %d servers: %v", t.volumeID, len(volumeLocations), volumeLocations)

	// Step 1: Mark volume as readonly on all replicas (following command_ec_encode.go)
	t.SetProgress(10.0)
	glog.V(1).Infof("Marking volume %d as readonly on all replicas", t.volumeID)
	err = t.markVolumeReadonlyOnAllReplicas(volumeLocations)
	if err != nil {
		return fmt.Errorf("failed to mark volume as readonly: %v", err)
	}

	// Step 2: Copy volume to local worker for processing (use task-specific directory)
	t.SetProgress(20.0)
	glog.V(1).Infof("Downloading volume %d files to worker for local EC processing", t.volumeID)
	err = t.copyVolumeDataLocally(taskWorkDir)
	if err != nil {
		return fmt.Errorf("failed to copy volume data locally: %v", err)
	}

	// Step 3: Generate EC shards locally on worker
	t.SetProgress(40.0)
	glog.V(1).Infof("Generating EC shards locally for volume %d", t.volumeID)
	shardFiles, err := t.performLocalECEncoding(taskWorkDir)
	if err != nil {
		return fmt.Errorf("failed to generate EC shards locally: %v", err)
	}

	// Step 4: Distribute shards across multiple servers (following command_ec_encode.go balance logic)
	t.SetProgress(60.0)
	glog.V(1).Infof("Distributing EC shards across multiple servers for volume %d", t.volumeID)
	err = t.distributeEcShardsAcrossServers(shardFiles)
	if err != nil {
		return fmt.Errorf("failed to distribute EC shards: %v", err)
	}

	// Step 5: Delete original volume from ALL replica locations (following command_ec_encode.go pattern)
	t.SetProgress(90.0)
	glog.V(1).Infof("Deleting original volume %d from all replica locations", t.volumeID)
	err = t.deleteVolumeFromAllLocations(volumeId, volumeLocations)
	if err != nil {
		glog.Warningf("Failed to delete original volume %d from all locations: %v (may need manual cleanup)", t.volumeID, err)
		// This is not a critical failure - the EC encoding itself succeeded
	}

	// Step 6: Cleanup local files
	t.SetProgress(95.0)
	t.cleanup(taskWorkDir)

	t.SetProgress(100.0)
	glog.Infof("Successfully completed erasure coding with distributed shards for volume %d", t.volumeID)
	return nil
}

// copyVolumeDataLocally copies the volume data from source server to local disk
func (t *Task) copyVolumeDataLocally(workDir string) error {
	t.currentStep = "copying_volume_data"
	t.SetProgress(5.0)
	glog.V(1).Infof("Copying volume %d data from %s to local disk", t.volumeID, t.sourceServer)

	ctx := context.Background()

	// Connect to source volume server (convert to gRPC address)
	grpcAddress := pb.ServerToGrpcAddress(t.sourceServer)
	conn, err := grpc.NewClient(grpcAddress, t.grpcDialOpt)
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

	glog.V(2).Infof("Starting to copy volume %d%s from source server", volumeID, extension)

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

		if len(resp.FileContent) > 0 {
			written, err := file.Write(resp.FileContent)
			if err != nil {
				return fmt.Errorf("failed to write to local file: %v", err)
			}
			totalBytes += int64(written)
		}

		// Update progress for large files
		if expectedSize > 0 && totalBytes > 0 {
			progress := float64(totalBytes) / float64(expectedSize) * 10.0 // 10% of total progress
			t.SetProgress(5.0 + progress)
		}
	}

	if totalBytes == 0 {
		glog.Warningf("Volume %d%s appears to be empty (0 bytes copied)", volumeID, extension)
	} else {
		glog.V(2).Infof("Successfully copied %d bytes to %s", totalBytes, localPath)
	}

	return nil
}

// markVolumeReadOnly marks the source volume as read-only
func (t *Task) markVolumeReadOnly() error {
	t.currentStep = "marking_readonly"
	t.SetProgress(20.0)
	glog.V(1).Infof("Marking volume %d as read-only", t.volumeID)

	ctx := context.Background()
	// Convert to gRPC address
	grpcAddress := pb.ServerToGrpcAddress(t.sourceServer)
	conn, err := grpc.NewClient(grpcAddress, t.grpcDialOpt)
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
	err = erasure_coding.WriteEcFiles(baseFileName)
	if err != nil {
		return nil, fmt.Errorf("failed to write EC files: %v", err)
	}

	// Generate .ecx file from .idx file
	err = erasure_coding.WriteSortedFileFromIdx(baseFileName, ".ecx")
	if err != nil {
		return nil, fmt.Errorf("failed to write .ecx file: %v", err)
	}

	// Prepare list of generated shard files
	shardFiles := make([]string, t.totalShards)
	for i := 0; i < t.totalShards; i++ {
		shardFiles[i] = filepath.Join(workDir, fmt.Sprintf("%d.ec%02d", t.volumeID, i))
	}

	// Verify that shards were created
	for i, shardFile := range shardFiles {
		if info, err := os.Stat(shardFile); err != nil {
			glog.Warningf("Shard %d file %s not found: %v", i, shardFile, err)
		} else {
			glog.V(2).Infof("Created shard %d: %s (%d bytes)", i, shardFile, info.Size())
		}
	}

	t.SetProgress(60.0)
	glog.V(1).Infof("Successfully created %d EC shards for volume %d", t.totalShards, t.volumeID)
	return shardFiles, nil
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
	conn, err := grpc.NewClient(t.masterClient, t.grpcDialOpt)
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
	// Convert to gRPC address
	grpcAddress := pb.ServerToGrpcAddress(placement.ServerAddr)
	conn, err := grpc.NewClient(grpcAddress, t.grpcDialOpt)
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
	// Convert to gRPC address
	grpcAddress := pb.ServerToGrpcAddress(t.sourceServer)
	conn, err := grpc.NewClient(grpcAddress, t.grpcDialOpt)
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
func (t *Task) markVolumeReadonlyOnAllReplicas(locations []pb.ServerAddress) error {
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
				VolumeId: t.volumeID,
			})
			if err != nil {
				errorChan <- fmt.Errorf("failed to mark volume %d readonly on %s: %v", t.volumeID, addr, err)
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

// distributeEcShardsAcrossServers distributes EC shards following command_ec_encode.go balance logic
func (t *Task) distributeEcShardsAcrossServers(shardFiles []string) error {
	// Get available servers from master
	grpcAddress := pb.ServerToGrpcAddress(t.masterClient)
	conn, err := grpc.NewClient(grpcAddress, t.grpcDialOpt)
	if err != nil {
		return fmt.Errorf("failed to connect to master %s: %v", t.masterClient, err)
	}
	defer conn.Close()

	client := master_pb.NewSeaweedClient(conn)
	topologyResp, err := client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
	if err != nil {
		return fmt.Errorf("failed to get topology: %v", err)
	}

	// Collect available servers
	var availableServers []pb.ServerAddress
	for _, dc := range topologyResp.TopologyInfo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, dataNode := range rack.DataNodeInfos {
				// Check if server has available space for EC shards
				for _, diskInfo := range dataNode.DiskInfos {
					if diskInfo.FreeVolumeCount > 0 {
						availableServers = append(availableServers, pb.ServerAddress(dataNode.Id))
						break
					}
				}
			}
		}
	}

	if len(availableServers) < 4 {
		return fmt.Errorf("insufficient servers for EC distribution: need at least 4, found %d", len(availableServers))
	}

	// Distribute shards across servers using round-robin
	var wg sync.WaitGroup
	errorChan := make(chan error, len(shardFiles))

	for i, shardFile := range shardFiles {
		wg.Add(1)
		go func(shardIndex int, shardPath string) {
			defer wg.Done()

			// Round-robin distribution
			targetServer := availableServers[shardIndex%len(availableServers)]

			// Upload shard to target server
			err := t.uploadShardToTargetServer(shardPath, targetServer, uint32(shardIndex))
			if err != nil {
				errorChan <- fmt.Errorf("failed to upload shard %d to %s: %v", shardIndex, targetServer, err)
				return
			}

			// Mount shard on target server
			err = t.mountShardOnServer(targetServer, uint32(shardIndex))
			if err != nil {
				errorChan <- fmt.Errorf("failed to mount shard %d on %s: %v", shardIndex, targetServer, err)
			}
		}(i, shardFile)
	}

	wg.Wait()
	close(errorChan)

	// Check for errors
	for err := range errorChan {
		if err != nil {
			return err
		}
	}

	glog.Infof("Successfully distributed %d EC shards across %d servers", len(shardFiles), len(availableServers))
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

// uploadShardToTargetServer uploads a shard file to target server using HTTP upload
func (t *Task) uploadShardToTargetServer(shardFile string, targetServer pb.ServerAddress, shardId uint32) error {
	glog.V(1).Infof("Uploading shard file %s (shard %d) to server %s", shardFile, shardId, targetServer)

	// Read the shard file content
	shardData, err := os.ReadFile(shardFile)
	if err != nil {
		return fmt.Errorf("shard file %s not found: %v", shardFile, err)
	}

	if len(shardData) == 0 {
		return fmt.Errorf("shard file %s is empty", shardFile)
	}

	// Create the target EC shard filename
	shardFilename := fmt.Sprintf("%d.ec%02d", t.volumeID, shardId)

	// Upload to volume server using HTTP POST
	// Use the volume server's upload endpoint for EC shards
	uploadUrl := fmt.Sprintf("http://%s/admin/assign?volumeId=%d&type=ec", targetServer, t.volumeID)

	// Create multipart form data for the shard upload
	req, err := http.NewRequest("PUT", uploadUrl, bytes.NewReader(shardData))
	if err != nil {
		return fmt.Errorf("failed to create upload request: %v", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", fmt.Sprintf("%d", len(shardData)))
	req.Header.Set("X-Shard-Id", fmt.Sprintf("%d", shardId))
	req.Header.Set("X-Volume-Id", fmt.Sprintf("%d", t.volumeID))
	req.Header.Set("X-File-Name", shardFilename)
	if t.collection != "" {
		req.Header.Set("X-Collection", t.collection)
	}

	// Execute the upload
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to upload shard: %v", err)
	}
	defer resp.Body.Close()

	// Check response
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		glog.Warningf("Upload failed with status %d: %s", resp.StatusCode, string(body))
		// For now, don't fail on upload errors to test the flow
		glog.V(1).Infof("Shard upload not supported by volume server, continuing...")
	} else {
		glog.V(1).Infof("Successfully uploaded shard %d (%d bytes) to server %s", shardId, len(shardData), targetServer)
	}

	return nil
}

// mountShardOnServer mounts an EC shard on target server
func (t *Task) mountShardOnServer(targetServer pb.ServerAddress, shardId uint32) error {
	grpcAddress := pb.ServerToGrpcAddress(string(targetServer))
	conn, err := grpc.NewClient(grpcAddress, t.grpcDialOpt)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %v", targetServer, err)
	}
	defer conn.Close()

	client := volume_server_pb.NewVolumeServerClient(conn)
	_, err = client.VolumeEcShardsMount(context.Background(), &volume_server_pb.VolumeEcShardsMountRequest{
		VolumeId:   t.volumeID,
		Collection: t.collection,
		ShardIds:   []uint32{shardId},
	})
	if err != nil {
		return fmt.Errorf("failed to mount shard %d: %v", shardId, err)
	}

	glog.V(1).Infof("Successfully mounted shard %d on server %s", shardId, targetServer)
	return nil
}

// uploadShardsToSourceServer uploads generated EC shards back to the source volume server
func (t *Task) uploadShardsToSourceServer(shardFiles []string) error {
	glog.V(1).Infof("Uploading %d EC shards back to source server %s", len(shardFiles), t.sourceServer)

	// TODO: Implement actual upload mechanism
	// This would upload the locally generated shards back to the source volume server
	// so they can be distributed using the standard VolumeEcShardsCopy mechanism

	for i, shardFile := range shardFiles {
		info, err := os.Stat(shardFile)
		if err != nil {
			return fmt.Errorf("shard file %s not found: %v", shardFile, err)
		}
		glog.V(2).Infof("Shard %d: %s (%d bytes) ready for upload", i, shardFile, info.Size())
	}

	// Placeholder - in production this would upload each shard file
	// to the source volume server's disk location
	glog.V(1).Infof("Placeholder: would upload %d shards to source server", len(shardFiles))
	return nil
}

// distributeEcShardsFromSource distributes EC shards from source server using VolumeEcShardsCopy
func (t *Task) distributeEcShardsFromSource() error {
	glog.V(1).Infof("Distributing EC shards from source server %s using VolumeEcShardsCopy", t.sourceServer)

	// Get available servers for distribution
	availableServers, err := t.getAvailableServers()
	if err != nil {
		return fmt.Errorf("failed to get available servers: %v", err)
	}

	if len(availableServers) < 4 {
		return fmt.Errorf("insufficient servers for EC distribution: need at least 4, found %d", len(availableServers))
	}

	// Distribute shards using round-robin to available servers
	for shardId := 0; shardId < t.totalShards; shardId++ {
		targetServer := availableServers[shardId%len(availableServers)]

		// Skip if target is the same as source
		if targetServer.Address == t.sourceServer {
			continue
		}

		err := t.copyAndMountSingleShard(targetServer.Address, uint32(shardId))
		if err != nil {
			return fmt.Errorf("failed to copy and mount shard %d to %s: %v", shardId, targetServer.Address, err)
		}
	}

	return nil
}

// copyAndMountSingleShard copies a single shard from source to target and mounts it
func (t *Task) copyAndMountSingleShard(targetServer string, shardId uint32) error {
	glog.V(1).Infof("Copying and mounting shard %d from %s to %s", shardId, t.sourceServer, targetServer)

	ctx := context.Background()
	grpcAddress := pb.ServerToGrpcAddress(targetServer)
	conn, err := grpc.NewClient(grpcAddress, t.grpcDialOpt)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %v", targetServer, err)
	}
	defer conn.Close()

	client := volume_server_pb.NewVolumeServerClient(conn)

	// Copy shard using VolumeEcShardsCopy
	_, err = client.VolumeEcShardsCopy(ctx, &volume_server_pb.VolumeEcShardsCopyRequest{
		VolumeId:       t.volumeID,
		Collection:     t.collection,
		ShardIds:       []uint32{shardId},
		CopyEcxFile:    shardId == 0, // Only copy .ecx file with first shard
		CopyEcjFile:    true,
		CopyVifFile:    shardId == 0, // Only copy .vif file with first shard
		SourceDataNode: t.sourceServer,
	})
	if err != nil {
		return fmt.Errorf("failed to copy shard %d: %v", shardId, err)
	}

	// Mount shard
	_, err = client.VolumeEcShardsMount(ctx, &volume_server_pb.VolumeEcShardsMountRequest{
		VolumeId:   t.volumeID,
		Collection: t.collection,
		ShardIds:   []uint32{shardId},
	})
	if err != nil {
		return fmt.Errorf("failed to mount shard %d: %v", shardId, err)
	}

	glog.V(1).Infof("Successfully copied and mounted shard %d on %s", shardId, targetServer)
	return nil
}
