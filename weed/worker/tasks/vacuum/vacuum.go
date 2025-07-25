package vacuum

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Task implements vacuum operation to reclaim disk space
type Task struct {
	*tasks.BaseTask
	server           string
	volumeID         uint32
	garbageThreshold float64
}

// NewTask creates a new vacuum task instance
func NewTask(server string, volumeID uint32) *Task {
	task := &Task{
		BaseTask:         tasks.NewBaseTask(types.TaskTypeVacuum),
		server:           server,
		volumeID:         volumeID,
		garbageThreshold: 0.3, // Default 30% threshold
	}
	return task
}

// Execute performs the vacuum operation
func (t *Task) Execute(params types.TaskParams) error {
	glog.Infof("Starting vacuum for volume %d on server %s", t.volumeID, t.server)

	ctx := context.Background()

	// Parse garbage threshold from parameters
	if thresholdParam, ok := params.Parameters["garbage_threshold"]; ok {
		if thresholdStr, ok := thresholdParam.(string); ok {
			if threshold, err := strconv.ParseFloat(thresholdStr, 64); err == nil {
				t.garbageThreshold = threshold
			}
		}
	}

	// Convert server address to gRPC address and use proper dial option
	grpcAddress := pb.ServerToGrpcAddress(t.server)
	var dialOpt grpc.DialOption = grpc.WithTransportCredentials(insecure.NewCredentials())
	if params.GrpcDialOption != nil {
		dialOpt = params.GrpcDialOption
	}

	conn, err := grpc.NewClient(grpcAddress, dialOpt)
	if err != nil {
		return fmt.Errorf("failed to connect to volume server %s: %v", t.server, err)
	}
	defer conn.Close()

	client := volume_server_pb.NewVolumeServerClient(conn)

	// Step 1: Check vacuum eligibility
	t.SetProgress(10.0)
	glog.V(1).Infof("Checking vacuum eligibility for volume %d", t.volumeID)

	checkResp, err := client.VacuumVolumeCheck(ctx, &volume_server_pb.VacuumVolumeCheckRequest{
		VolumeId: t.volumeID,
	})
	if err != nil {
		return fmt.Errorf("vacuum check failed for volume %d: %v", t.volumeID, err)
	}

	// Check if garbage ratio meets threshold
	if checkResp.GarbageRatio < t.garbageThreshold {
		return fmt.Errorf("volume %d garbage ratio %.2f%% is below threshold %.2f%%, skipping vacuum",
			t.volumeID, checkResp.GarbageRatio*100, t.garbageThreshold*100)
	}

	glog.V(1).Infof("Volume %d has %.2f%% garbage, proceeding with vacuum",
		t.volumeID, checkResp.GarbageRatio*100)

	// Step 2: Compact volume
	t.SetProgress(30.0)
	glog.V(1).Infof("Starting compact for volume %d", t.volumeID)

	compactStream, err := client.VacuumVolumeCompact(ctx, &volume_server_pb.VacuumVolumeCompactRequest{
		VolumeId: t.volumeID,
	})
	if err != nil {
		return fmt.Errorf("vacuum compact failed for volume %d: %v", t.volumeID, err)
	}

	// Process compact stream and track progress
	var processedBytes int64
	var totalBytes int64

	for {
		resp, err := compactStream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("vacuum compact stream error for volume %d: %v", t.volumeID, err)
		}

		processedBytes = resp.ProcessedBytes
		if resp.LoadAvg_1M > 0 {
			totalBytes = int64(resp.LoadAvg_1M) // This is a rough approximation
		}

		// Update progress based on processed bytes (30% to 70% of total progress)
		if totalBytes > 0 {
			compactProgress := float64(processedBytes) / float64(totalBytes)
			if compactProgress > 1.0 {
				compactProgress = 1.0
			}
			progress := 30.0 + (compactProgress * 40.0) // 30% to 70%
			t.SetProgress(progress)
		}

		glog.V(2).Infof("Volume %d compact progress: %d bytes processed", t.volumeID, processedBytes)
	}

	// Step 3: Commit vacuum changes
	t.SetProgress(80.0)
	glog.V(1).Infof("Committing vacuum for volume %d", t.volumeID)

	commitResp, err := client.VacuumVolumeCommit(ctx, &volume_server_pb.VacuumVolumeCommitRequest{
		VolumeId: t.volumeID,
	})
	if err != nil {
		return fmt.Errorf("vacuum commit failed for volume %d: %v", t.volumeID, err)
	}

	// Step 4: Cleanup temporary files
	t.SetProgress(90.0)
	glog.V(1).Infof("Cleaning up vacuum files for volume %d", t.volumeID)

	_, err = client.VacuumVolumeCleanup(ctx, &volume_server_pb.VacuumVolumeCleanupRequest{
		VolumeId: t.volumeID,
	})
	if err != nil {
		// Log warning but don't fail the task
		glog.Warningf("Vacuum cleanup warning for volume %d: %v", t.volumeID, err)
	}

	t.SetProgress(100.0)

	newVolumeSize := commitResp.VolumeSize
	glog.Infof("Successfully completed vacuum for volume %d on server %s, new volume size: %d bytes",
		t.volumeID, t.server, newVolumeSize)

	return nil
}

// Validate validates the task parameters
func (t *Task) Validate(params types.TaskParams) error {
	if params.VolumeID == 0 {
		return fmt.Errorf("volume_id is required")
	}
	if params.Server == "" {
		return fmt.Errorf("server is required")
	}
	return nil
}

// EstimateTime estimates the time needed for the task
func (t *Task) EstimateTime(params types.TaskParams) time.Duration {
	// Base time for vacuum operations - varies by volume size and garbage ratio
	// Typically vacuum is faster than EC encoding
	baseTime := 5 * time.Minute

	// Could adjust based on volume size and garbage ratio if available in params
	if size, ok := params.Parameters["volume_size"].(int64); ok {
		// Rough estimate: 30 seconds per GB for vacuum
		estimatedTime := time.Duration(size/(1024*1024*1024)) * 30 * time.Second
		if estimatedTime > baseTime {
			return estimatedTime
		}
	}

	return baseTime
}

// GetProgress returns the current progress
func (t *Task) GetProgress() float64 {
	return t.BaseTask.GetProgress()
}

// Cancel cancels the task
func (t *Task) Cancel() error {
	return t.BaseTask.Cancel()
}
