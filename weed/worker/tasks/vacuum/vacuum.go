package vacuum

import (
	"context"
	"fmt"
	"io"
	"time"

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
	// Use BaseTask.ExecuteTask to handle logging initialization
	return t.ExecuteTask(context.Background(), params, t.executeImpl)
}

// executeImpl is the actual vacuum implementation
func (t *Task) executeImpl(ctx context.Context, params types.TaskParams) error {
	t.LogInfo("Starting vacuum for volume %d on server %s", t.volumeID, t.server)

	// Parse garbage threshold from typed parameters
	if params.TypedParams != nil {
		if vacuumParams := params.TypedParams.GetVacuumParams(); vacuumParams != nil {
			t.garbageThreshold = vacuumParams.GarbageThreshold
			t.LogWithFields("INFO", "Using garbage threshold from parameters", map[string]interface{}{
				"threshold": t.garbageThreshold,
			})
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
		t.LogError("Failed to connect to volume server %s: %v", t.server, err)
		return fmt.Errorf("failed to connect to volume server %s: %v", t.server, err)
	}
	defer conn.Close()

	client := volume_server_pb.NewVolumeServerClient(conn)

	// Step 1: Check vacuum eligibility
	t.SetProgress(10.0)
	t.LogDebug("Checking vacuum eligibility for volume %d", t.volumeID)

	checkResp, err := client.VacuumVolumeCheck(ctx, &volume_server_pb.VacuumVolumeCheckRequest{
		VolumeId: t.volumeID,
	})
	if err != nil {
		t.LogError("Vacuum check failed for volume %d: %v", t.volumeID, err)
		return fmt.Errorf("vacuum check failed for volume %d: %v", t.volumeID, err)
	}

	// Check if garbage ratio meets threshold
	if checkResp.GarbageRatio < t.garbageThreshold {
		t.LogWarning("Volume %d garbage ratio %.2f%% is below threshold %.2f%%, skipping vacuum",
			t.volumeID, checkResp.GarbageRatio*100, t.garbageThreshold*100)
		return fmt.Errorf("volume %d garbage ratio %.2f%% is below threshold %.2f%%, skipping vacuum",
			t.volumeID, checkResp.GarbageRatio*100, t.garbageThreshold*100)
	}

	t.LogWithFields("INFO", "Volume eligible for vacuum", map[string]interface{}{
		"volume_id":       t.volumeID,
		"garbage_ratio":   checkResp.GarbageRatio,
		"threshold":       t.garbageThreshold,
		"garbage_percent": checkResp.GarbageRatio * 100,
	})

	// Step 2: Compact volume
	t.SetProgress(30.0)
	t.LogInfo("Starting compact for volume %d", t.volumeID)

	compactStream, err := client.VacuumVolumeCompact(ctx, &volume_server_pb.VacuumVolumeCompactRequest{
		VolumeId: t.volumeID,
	})
	if err != nil {
		t.LogError("Vacuum compact failed for volume %d: %v", t.volumeID, err)
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
			t.LogError("Vacuum compact stream error for volume %d: %v", t.volumeID, err)
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

		t.LogWithFields("DEBUG", "Volume compact progress", map[string]interface{}{
			"volume_id":        t.volumeID,
			"processed_bytes":  processedBytes,
			"total_bytes":      totalBytes,
			"compact_progress": fmt.Sprintf("%.1f%%", (float64(processedBytes)/float64(totalBytes))*100),
		})
	}

	// Step 3: Commit vacuum changes
	t.SetProgress(80.0)
	t.LogInfo("Committing vacuum for volume %d", t.volumeID)

	commitResp, err := client.VacuumVolumeCommit(ctx, &volume_server_pb.VacuumVolumeCommitRequest{
		VolumeId: t.volumeID,
	})
	if err != nil {
		t.LogError("Vacuum commit failed for volume %d: %v", t.volumeID, err)
		return fmt.Errorf("vacuum commit failed for volume %d: %v", t.volumeID, err)
	}

	// Step 4: Cleanup temporary files
	t.SetProgress(90.0)
	t.LogInfo("Cleaning up vacuum files for volume %d", t.volumeID)

	_, err = client.VacuumVolumeCleanup(ctx, &volume_server_pb.VacuumVolumeCleanupRequest{
		VolumeId: t.volumeID,
	})
	if err != nil {
		// Log warning but don't fail the task
		t.LogWarning("Vacuum cleanup warning for volume %d: %v", t.volumeID, err)
	}

	t.SetProgress(100.0)

	newVolumeSize := commitResp.VolumeSize
	t.LogWithFields("INFO", "Successfully completed vacuum", map[string]interface{}{
		"volume_id":         t.volumeID,
		"server":            t.server,
		"new_volume_size":   newVolumeSize,
		"garbage_reclaimed": true,
	})

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

	// Use default estimation since volume size is not available in typed params
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
