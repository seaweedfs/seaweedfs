package vacuum

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/config"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
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

// VacuumConfigV2 extends BaseConfig with vacuum-specific settings
type VacuumConfigV2 struct {
	base.BaseConfig
	GarbageThreshold    float64 `json:"garbage_threshold"`
	MinVolumeAgeSeconds int     `json:"min_volume_age_seconds"`
	MinIntervalSeconds  int     `json:"min_interval_seconds"`
}

// vacuumDetection implements the detection logic for vacuum tasks
func vacuumDetection(metrics []*types.VolumeHealthMetrics, clusterInfo *types.ClusterInfo, config base.TaskConfig) ([]*types.TaskDetectionResult, error) {
	if !config.IsEnabled() {
		return nil, nil
	}

	vacuumConfig := config.(*VacuumConfigV2)
	var results []*types.TaskDetectionResult
	minVolumeAge := time.Duration(vacuumConfig.MinVolumeAgeSeconds) * time.Second

	for _, metric := range metrics {
		// Check if volume needs vacuum
		if metric.GarbageRatio >= vacuumConfig.GarbageThreshold && metric.Age >= minVolumeAge {
			priority := types.TaskPriorityNormal
			if metric.GarbageRatio > 0.6 {
				priority = types.TaskPriorityHigh
			}

			result := &types.TaskDetectionResult{
				TaskType:   types.TaskTypeVacuum,
				VolumeID:   metric.VolumeID,
				Server:     metric.Server,
				Collection: metric.Collection,
				Priority:   priority,
				Reason:     "Volume has excessive garbage requiring vacuum",
				Parameters: map[string]interface{}{
					"garbage_ratio": metric.GarbageRatio,
					"volume_age":    metric.Age.String(),
				},
				ScheduleAt: time.Now(),
			}
			results = append(results, result)
		}
	}

	return results, nil
}

// vacuumScheduling implements the scheduling logic for vacuum tasks
func vacuumScheduling(task *types.Task, runningTasks []*types.Task, availableWorkers []*types.Worker, config base.TaskConfig) bool {
	vacuumConfig := config.(*VacuumConfigV2)

	// Count running vacuum tasks
	runningVacuumCount := 0
	for _, runningTask := range runningTasks {
		if runningTask.Type == types.TaskTypeVacuum {
			runningVacuumCount++
		}
	}

	// Check concurrency limit
	if runningVacuumCount >= vacuumConfig.MaxConcurrent {
		return false
	}

	// Check for available workers with vacuum capability
	for _, worker := range availableWorkers {
		if worker.CurrentLoad < worker.MaxConcurrent {
			for _, capability := range worker.Capabilities {
				if capability == types.TaskTypeVacuum {
					return true
				}
			}
		}
	}

	return false
}

// createVacuumTask creates a vacuum task instance
func createVacuumTask(params types.TaskParams) (types.TaskInterface, error) {
	// Validate parameters
	if params.VolumeID == 0 {
		return nil, fmt.Errorf("volume_id is required")
	}
	if params.Server == "" {
		return nil, fmt.Errorf("server is required")
	}

	// Use existing vacuum task implementation
	task := NewTask(params.Server, params.VolumeID)
	task.SetEstimatedDuration(task.EstimateTime(params))
	return task, nil
}

// getVacuumConfigSpec returns the configuration schema for vacuum tasks
func getVacuumConfigSpec() base.ConfigSpec {
	return base.ConfigSpec{
		Fields: []*config.Field{
			{
				Name:         "enabled",
				JSONName:     "enabled",
				Type:         config.FieldTypeBool,
				DefaultValue: true,
				Required:     false,
				DisplayName:  "Enable Vacuum Tasks",
				Description:  "Whether vacuum tasks should be automatically created",
				HelpText:     "Toggle this to enable or disable automatic vacuum task generation",
				InputType:    "checkbox",
				CSSClasses:   "form-check-input",
			},
			{
				Name:         "garbage_threshold",
				JSONName:     "garbage_threshold",
				Type:         config.FieldTypeFloat,
				DefaultValue: 0.3,
				MinValue:     0.0,
				MaxValue:     1.0,
				Required:     true,
				DisplayName:  "Garbage Percentage Threshold",
				Description:  "Trigger vacuum when garbage ratio exceeds this percentage",
				HelpText:     "Volumes with more deleted content than this threshold will be vacuumed",
				Placeholder:  "0.30 (30%)",
				Unit:         config.UnitNone,
				InputType:    "number",
				CSSClasses:   "form-control",
			},
			{
				Name:         "scan_interval_seconds",
				JSONName:     "scan_interval_seconds",
				Type:         config.FieldTypeInterval,
				DefaultValue: 2 * 60 * 60,
				MinValue:     10 * 60,
				MaxValue:     24 * 60 * 60,
				Required:     true,
				DisplayName:  "Scan Interval",
				Description:  "How often to scan for volumes needing vacuum",
				HelpText:     "The system will check for volumes that need vacuuming at this interval",
				Placeholder:  "2",
				Unit:         config.UnitHours,
				InputType:    "interval",
				CSSClasses:   "form-control",
			},
			{
				Name:         "max_concurrent",
				JSONName:     "max_concurrent",
				Type:         config.FieldTypeInt,
				DefaultValue: 2,
				MinValue:     1,
				MaxValue:     10,
				Required:     true,
				DisplayName:  "Max Concurrent Tasks",
				Description:  "Maximum number of vacuum tasks that can run simultaneously",
				HelpText:     "Limits the number of vacuum operations running at the same time to control system load",
				Placeholder:  "2 (default)",
				Unit:         config.UnitCount,
				InputType:    "number",
				CSSClasses:   "form-control",
			},
			{
				Name:         "min_volume_age_seconds",
				JSONName:     "min_volume_age_seconds",
				Type:         config.FieldTypeInterval,
				DefaultValue: 24 * 60 * 60,
				MinValue:     1 * 60 * 60,
				MaxValue:     7 * 24 * 60 * 60,
				Required:     true,
				DisplayName:  "Minimum Volume Age",
				Description:  "Only vacuum volumes older than this duration",
				HelpText:     "Prevents vacuuming of recently created volumes that may still be actively written to",
				Placeholder:  "24",
				Unit:         config.UnitHours,
				InputType:    "interval",
				CSSClasses:   "form-control",
			},
			{
				Name:         "min_interval_seconds",
				JSONName:     "min_interval_seconds",
				Type:         config.FieldTypeInterval,
				DefaultValue: 7 * 24 * 60 * 60,
				MinValue:     1 * 24 * 60 * 60,
				MaxValue:     30 * 24 * 60 * 60,
				Required:     true,
				DisplayName:  "Minimum Interval",
				Description:  "Minimum time between vacuum operations on the same volume",
				HelpText:     "Prevents excessive vacuuming of the same volume by enforcing a minimum wait time",
				Placeholder:  "7",
				Unit:         config.UnitDays,
				InputType:    "interval",
				CSSClasses:   "form-control",
			},
		},
	}
}

// initVacuumV2 registers the refactored vacuum task (replaces the old registration)
func initVacuumV2() {
	// Create configuration instance
	config := &VacuumConfigV2{
		BaseConfig: base.BaseConfig{
			Enabled:             true,
			ScanIntervalSeconds: 2 * 60 * 60, // 2 hours
			MaxConcurrent:       2,
		},
		GarbageThreshold:    0.3,              // 30%
		MinVolumeAgeSeconds: 24 * 60 * 60,     // 24 hours
		MinIntervalSeconds:  7 * 24 * 60 * 60, // 7 days
	}

	// Create complete task definition
	taskDef := &base.TaskDefinition{
		Type:         types.TaskTypeVacuum,
		Name:         "vacuum",
		DisplayName:  "Volume Vacuum",
		Description:  "Reclaims disk space by removing deleted files from volumes",
		Icon:         "fas fa-broom text-primary",
		Capabilities: []string{"vacuum", "storage"},

		Config:         config,
		ConfigSpec:     getVacuumConfigSpec(),
		CreateTask:     createVacuumTask,
		DetectionFunc:  vacuumDetection,
		ScanInterval:   2 * time.Hour,
		SchedulingFunc: vacuumScheduling,
		MaxConcurrent:  2,
		RepeatInterval: 7 * 24 * time.Hour,
	}

	// Register everything with a single function call!
	base.RegisterTask(taskDef)
}
