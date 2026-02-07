package table_maintenance

import (
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Global variable to hold the task definition for configuration updates
var globalTaskDef *base.TaskDefinition

// Auto-register this task when the package is imported
func init() {
	RegisterTableMaintenanceTask()

	// Register config updater
	tasks.AutoRegisterConfigUpdater(types.TaskTypeTableMaintenance, UpdateConfigFromPersistence)
}

// RegisterTableMaintenanceTask registers the table maintenance task with the task system
func RegisterTableMaintenanceTask() {
	// Create configuration instance
	config := NewDefaultConfig()

	// Create complete task definition
	taskDef := &base.TaskDefinition{
		Type:         types.TaskTypeTableMaintenance,
		Name:         "table_maintenance",
		DisplayName:  "Table Maintenance",
		Description:  "Performs maintenance operations on S3 Table Buckets including compaction, snapshot expiration, and orphan cleanup",
		Icon:         "fas fa-table text-info",
		Capabilities: []string{"table_maintenance", "iceberg", "s3tables"},

		Config:     config,
		ConfigSpec: GetConfigSpec(),
		CreateTask: func(params *worker_pb.TaskParams) (types.Task, error) {
			if params == nil {
				return nil, fmt.Errorf("task parameters are required")
			}
			if len(params.Sources) == 0 {
				return nil, fmt.Errorf("at least one source (table path) is required")
			}

			// Parse table info from parameters
			tablePath := params.Sources[0].Node
			tableBucket := params.Collection

			// Determine job type from source node format:
			// Format: "job_type:table_path" (e.g., "compaction:/table-buckets/bucket/ns/table")
			// If no prefix, default to compaction for backward compatibility.
			// NOTE: A proper implementation would define TableMaintenanceTaskParams in
			// weed/pb/worker.proto to pass job details explicitly, similar to VacuumTaskParams.
			jobType := JobTypeCompaction
			if colonIdx := strings.Index(tablePath, ":"); colonIdx > 0 && colonIdx < len(tablePath)-1 {
				jobTypeStr := tablePath[:colonIdx]
				tablePath = tablePath[colonIdx+1:]
				switch TableMaintenanceJobType(jobTypeStr) {
				case JobTypeCompaction:
					jobType = JobTypeCompaction
				case JobTypeSnapshotExpiration:
					jobType = JobTypeSnapshotExpiration
				case JobTypeOrphanCleanup:
					jobType = JobTypeOrphanCleanup
				case JobTypeManifestRewrite:
					jobType = JobTypeManifestRewrite
				default:
					glog.Warningf("Unknown job type '%s', defaulting to compaction", jobTypeStr)
				}
			}

			// Parse namespace and tableName from tablePath
			// Expected format: /table-buckets/bucketName/namespaceName/tableName
			namespace := ""
			tableName := ""
			parts := strings.Split(tablePath, "/")
			if len(parts) > 3 {
				namespace = parts[3]
			}
			if len(parts) > 4 {
				tableName = parts[4]
			}

			// Create the maintenance job
			job := &TableMaintenanceJob{
				JobType:     jobType,
				TableBucket: tableBucket,
				TablePath:   tablePath,
				Priority:    types.TaskPriorityNormal,
				CreatedAt:   time.Now(),
			}

			return NewTableMaintenanceTask(
				fmt.Sprintf("table-maintenance-%s-%d", tableBucket, time.Now().UnixNano()),
				tableBucket,
				namespace,
				tableName,
				job,
			), nil
		},
		DetectionFunc:  Detection,
		ScanInterval:   30 * time.Minute,
		SchedulingFunc: Scheduling,
		MaxConcurrent:  2,
		RepeatInterval: 24 * time.Hour,
	}

	// Store task definition globally for configuration updates
	globalTaskDef = taskDef

	// Register everything with a single function call
	base.RegisterTask(taskDef)

	glog.V(1).Infof("Registered table_maintenance task type")
}

// UpdateConfigFromPersistence updates the table maintenance configuration from persistence
func UpdateConfigFromPersistence(configPersistence interface{}) error {
	if globalTaskDef == nil {
		return fmt.Errorf("table_maintenance task not registered")
	}

	// Load configuration from persistence
	newConfig := LoadConfigFromPersistence(configPersistence)
	if newConfig == nil {
		return fmt.Errorf("failed to load configuration from persistence")
	}

	// Update the task definition's config
	globalTaskDef.Config = newConfig

	glog.V(1).Infof("Updated table_maintenance task configuration from persistence")
	return nil
}
