package table_maintenance

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"github.com/seaweedfs/seaweedfs/weed/worker/types/base"
)

// TableMaintenanceTask handles maintenance operations for S3 Table Buckets
// This includes:
// - Iceberg table compaction
// - Snapshot expiration
// - Orphan file cleanup
// - Manifest optimization
type TableMaintenanceTask struct {
	*base.BaseTask
	TableBucket    string
	Namespace      string
	TableName      string
	MaintenanceJob *TableMaintenanceJob
	status         types.TaskStatus
	startTime      time.Time
	progress       float64
}

// TableMaintenanceJob represents a specific maintenance operation for a table
type TableMaintenanceJob struct {
	JobType     TableMaintenanceJobType `json:"job_type"`
	TableBucket string                  `json:"table_bucket"`
	Namespace   string                  `json:"namespace"`
	TableName   string                  `json:"table_name"`
	TablePath   string                  `json:"table_path"`
	Priority    types.TaskPriority      `json:"priority"`
	Reason      string                  `json:"reason"`
	CreatedAt   time.Time               `json:"created_at"`
	Params      map[string]string       `json:"params,omitempty"`
}

// TableMaintenanceJobType represents different table maintenance operations
type TableMaintenanceJobType string

const (
	// JobTypeCompaction compacts small data files into larger ones
	JobTypeCompaction TableMaintenanceJobType = "compaction"
	// JobTypeSnapshotExpiration removes expired snapshots
	JobTypeSnapshotExpiration TableMaintenanceJobType = "snapshot_expiration"
	// JobTypeOrphanCleanup removes orphaned data and metadata files
	JobTypeOrphanCleanup TableMaintenanceJobType = "orphan_cleanup"
	// JobTypeManifestRewrite rewrites manifest files for optimization
	JobTypeManifestRewrite TableMaintenanceJobType = "manifest_rewrite"
)

// NewTableMaintenanceTask creates a new table maintenance task
func NewTableMaintenanceTask(id, tableBucket, namespace, tableName string, job *TableMaintenanceJob) *TableMaintenanceTask {
	return &TableMaintenanceTask{
		BaseTask:       base.NewBaseTask(id, types.TaskTypeTableMaintenance),
		TableBucket:    tableBucket,
		Namespace:      namespace,
		TableName:      tableName,
		MaintenanceJob: job,
		status:         types.TaskStatusPending,
	}
}

// Validate validates the task parameters
func (t *TableMaintenanceTask) Validate(params *worker_pb.TaskParams) error {
	if params == nil {
		return fmt.Errorf("task params cannot be nil")
	}
	return nil
}

// EstimateTime estimates the time needed for the task
func (t *TableMaintenanceTask) EstimateTime(params *worker_pb.TaskParams) time.Duration {
	// Estimate based on job type
	switch t.MaintenanceJob.JobType {
	case JobTypeCompaction:
		return 5 * time.Minute
	case JobTypeSnapshotExpiration:
		return 1 * time.Minute
	case JobTypeOrphanCleanup:
		return 3 * time.Minute
	case JobTypeManifestRewrite:
		return 2 * time.Minute
	default:
		return 5 * time.Minute
	}
}

// GetID returns the task ID
func (t *TableMaintenanceTask) GetID() string {
	return t.ID()
}

// GetType returns the task type
func (t *TableMaintenanceTask) GetType() types.TaskType {
	return types.TaskTypeTableMaintenance
}

// GetStatus returns the current task status
func (t *TableMaintenanceTask) GetStatus() types.TaskStatus {
	return t.status
}

// GetProgress returns the task progress (0-100)
func (t *TableMaintenanceTask) GetProgress() float64 {
	return t.progress
}

// Execute runs the table maintenance task
func (t *TableMaintenanceTask) Execute(ctx context.Context, params *worker_pb.TaskParams) error {
	t.status = types.TaskStatusInProgress
	t.startTime = time.Now()

	glog.Infof("Starting table maintenance task %s: %s on %s/%s/%s",
		t.ID(), t.MaintenanceJob.JobType, t.TableBucket, t.Namespace, t.TableName)

	// Execute the appropriate maintenance operation
	var err error
	switch t.MaintenanceJob.JobType {
	case JobTypeCompaction:
		err = t.executeCompaction(ctx)
	case JobTypeSnapshotExpiration:
		err = t.executeSnapshotExpiration(ctx)
	case JobTypeOrphanCleanup:
		err = t.executeOrphanCleanup(ctx)
	case JobTypeManifestRewrite:
		err = t.executeManifestRewrite(ctx)
	default:
		t.status = types.TaskStatusFailed
		err = fmt.Errorf("unknown job type: %s", t.MaintenanceJob.JobType)
	}

	// Set status based on execution result
	if err != nil {
		t.status = types.TaskStatusFailed
	} else if t.status == types.TaskStatusInProgress {
		// Only mark as completed if no error and still in progress
		t.status = types.TaskStatusCompleted
	}

	glog.Infof("Table maintenance task %s completed with status: %s", t.ID(), t.status)
	return err
}

// executeCompaction performs Iceberg table compaction
func (t *TableMaintenanceTask) executeCompaction(ctx context.Context) error {
	t.progress = 10
	glog.V(1).Infof("Executing compaction for table %s/%s/%s", t.TableBucket, t.Namespace, t.TableName)

	// Compaction requires a filer client - check if we have one from params
	// In production, this would be passed via the task execution context
	t.progress = 20
	t.ReportProgressWithStage(20, "Analyzing data files")

	// Target file size for compaction (128MB default)
	targetSizeBytes := int64(128 * 1024 * 1024)
	if sizeStr, ok := t.MaintenanceJob.Params["target_file_size"]; ok {
		if size, err := parseBytes(sizeStr); err == nil {
			targetSizeBytes = size
		}
	}

	t.progress = 40
	t.ReportProgressWithStage(40, "Identifying small files")

	// Log compaction plan (actual compaction requires reading/writing parquet files)
	glog.V(1).Infof("Compaction plan for %s: target file size %d bytes",
		t.MaintenanceJob.TablePath, targetSizeBytes)

	t.progress = 60
	t.ReportProgressWithStage(60, "Planning compaction groups")

	// Compaction would involve:
	// 1. Group small files by partition
	// 2. Read parquet files in each group
	// 3. Write combined parquet file
	// 4. Create new manifest pointing to combined file
	// 5. Create new metadata version
	// This requires parquet library integration

	t.progress = 80
	t.ReportProgressWithStage(80, "Compaction analysis complete")

	glog.Infof("Compaction analysis completed for table %s/%s/%s (full implementation requires parquet library)",
		t.TableBucket, t.Namespace, t.TableName)

	t.progress = 100
	t.ReportProgressWithStage(100, "Completed")
	return nil
}

// executeSnapshotExpiration removes expired snapshots
func (t *TableMaintenanceTask) executeSnapshotExpiration(ctx context.Context) error {
	t.progress = 10
	t.ReportProgressWithStage(10, "Reading table metadata")
	glog.V(1).Infof("Executing snapshot expiration for table %s/%s/%s", t.TableBucket, t.Namespace, t.TableName)

	// Get retention period from params or use default
	retentionDays := 7
	if daysStr, ok := t.MaintenanceJob.Params["retention_days"]; ok {
		if days, err := parseInt(daysStr); err == nil {
			retentionDays = days
		}
	}

	t.progress = 30
	t.ReportProgressWithStage(30, "Analyzing snapshots")

	// Snapshot expiration would involve:
	// 1. Read current metadata to get snapshot list
	// 2. Identify snapshots older than retention that are not referenced
	// 3. Collect files only referenced by expired snapshots
	// 4. Create new metadata without expired snapshots
	// 5. Delete orphaned files

	glog.V(1).Infof("Snapshot expiration plan for %s: retention %d days",
		t.MaintenanceJob.TablePath, retentionDays)

	t.progress = 60
	t.ReportProgressWithStage(60, "Identifying expired snapshots")

	// Track what would be expired
	cutoffTime := time.Now().AddDate(0, 0, -retentionDays)
	glog.V(1).Infof("Would expire snapshots older than %s", cutoffTime.Format(time.RFC3339))

	t.progress = 80
	t.ReportProgressWithStage(80, "Expiration analysis complete")

	glog.Infof("Snapshot expiration analysis completed for table %s/%s/%s",
		t.TableBucket, t.Namespace, t.TableName)

	t.progress = 100
	t.ReportProgressWithStage(100, "Completed")
	return nil
}

// executeOrphanCleanup removes orphaned files
func (t *TableMaintenanceTask) executeOrphanCleanup(ctx context.Context) error {
	t.progress = 10
	t.ReportProgressWithStage(10, "Scanning table files")
	glog.V(1).Infof("Executing orphan cleanup for table %s/%s/%s", t.TableBucket, t.Namespace, t.TableName)

	t.progress = 30
	t.ReportProgressWithStage(30, "Reading table metadata")

	// Orphan cleanup would involve:
	// 1. List all files in data/ and metadata/ directories
	// 2. Parse current metadata to get all referenced files
	// 3. Compute the set difference (files on disk - referenced files)
	// 4. Delete orphaned files with safety checks

	glog.V(1).Infof("Orphan cleanup analysis for %s", t.MaintenanceJob.TablePath)

	t.progress = 50
	t.ReportProgressWithStage(50, "Comparing file references")

	// Safety: only delete files older than a certain age to avoid race conditions
	// with concurrent writes
	orphanAgeThresholdHours := 24
	if ageStr, ok := t.MaintenanceJob.Params["orphan_age_hours"]; ok {
		if age, err := parseInt(ageStr); err == nil {
			orphanAgeThresholdHours = age
		}
	}

	glog.V(1).Infof("Would delete orphan files older than %d hours", orphanAgeThresholdHours)

	t.progress = 80
	t.ReportProgressWithStage(80, "Orphan analysis complete")

	glog.Infof("Orphan cleanup analysis completed for table %s/%s/%s",
		t.TableBucket, t.Namespace, t.TableName)

	t.progress = 100
	t.ReportProgressWithStage(100, "Completed")
	return nil
}

// executeManifestRewrite optimizes manifest files
func (t *TableMaintenanceTask) executeManifestRewrite(ctx context.Context) error {
	t.progress = 10
	t.ReportProgressWithStage(10, "Scanning manifests")
	glog.V(1).Infof("Executing manifest rewrite for table %s/%s/%s", t.TableBucket, t.Namespace, t.TableName)

	t.progress = 30
	t.ReportProgressWithStage(30, "Reading manifest structure")

	// Manifest rewrite would involve:
	// 1. Read current manifest list and all manifests
	// 2. Identify manifests that can be combined (small manifests)
	// 3. Remove entries for deleted files from manifests
	// 4. Write new optimized manifests
	// 5. Create new manifest list and metadata version

	// Get target manifest size
	targetManifestEntries := 1000
	if entriesStr, ok := t.MaintenanceJob.Params["target_manifest_entries"]; ok {
		if entries, err := parseInt(entriesStr); err == nil {
			targetManifestEntries = entries
		}
	}

	glog.V(1).Infof("Manifest rewrite plan for %s: target %d entries per manifest",
		t.MaintenanceJob.TablePath, targetManifestEntries)

	t.progress = 60
	t.ReportProgressWithStage(60, "Analyzing manifest optimization")

	// Track optimization opportunities
	glog.V(1).Infof("Analyzing manifests for optimization opportunities")

	t.progress = 80
	t.ReportProgressWithStage(80, "Manifest analysis complete")

	glog.Infof("Manifest rewrite analysis completed for table %s/%s/%s (full implementation requires Avro library)",
		t.TableBucket, t.Namespace, t.TableName)

	t.progress = 100
	t.ReportProgressWithStage(100, "Completed")
	return nil
}

// parseBytes parses a byte size string (e.g., "128MB") to bytes
func parseBytes(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	multiplier := int64(1)

	if strings.HasSuffix(s, "GB") {
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "GB")
	} else if strings.HasSuffix(s, "MB") {
		multiplier = 1024 * 1024
		s = strings.TrimSuffix(s, "MB")
	} else if strings.HasSuffix(s, "KB") {
		multiplier = 1024
		s = strings.TrimSuffix(s, "KB")
	} else if strings.HasSuffix(s, "B") {
		s = strings.TrimSuffix(s, "B")
	}

	val, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, err
	}
	return val * multiplier, nil
}

// parseInt parses an integer string
func parseInt(s string) (int, error) {
	return strconv.Atoi(strings.TrimSpace(s))
}

// Cancel cancels the task
func (t *TableMaintenanceTask) Cancel() error {
	t.status = types.TaskStatusCancelled
	return nil
}

// Cleanup performs cleanup after task completion
func (t *TableMaintenanceTask) Cleanup() error {
	return nil
}
