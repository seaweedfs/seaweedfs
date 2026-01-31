package table_maintenance

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// TableMaintenanceDetector implements types.TaskDetector for table maintenance
type TableMaintenanceDetector struct {
	config *Config
}

// NewTableMaintenanceDetector creates a new table maintenance detector
func NewTableMaintenanceDetector(config *Config) *TableMaintenanceDetector {
	return &TableMaintenanceDetector{
		config: config,
	}
}

// ScanInterval returns how often to scan for table maintenance needs
func (d *TableMaintenanceDetector) ScanInterval() time.Duration {
	if d.config != nil && d.config.ScanIntervalMinutes > 0 {
		return time.Duration(d.config.ScanIntervalMinutes) * time.Minute
	}
	return 30 * time.Minute // Default: scan every 30 minutes
}

// DetectTasks scans for tables that need maintenance
func (d *TableMaintenanceDetector) DetectTasks(metrics []*types.VolumeHealthMetrics) ([]*types.TaskDetectionResult, error) {
	glog.V(2).Infof("Table maintenance detection starting")

	// Table maintenance doesn't use volume metrics - it scans table buckets
	// The actual scanning is done by the admin server's table scanner
	// Workers pick up jobs from the admin server's queue

	// This detector returns empty results because table maintenance jobs
	// are created by the admin server's table scanner, not by volume metrics
	return nil, nil
}

// Detection is the function signature required by the task registration system
func Detection(metrics []*types.VolumeHealthMetrics, clusterInfo *types.ClusterInfo, config base.TaskConfig) ([]*types.TaskDetectionResult, error) {
	if !config.IsEnabled() {
		return nil, nil
	}

	tableConfig, ok := config.(*Config)
	if !ok {
		tableConfig = NewDefaultConfig()
	}

	detector := NewTableMaintenanceDetector(tableConfig)
	return detector.DetectTasks(metrics)
}

// TableMaintenanceScanner scans table buckets for maintenance needs
// This is called by the admin server to populate the maintenance queue
type TableMaintenanceScanner struct {
	config *Config
}

// NewTableMaintenanceScanner creates a new table maintenance scanner
func NewTableMaintenanceScanner(config *Config) *TableMaintenanceScanner {
	return &TableMaintenanceScanner{
		config: config,
	}
}

// ScanTableBucket scans a table bucket for tables needing maintenance
// Returns a list of maintenance jobs that should be queued
func (s *TableMaintenanceScanner) ScanTableBucket(bucketName string, tables []TableInfo) ([]*TableMaintenanceJob, error) {
	glog.V(1).Infof("Scanning table bucket %s for maintenance needs", bucketName)

	var jobs []*TableMaintenanceJob

	for _, table := range tables {
		// Check each table for maintenance needs
		tableJobs := s.checkTableMaintenanceNeeds(bucketName, table)
		jobs = append(jobs, tableJobs...)
	}

	glog.V(1).Infof("Found %d maintenance jobs for table bucket %s", len(jobs), bucketName)
	return jobs, nil
}

// TableInfo represents basic table information for scanning
type TableInfo struct {
	Namespace        string
	TableName        string
	TablePath        string
	LastCompaction   time.Time
	DataFileCount    int
	SnapshotCount    int
	OldestSnapshot   time.Time
	TotalSizeBytes   int64
	DeletedFileCount int
}

// checkTableMaintenanceNeeds checks if a table needs maintenance
func (s *TableMaintenanceScanner) checkTableMaintenanceNeeds(bucketName string, table TableInfo) []*TableMaintenanceJob {
	var jobs []*TableMaintenanceJob
	now := time.Now()

	// Check for compaction needs
	if s.needsCompaction(table) {
		jobs = append(jobs, &TableMaintenanceJob{
			JobType:     JobTypeCompaction,
			TableBucket: bucketName,
			Namespace:   table.Namespace,
			TableName:   table.TableName,
			TablePath:   table.TablePath,
			Priority:    types.TaskPriorityNormal,
			Reason:      "Table has many small files that can be compacted",
			CreatedAt:   now,
		})
	}

	// Check for snapshot expiration needs
	if s.needsSnapshotExpiration(table) {
		jobs = append(jobs, &TableMaintenanceJob{
			JobType:     JobTypeSnapshotExpiration,
			TableBucket: bucketName,
			Namespace:   table.Namespace,
			TableName:   table.TableName,
			TablePath:   table.TablePath,
			Priority:    types.TaskPriorityLow,
			Reason:      "Table has expired snapshots that can be removed",
			CreatedAt:   now,
		})
	}

	// Check for orphan cleanup needs
	if s.needsOrphanCleanup(table) {
		jobs = append(jobs, &TableMaintenanceJob{
			JobType:     JobTypeOrphanCleanup,
			TableBucket: bucketName,
			Namespace:   table.Namespace,
			TableName:   table.TableName,
			TablePath:   table.TablePath,
			Priority:    types.TaskPriorityLow,
			Reason:      "Table has orphaned files that can be removed",
			CreatedAt:   now,
		})
	}

	return jobs
}

// needsCompaction checks if a table needs compaction
func (s *TableMaintenanceScanner) needsCompaction(table TableInfo) bool {
	// Use config value directly - config is always set by NewTableMaintenanceScanner
	return table.DataFileCount > s.config.CompactionFileThreshold
}

// needsSnapshotExpiration checks if a table has expired snapshots
func (s *TableMaintenanceScanner) needsSnapshotExpiration(table TableInfo) bool {
	if table.SnapshotCount <= 1 {
		return false // Keep at least one snapshot
	}

	// Use config value directly - config is always set by NewTableMaintenanceScanner
	cutoff := time.Now().AddDate(0, 0, -s.config.SnapshotRetentionDays)
	return table.OldestSnapshot.Before(cutoff)
}

// needsOrphanCleanup checks if a table might have orphaned files
func (s *TableMaintenanceScanner) needsOrphanCleanup(table TableInfo) bool {
	// Tables with deleted files might have orphans
	return table.DeletedFileCount > 0
}

// CreateTaskParams creates task parameters for a maintenance job
func CreateTaskParams(job *TableMaintenanceJob) *worker_pb.TaskParams {
	return &worker_pb.TaskParams{
		Sources: []*worker_pb.TaskSource{
			{
				Node: job.TablePath,
			},
		},
		VolumeId:   0, // Not volume-specific
		Collection: job.TableBucket,
	}
}
