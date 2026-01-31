package table_maintenance

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

func TestNewTableMaintenanceTask(t *testing.T) {
	job := &TableMaintenanceJob{
		JobType:     JobTypeCompaction,
		TableBucket: "test-bucket",
		Namespace:   "test-namespace",
		TableName:   "test-table",
		TablePath:   "/table-buckets/test-bucket/test-namespace/test-table",
		Priority:    types.TaskPriorityNormal,
		Reason:      "Test job",
		CreatedAt:   time.Now(),
	}

	task := NewTableMaintenanceTask("test-id", "test-bucket", "test-namespace", "test-table", job)

	if task == nil {
		t.Fatal("Expected non-nil task")
	}
	if task.ID() != "test-id" {
		t.Errorf("Expected ID 'test-id', got '%s'", task.ID())
	}
	if task.Type() != types.TaskTypeTableMaintenance {
		t.Errorf("Expected type TableMaintenance, got %v", task.Type())
	}
	if task.TableBucket != "test-bucket" {
		t.Errorf("Expected bucket 'test-bucket', got '%s'", task.TableBucket)
	}
	if task.GetStatus() != types.TaskStatusPending {
		t.Errorf("Expected status Pending, got %v", task.GetStatus())
	}
}

func TestTableMaintenanceTask_Validate(t *testing.T) {
	job := &TableMaintenanceJob{
		JobType:     JobTypeCompaction,
		TableBucket: "test-bucket",
	}
	task := NewTableMaintenanceTask("test-id", "test-bucket", "ns", "table", job)

	// Test nil params
	err := task.Validate(nil)
	if err == nil {
		t.Error("Expected error for nil params")
	}

	// Test valid params
	params := &worker_pb.TaskParams{
		Collection: "test-bucket",
	}
	err = task.Validate(params)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestTableMaintenanceTask_EstimateTime(t *testing.T) {
	testCases := []struct {
		jobType  TableMaintenanceJobType
		expected time.Duration
	}{
		{JobTypeCompaction, 5 * time.Minute},
		{JobTypeSnapshotExpiration, 1 * time.Minute},
		{JobTypeOrphanCleanup, 3 * time.Minute},
		{JobTypeManifestRewrite, 2 * time.Minute},
	}

	for _, tc := range testCases {
		job := &TableMaintenanceJob{JobType: tc.jobType}
		task := NewTableMaintenanceTask("test", "bucket", "ns", "table", job)

		estimate := task.EstimateTime(nil)
		if estimate != tc.expected {
			t.Errorf("For job type %s: expected %v, got %v", tc.jobType, tc.expected, estimate)
		}
	}
}

func TestTableMaintenanceTask_Execute(t *testing.T) {
	job := &TableMaintenanceJob{
		JobType:     JobTypeCompaction,
		TableBucket: "test-bucket",
		Namespace:   "test-namespace",
		TableName:   "test-table",
	}
	task := NewTableMaintenanceTask("test-id", "test-bucket", "test-namespace", "test-table", job)

	ctx := context.Background()
	params := &worker_pb.TaskParams{}

	err := task.Execute(ctx, params)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if task.GetStatus() != types.TaskStatusCompleted {
		t.Errorf("Expected status Completed, got %v", task.GetStatus())
	}
	if task.GetProgress() != 100 {
		t.Errorf("Expected progress 100, got %v", task.GetProgress())
	}
}

func TestTableMaintenanceTask_Cancel(t *testing.T) {
	job := &TableMaintenanceJob{JobType: JobTypeCompaction}
	task := NewTableMaintenanceTask("test", "bucket", "ns", "table", job)

	err := task.Cancel()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if task.GetStatus() != types.TaskStatusCancelled {
		t.Errorf("Expected status Cancelled, got %v", task.GetStatus())
	}
}

func TestNewDefaultConfig(t *testing.T) {
	config := NewDefaultConfig()

	if config == nil {
		t.Fatal("Expected non-nil config")
	}
	if !config.Enabled {
		t.Error("Expected enabled by default")
	}
	if config.ScanIntervalMinutes != 30 {
		t.Errorf("Expected 30 minutes, got %d", config.ScanIntervalMinutes)
	}
	if config.CompactionFileThreshold != 100 {
		t.Errorf("Expected 100 files, got %d", config.CompactionFileThreshold)
	}
	if config.SnapshotRetentionDays != 7 {
		t.Errorf("Expected 7 days, got %d", config.SnapshotRetentionDays)
	}
}

func TestTableMaintenanceScanner_NeedsCompaction(t *testing.T) {
	config := NewDefaultConfig()
	scanner := NewTableMaintenanceScanner(config)

	// Table with few files - no compaction needed
	table1 := TableInfo{
		DataFileCount: 50,
	}
	if scanner.needsCompaction(table1) {
		t.Error("Should not need compaction with only 50 files")
	}

	// Table with many files - needs compaction
	table2 := TableInfo{
		DataFileCount: 150,
	}
	if !scanner.needsCompaction(table2) {
		t.Error("Should need compaction with 150 files")
	}
}

func TestTableMaintenanceScanner_NeedsSnapshotExpiration(t *testing.T) {
	config := NewDefaultConfig()
	scanner := NewTableMaintenanceScanner(config)

	// Single snapshot - no expiration needed
	table1 := TableInfo{
		SnapshotCount:  1,
		OldestSnapshot: time.Now().AddDate(0, 0, -30),
	}
	if scanner.needsSnapshotExpiration(table1) {
		t.Error("Should not expire the only snapshot")
	}

	// Recent snapshots - no expiration needed
	table2 := TableInfo{
		SnapshotCount:  5,
		OldestSnapshot: time.Now().AddDate(0, 0, -3),
	}
	if scanner.needsSnapshotExpiration(table2) {
		t.Error("Should not expire recent snapshots")
	}

	// Old snapshots - expiration needed
	table3 := TableInfo{
		SnapshotCount:  5,
		OldestSnapshot: time.Now().AddDate(0, 0, -30),
	}
	if !scanner.needsSnapshotExpiration(table3) {
		t.Error("Should expire old snapshots")
	}
}

func TestScheduling(t *testing.T) {
	config := NewDefaultConfig()

	// Test with available workers
	task := &types.TaskInput{
		Type: types.TaskTypeTableMaintenance,
	}
	runningTasks := []*types.TaskInput{}
	workers := []*types.WorkerData{
		{
			ID:           "worker1",
			CurrentLoad:  0,
			MaxConcurrent: 5,
			Capabilities: []types.TaskType{types.TaskTypeTableMaintenance},
		},
	}

	result := Scheduling(task, runningTasks, workers, config)
	if !result {
		t.Error("Should schedule when workers are available")
	}

	// Test at max concurrent
	runningTasks = []*types.TaskInput{
		{Type: types.TaskTypeTableMaintenance},
		{Type: types.TaskTypeTableMaintenance},
	}
	result = Scheduling(task, runningTasks, workers, config)
	if result {
		t.Error("Should not schedule when at max concurrent")
	}
}

func TestGetConfigSpec(t *testing.T) {
	spec := GetConfigSpec()

	if len(spec.Fields) == 0 {
		t.Error("Expected non-empty config spec")
	}

	// Check for expected fields
	fieldNames := make(map[string]bool)
	for _, field := range spec.Fields {
		fieldNames[field.Name] = true
	}

	expectedFields := []string{"enabled", "scan_interval_minutes", "compaction_file_threshold", "snapshot_retention_days", "max_concurrent"}
	for _, name := range expectedFields {
		if !fieldNames[name] {
			t.Errorf("Missing expected field: %s", name)
		}
	}
}
