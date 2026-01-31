package table_maintenance

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Integration tests for table maintenance
// These tests verify the complete workflow of table maintenance operations

func TestTableMaintenanceWorkflow_Compaction(t *testing.T) {
	// Test the complete compaction workflow
	job := &TableMaintenanceJob{
		JobType:     JobTypeCompaction,
		TableBucket: "test-bucket",
		Namespace:   "test-namespace",
		TableName:   "test-table",
		TablePath:   "/table-buckets/test-bucket/test-namespace/test-table",
		Priority:    types.TaskPriorityNormal,
		Reason:      "Table has many small files",
		CreatedAt:   time.Now(),
		Params: map[string]string{
			"target_file_size": "128MB",
		},
	}

	task := NewTableMaintenanceTask("compaction-test-1", "test-bucket", "test-namespace", "test-table", job)

	// Verify initial state
	if task.GetStatus() != types.TaskStatusPending {
		t.Errorf("Expected pending status, got %v", task.GetStatus())
	}

	// Execute the task
	ctx := context.Background()
	params := &worker_pb.TaskParams{
		Collection: "test-bucket",
		Sources: []*worker_pb.TaskSource{
			{Node: "/table-buckets/test-bucket/test-namespace/test-table"},
		},
	}

	err := task.Execute(ctx, params)
	if err != nil {
		t.Errorf("Unexpected error during compaction: %v", err)
	}

	// Verify completion
	if task.GetStatus() != types.TaskStatusCompleted {
		t.Errorf("Expected completed status, got %v", task.GetStatus())
	}

	if task.GetProgress() != 100 {
		t.Errorf("Expected progress 100, got %v", task.GetProgress())
	}
}

func TestTableMaintenanceWorkflow_SnapshotExpiration(t *testing.T) {
	job := &TableMaintenanceJob{
		JobType:     JobTypeSnapshotExpiration,
		TableBucket: "test-bucket",
		Namespace:   "test-namespace",
		TableName:   "test-table",
		TablePath:   "/table-buckets/test-bucket/test-namespace/test-table",
		Priority:    types.TaskPriorityLow,
		Reason:      "Table has expired snapshots",
		CreatedAt:   time.Now(),
		Params: map[string]string{
			"retention_days": "7",
		},
	}

	task := NewTableMaintenanceTask("snapshot-exp-test-1", "test-bucket", "test-namespace", "test-table", job)

	ctx := context.Background()
	params := &worker_pb.TaskParams{
		Collection: "test-bucket",
	}

	err := task.Execute(ctx, params)
	if err != nil {
		t.Errorf("Unexpected error during snapshot expiration: %v", err)
	}

	if task.GetStatus() != types.TaskStatusCompleted {
		t.Errorf("Expected completed status, got %v", task.GetStatus())
	}
}

func TestTableMaintenanceWorkflow_OrphanCleanup(t *testing.T) {
	job := &TableMaintenanceJob{
		JobType:     JobTypeOrphanCleanup,
		TableBucket: "test-bucket",
		Namespace:   "test-namespace",
		TableName:   "test-table",
		TablePath:   "/table-buckets/test-bucket/test-namespace/test-table",
		Priority:    types.TaskPriorityLow,
		Reason:      "Table has orphan files",
		CreatedAt:   time.Now(),
		Params: map[string]string{
			"orphan_age_hours": "24",
		},
	}

	task := NewTableMaintenanceTask("orphan-cleanup-test-1", "test-bucket", "test-namespace", "test-table", job)

	ctx := context.Background()
	params := &worker_pb.TaskParams{
		Collection: "test-bucket",
	}

	err := task.Execute(ctx, params)
	if err != nil {
		t.Errorf("Unexpected error during orphan cleanup: %v", err)
	}

	if task.GetStatus() != types.TaskStatusCompleted {
		t.Errorf("Expected completed status, got %v", task.GetStatus())
	}
}

func TestTableMaintenanceWorkflow_ManifestRewrite(t *testing.T) {
	job := &TableMaintenanceJob{
		JobType:     JobTypeManifestRewrite,
		TableBucket: "test-bucket",
		Namespace:   "test-namespace",
		TableName:   "test-table",
		TablePath:   "/table-buckets/test-bucket/test-namespace/test-table",
		Priority:    types.TaskPriorityLow,
		Reason:      "Table has fragmented manifests",
		CreatedAt:   time.Now(),
		Params: map[string]string{
			"target_manifest_entries": "1000",
		},
	}

	task := NewTableMaintenanceTask("manifest-rewrite-test-1", "test-bucket", "test-namespace", "test-table", job)

	ctx := context.Background()
	params := &worker_pb.TaskParams{
		Collection: "test-bucket",
	}

	err := task.Execute(ctx, params)
	if err != nil {
		t.Errorf("Unexpected error during manifest rewrite: %v", err)
	}

	if task.GetStatus() != types.TaskStatusCompleted {
		t.Errorf("Expected completed status, got %v", task.GetStatus())
	}
}

func TestTableMaintenanceWorkflow_CancellationDuringExecution(t *testing.T) {
	job := &TableMaintenanceJob{
		JobType:     JobTypeCompaction,
		TableBucket: "test-bucket",
		Namespace:   "test-namespace",
		TableName:   "test-table",
		TablePath:   "/table-buckets/test-bucket/test-namespace/test-table",
		Priority:    types.TaskPriorityNormal,
		CreatedAt:   time.Now(),
	}

	task := NewTableMaintenanceTask("cancel-test-1", "test-bucket", "test-namespace", "test-table", job)

	// Cancel before execution
	err := task.Cancel()
	if err != nil {
		t.Errorf("Unexpected error during cancellation: %v", err)
	}

	if task.GetStatus() != types.TaskStatusCancelled {
		t.Errorf("Expected cancelled status, got %v", task.GetStatus())
	}

	if !task.IsCancellable() {
		t.Error("Task should be cancellable")
	}
}

func TestTableMaintenanceWorkflow_UnknownJobType(t *testing.T) {
	job := &TableMaintenanceJob{
		JobType:     TableMaintenanceJobType("unknown_type"),
		TableBucket: "test-bucket",
		Namespace:   "test-namespace",
		TableName:   "test-table",
		CreatedAt:   time.Now(),
	}

	task := NewTableMaintenanceTask("unknown-test-1", "test-bucket", "test-namespace", "test-table", job)

	ctx := context.Background()
	params := &worker_pb.TaskParams{}

	err := task.Execute(ctx, params)
	if err == nil {
		t.Error("Expected error for unknown job type")
	}

	if task.GetStatus() != types.TaskStatusFailed {
		t.Errorf("Expected failed status, got %v", task.GetStatus())
	}
}

func TestDetectionToTaskWorkflow(t *testing.T) {
	// Test the full detection to task creation workflow
	config := NewDefaultConfig()
	scanner := NewTableMaintenanceScanner(config)

	// Simulate table info that needs maintenance
	tables := []TableInfo{
		{
			Namespace:        "default",
			TableName:        "large_table",
			TablePath:        "/table-buckets/bucket1/default/large_table",
			DataFileCount:    150, // Above threshold
			SnapshotCount:    10,
			OldestSnapshot:   time.Now().AddDate(0, 0, -30), // Old snapshots
			TotalSizeBytes:   1024 * 1024 * 1024,            // 1GB
			DeletedFileCount: 5,
		},
		{
			Namespace:        "default",
			TableName:        "small_table",
			TablePath:        "/table-buckets/bucket1/default/small_table",
			DataFileCount:    10, // Below threshold
			SnapshotCount:    2,
			OldestSnapshot:   time.Now().AddDate(0, 0, -3), // Recent
			TotalSizeBytes:   1024 * 1024,                  // 1MB
			DeletedFileCount: 0,
		},
	}

	jobs, err := scanner.ScanTableBucket("bucket1", tables)
	if err != nil {
		t.Fatalf("Unexpected error scanning table bucket: %v", err)
	}

	// Should find maintenance jobs for large_table only
	if len(jobs) == 0 {
		t.Error("Expected to find maintenance jobs for large_table")
	}

	// Verify job types
	hasCompaction := false
	hasSnapshotExpiration := false
	hasOrphanCleanup := false

	for _, job := range jobs {
		switch job.JobType {
		case JobTypeCompaction:
			hasCompaction = true
			if job.TableName != "large_table" {
				t.Errorf("Expected compaction job for large_table, got %s", job.TableName)
			}
		case JobTypeSnapshotExpiration:
			hasSnapshotExpiration = true
		case JobTypeOrphanCleanup:
			hasOrphanCleanup = true
		}
	}

	if !hasCompaction {
		t.Error("Expected compaction job for table with many files")
	}

	if !hasSnapshotExpiration {
		t.Error("Expected snapshot expiration job for table with old snapshots")
	}

	if !hasOrphanCleanup {
		t.Error("Expected orphan cleanup job for table with deleted files")
	}
}

func TestSchedulingWithConcurrencyLimits(t *testing.T) {
	config := NewDefaultConfig()
	config.MaxConcurrent = 2

	// Create running tasks at limit
	runningTasks := []*types.TaskInput{
		{Type: types.TaskTypeTableMaintenance},
		{Type: types.TaskTypeTableMaintenance},
	}

	// Create available workers
	workers := []*types.WorkerData{
		{
			ID:            "worker1",
			CurrentLoad:   0,
			MaxConcurrent: 5,
			Capabilities:  []types.TaskType{types.TaskTypeTableMaintenance},
		},
	}

	// New task to schedule
	newTask := &types.TaskInput{
		Type: types.TaskTypeTableMaintenance,
	}

	// Should not schedule when at max concurrent
	result := Scheduling(newTask, runningTasks, workers, config)
	if result {
		t.Error("Should not schedule when at max concurrent limit")
	}

	// Reduce running tasks
	runningTasks = []*types.TaskInput{
		{Type: types.TaskTypeTableMaintenance},
	}

	// Should now schedule
	result = Scheduling(newTask, runningTasks, workers, config)
	if !result {
		t.Error("Should schedule when under max concurrent limit")
	}
}

func TestSchedulingWithNoCapableWorkers(t *testing.T) {
	config := NewDefaultConfig()

	runningTasks := []*types.TaskInput{}

	// Workers without table maintenance capability
	workers := []*types.WorkerData{
		{
			ID:            "worker1",
			CurrentLoad:   0,
			MaxConcurrent: 5,
			Capabilities:  []types.TaskType{types.TaskTypeVacuum}, // Only vacuum
		},
	}

	newTask := &types.TaskInput{
		Type: types.TaskTypeTableMaintenance,
	}

	result := Scheduling(newTask, runningTasks, workers, config)
	if result {
		t.Error("Should not schedule when no workers have required capability")
	}
}

func TestSchedulingWithOverloadedWorkers(t *testing.T) {
	config := NewDefaultConfig()

	runningTasks := []*types.TaskInput{}

	// Worker at capacity
	workers := []*types.WorkerData{
		{
			ID:            "worker1",
			CurrentLoad:   5, // At max
			MaxConcurrent: 5,
			Capabilities:  []types.TaskType{types.TaskTypeTableMaintenance},
		},
	}

	newTask := &types.TaskInput{
		Type: types.TaskTypeTableMaintenance,
	}

	result := Scheduling(newTask, runningTasks, workers, config)
	if result {
		t.Error("Should not schedule when all workers are at capacity")
	}
}

func TestConfigPersistence(t *testing.T) {
	// Test config to policy conversion
	config := NewDefaultConfig()
	config.ScanIntervalMinutes = 60
	config.CompactionFileThreshold = 200
	config.SnapshotRetentionDays = 14
	config.MaxConcurrent = 4

	policy := config.ToTaskPolicy()

	if policy == nil {
		t.Fatal("ToTaskPolicy returned nil")
	}

	if !policy.Enabled {
		t.Error("Expected enabled policy")
	}

	if policy.MaxConcurrent != 4 {
		t.Errorf("Expected MaxConcurrent 4, got %d", policy.MaxConcurrent)
	}

	// Test round-trip
	newConfig := NewDefaultConfig()
	err := newConfig.FromTaskPolicy(policy)
	if err != nil {
		t.Fatalf("FromTaskPolicy failed: %v", err)
	}

	if newConfig.MaxConcurrent != 4 {
		t.Errorf("Expected MaxConcurrent 4 after round-trip, got %d", newConfig.MaxConcurrent)
	}
}

func TestIcebergOps_GetExpiredSnapshots(t *testing.T) {
	mc := &TableMaintenanceContext{
		Config: NewDefaultConfig(),
	}

	now := time.Now()
	metadata := &IcebergTableMetadata{
		FormatVersion: 2,
		CurrentSnap:   3,
		Snapshots: []IcebergSnapshot{
			{SnapshotID: 1, TimestampMs: now.AddDate(0, 0, -30).UnixMilli()}, // Old, not current
			{SnapshotID: 2, TimestampMs: now.AddDate(0, 0, -10).UnixMilli()}, // Old, not current
			{SnapshotID: 3, TimestampMs: now.AddDate(0, 0, -1).UnixMilli()},  // Current snapshot
		},
		Refs: map[string]SnapRef{
			"main": {SnapshotID: 3, Type: "branch"},
		},
	}

	expired := mc.GetExpiredSnapshots(metadata, 7)

	// Should find snapshots 1 and 2 as expired (older than 7 days, not current, not referenced)
	if len(expired) != 2 {
		t.Errorf("Expected 2 expired snapshots, got %d", len(expired))
	}

	// Current snapshot should never be expired
	for _, s := range expired {
		if s.SnapshotID == 3 {
			t.Error("Current snapshot should never be expired")
		}
	}
}

func TestParseBytes(t *testing.T) {
	testCases := []struct {
		input    string
		expected int64
	}{
		{"128MB", 128 * 1024 * 1024},
		{"1GB", 1024 * 1024 * 1024},
		{"512KB", 512 * 1024},
		{"1024B", 1024},
		{"1024", 1024},
		{" 256 MB ", 256 * 1024 * 1024},
	}

	for _, tc := range testCases {
		result, err := parseBytes(tc.input)
		if err != nil {
			t.Errorf("parseBytes(%q) failed: %v", tc.input, err)
			continue
		}
		if result != tc.expected {
			t.Errorf("parseBytes(%q) = %d, expected %d", tc.input, result, tc.expected)
		}
	}
}

func TestParseInt(t *testing.T) {
	testCases := []struct {
		input    string
		expected int
	}{
		{"42", 42},
		{" 100 ", 100},
		{"0", 0},
		{"-5", -5},
	}

	for _, tc := range testCases {
		result, err := parseInt(tc.input)
		if err != nil {
			t.Errorf("parseInt(%q) failed: %v", tc.input, err)
			continue
		}
		if result != tc.expected {
			t.Errorf("parseInt(%q) = %d, expected %d", tc.input, result, tc.expected)
		}
	}
}
