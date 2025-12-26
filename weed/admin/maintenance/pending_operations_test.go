package maintenance

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

func TestPendingOperations_ConflictDetection(t *testing.T) {
	pendingOps := NewPendingOperations()

	// Add a pending erasure coding operation on volume 123
	op := &PendingOperation{
		VolumeID:      123,
		OperationType: OpTypeErasureCoding,
		SourceNode:    "node1",
		TaskID:        "task-001",
		StartTime:     time.Now(),
		EstimatedSize: 1024 * 1024 * 1024, // 1GB
		Collection:    "test",
		Status:        "assigned",
	}

	pendingOps.AddOperation(op)

	// Test conflict detection
	if !pendingOps.HasPendingOperationOnVolume(123) {
		t.Errorf("Expected volume 123 to have pending operation")
	}

	if !pendingOps.WouldConflictWithPending(123, OpTypeVacuum) {
		t.Errorf("Expected conflict when trying to add vacuum operation on volume 123")
	}

	if pendingOps.HasPendingOperationOnVolume(124) {
		t.Errorf("Expected volume 124 to have no pending operation")
	}

	if pendingOps.WouldConflictWithPending(124, OpTypeVacuum) {
		t.Errorf("Expected no conflict for volume 124")
	}
}

func TestPendingOperations_CapacityProjection(t *testing.T) {
	pendingOps := NewPendingOperations()

	// Add operation moving volume from node1 to node2
	op1 := &PendingOperation{
		VolumeID:      100,
		OperationType: OpTypeVolumeMove,
		SourceNode:    "node1",
		DestNode:      "node2",
		TaskID:        "task-001",
		StartTime:     time.Now(),
		EstimatedSize: 2 * 1024 * 1024 * 1024, // 2GB
		Collection:    "test",
		Status:        "in_progress",
	}

	// Add operation moving volume from node3 to node1
	op2 := &PendingOperation{
		VolumeID:      101,
		OperationType: OpTypeVolumeMove,
		SourceNode:    "node3",
		DestNode:      "node1",
		TaskID:        "task-002",
		StartTime:     time.Now(),
		EstimatedSize: 1 * 1024 * 1024 * 1024, // 1GB
		Collection:    "test",
		Status:        "assigned",
	}

	pendingOps.AddOperation(op1)
	pendingOps.AddOperation(op2)

	// Test capacity impact for node1
	incoming, outgoing := pendingOps.GetPendingCapacityImpactForNode("node1")
	expectedIncoming := uint64(1 * 1024 * 1024 * 1024) // 1GB incoming
	expectedOutgoing := uint64(2 * 1024 * 1024 * 1024) // 2GB outgoing

	if incoming != expectedIncoming {
		t.Errorf("Expected incoming capacity %d, got %d", expectedIncoming, incoming)
	}

	if outgoing != expectedOutgoing {
		t.Errorf("Expected outgoing capacity %d, got %d", expectedOutgoing, outgoing)
	}

	// Test projection for node1
	currentUsed := uint64(10 * 1024 * 1024 * 1024)   // 10GB current
	totalCapacity := uint64(50 * 1024 * 1024 * 1024) // 50GB total

	projection := pendingOps.GetNodeCapacityProjection("node1", currentUsed, totalCapacity)

	expectedProjectedUsed := currentUsed + incoming - outgoing     // 10 + 1 - 2 = 9GB
	expectedProjectedFree := totalCapacity - expectedProjectedUsed // 50 - 9 = 41GB

	if projection.ProjectedUsed != expectedProjectedUsed {
		t.Errorf("Expected projected used %d, got %d", expectedProjectedUsed, projection.ProjectedUsed)
	}

	if projection.ProjectedFree != expectedProjectedFree {
		t.Errorf("Expected projected free %d, got %d", expectedProjectedFree, projection.ProjectedFree)
	}
}

func TestPendingOperations_VolumeFiltering(t *testing.T) {
	pendingOps := NewPendingOperations()

	// Create volume metrics
	metrics := []*types.VolumeHealthMetrics{
		{VolumeID: 100, Server: "node1", ServerAddress: "192.168.1.1:8080"},
		{VolumeID: 101, Server: "node2", ServerAddress: "192.168.1.2:8080"},
		{VolumeID: 102, Server: "node3", ServerAddress: "192.168.1.3:8080"},
		{VolumeID: 103, Server: "node1", ServerAddress: "192.168.1.1:8080"},
	}

	// Add pending operations on volumes 101 and 103
	op1 := &PendingOperation{
		VolumeID:      101,
		OperationType: OpTypeVacuum,
		SourceNode:    "node2",
		TaskID:        "task-001",
		StartTime:     time.Now(),
		EstimatedSize: 1024 * 1024 * 1024,
		Status:        "in_progress",
	}

	op2 := &PendingOperation{
		VolumeID:      103,
		OperationType: OpTypeErasureCoding,
		SourceNode:    "node1",
		TaskID:        "task-002",
		StartTime:     time.Now(),
		EstimatedSize: 2 * 1024 * 1024 * 1024,
		Status:        "assigned",
	}

	pendingOps.AddOperation(op1)
	pendingOps.AddOperation(op2)

	// Filter metrics
	filtered := pendingOps.FilterVolumeMetricsExcludingPending(metrics)

	// Should only have volumes 100 and 102 (101 and 103 are filtered out)
	if len(filtered) != 2 {
		t.Errorf("Expected 2 filtered metrics, got %d", len(filtered))
	}

	// Check that correct volumes remain
	foundVolumes := make(map[uint32]bool)
	for _, metric := range filtered {
		foundVolumes[metric.VolumeID] = true
	}

	if !foundVolumes[100] || !foundVolumes[102] {
		t.Errorf("Expected volumes 100 and 102 to remain after filtering")
	}

	if foundVolumes[101] || foundVolumes[103] {
		t.Errorf("Expected volumes 101 and 103 to be filtered out")
	}
}

func TestPendingOperations_OperationLifecycle(t *testing.T) {
	pendingOps := NewPendingOperations()

	// Add operation
	op := &PendingOperation{
		VolumeID:      200,
		OperationType: OpTypeVolumeBalance,
		SourceNode:    "node1",
		DestNode:      "node2",
		TaskID:        "task-balance-001",
		StartTime:     time.Now(),
		EstimatedSize: 1024 * 1024 * 1024,
		Status:        "assigned",
	}

	pendingOps.AddOperation(op)

	// Check it exists
	if !pendingOps.HasPendingOperationOnVolume(200) {
		t.Errorf("Expected volume 200 to have pending operation")
	}

	// Update status
	pendingOps.UpdateOperationStatus("task-balance-001", "in_progress")

	retrievedOp := pendingOps.GetPendingOperationOnVolume(200)
	if retrievedOp == nil {
		t.Errorf("Expected to retrieve pending operation for volume 200")
	} else if retrievedOp.Status != "in_progress" {
		t.Errorf("Expected operation status to be 'in_progress', got '%s'", retrievedOp.Status)
	}

	// Complete operation
	pendingOps.RemoveOperation("task-balance-001")

	if pendingOps.HasPendingOperationOnVolume(200) {
		t.Errorf("Expected volume 200 to have no pending operation after removal")
	}
}

func TestPendingOperations_StaleCleanup(t *testing.T) {
	pendingOps := NewPendingOperations()

	// Add recent operation
	recentOp := &PendingOperation{
		VolumeID:      300,
		OperationType: OpTypeVacuum,
		SourceNode:    "node1",
		TaskID:        "task-recent",
		StartTime:     time.Now(),
		EstimatedSize: 1024 * 1024 * 1024,
		Status:        "in_progress",
	}

	// Add stale operation (24 hours ago)
	staleOp := &PendingOperation{
		VolumeID:      301,
		OperationType: OpTypeErasureCoding,
		SourceNode:    "node2",
		TaskID:        "task-stale",
		StartTime:     time.Now().Add(-24 * time.Hour),
		EstimatedSize: 2 * 1024 * 1024 * 1024,
		Status:        "in_progress",
	}

	pendingOps.AddOperation(recentOp)
	pendingOps.AddOperation(staleOp)

	// Clean up operations older than 1 hour
	removedCount := pendingOps.CleanupStaleOperations(1 * time.Hour)

	if removedCount != 1 {
		t.Errorf("Expected to remove 1 stale operation, removed %d", removedCount)
	}

	// Recent operation should still exist
	if !pendingOps.HasPendingOperationOnVolume(300) {
		t.Errorf("Expected recent operation on volume 300 to still exist")
	}

	// Stale operation should be removed
	if pendingOps.HasPendingOperationOnVolume(301) {
		t.Errorf("Expected stale operation on volume 301 to be removed")
	}
}
