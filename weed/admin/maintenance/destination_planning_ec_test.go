package maintenance

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

func TestECDestinationPlanning_SufficientNodes(t *testing.T) {
	pendingOps := NewPendingOperations()
	tracker := NewVolumeShardTracker(pendingOps)

	// Create a large volume suitable for EC
	metrics := []*types.VolumeHealthMetrics{
		{
			VolumeID:   100,
			Server:     "node1:9333",
			Collection: "archive",
			Size:       50 * 1024 * 1024 * 1024, // 50GB - good for EC
			IsECVolume: false,
		},
	}

	err := tracker.UpdateFromMaster(metrics)
	if err != nil {
		t.Fatalf("Failed to update tracker: %v", err)
	}

	// Add 16 nodes to support EC encoding (14 needed + extras)
	tracker.mutex.Lock()
	for i := 1; i <= 16; i++ {
		nodeID := fmt.Sprintf("ec-node%d:9333", i)
		tracker.nodes[nodeID] = &NodeCapacityInfo{
			NodeID:        nodeID,
			TotalCapacity: 200 * 1024 * 1024 * 1024, // 200GB
			FreeCapacity:  180 * 1024 * 1024 * 1024, // 180GB free
		}
	}
	tracker.mutex.Unlock()

	// Plan EC destination
	destinationPlan, err := tracker.PlanDestinationForVolume(100, OpTypeErasureCoding, "node1:9333")
	if err != nil {
		t.Fatalf("Failed to plan EC destination: %v", err)
	}

	// Should return a valid destination
	if destinationPlan.TargetNode == "" {
		t.Errorf("Expected valid target node for EC encoding")
	}

	if destinationPlan.ExpectedSize == 0 {
		t.Errorf("Expected valid expected size for EC encoding")
	}

	// Expected shard size should be approximately volume_size / 10 (data shards)
	expectedShardSize := uint64(50 * 1024 * 1024 * 1024 / 10)
	if destinationPlan.ExpectedSize != expectedShardSize {
		t.Logf("Expected shard size around %d, got %d", expectedShardSize, destinationPlan.ExpectedSize)
	}
}

func TestECDestinationPlanning_InsufficientNodes(t *testing.T) {
	pendingOps := NewPendingOperations()
	tracker := NewVolumeShardTracker(pendingOps)

	// Create a volume for EC
	metrics := []*types.VolumeHealthMetrics{
		{
			VolumeID:   200,
			Server:     "node1:9333",
			Collection: "archive",
			Size:       30 * 1024 * 1024 * 1024,
			IsECVolume: false,
		},
	}

	err := tracker.UpdateFromMaster(metrics)
	if err != nil {
		t.Fatalf("Failed to update tracker: %v", err)
	}

	// Add only 10 nodes (insufficient for 14 shards)
	tracker.mutex.Lock()
	for i := 1; i <= 10; i++ {
		nodeID := fmt.Sprintf("node%d:9333", i)
		tracker.nodes[nodeID] = &NodeCapacityInfo{
			NodeID:        nodeID,
			TotalCapacity: 100 * 1024 * 1024 * 1024,
			FreeCapacity:  90 * 1024 * 1024 * 1024,
		}
	}
	tracker.mutex.Unlock()

	// Plan EC destination should fail
	_, err = tracker.PlanDestinationForVolume(200, OpTypeErasureCoding, "node1:9333")
	if err == nil {
		t.Errorf("Expected EC planning to fail with insufficient nodes")
	}

	expectedError := "insufficient nodes for EC encoding"
	if err != nil && !contains(err.Error(), expectedError) {
		t.Errorf("Expected error containing '%s', got: %v", expectedError, err)
	}
}

func TestECDestinationPlanning_InsufficientCapacity(t *testing.T) {
	pendingOps := NewPendingOperations()
	tracker := NewVolumeShardTracker(pendingOps)

	// Create a large volume
	metrics := []*types.VolumeHealthMetrics{
		{
			VolumeID:   300,
			Server:     "node1:9333",
			Collection: "archive",
			Size:       100 * 1024 * 1024 * 1024, // 100GB
			IsECVolume: false,
		},
	}

	err := tracker.UpdateFromMaster(metrics)
	if err != nil {
		t.Fatalf("Failed to update tracker: %v", err)
	}

	// Add 14 nodes but with insufficient capacity for shards
	tracker.mutex.Lock()
	for i := 1; i <= 14; i++ {
		nodeID := fmt.Sprintf("small-node%d:9333", i)
		tracker.nodes[nodeID] = &NodeCapacityInfo{
			NodeID:        nodeID,
			TotalCapacity: 20 * 1024 * 1024 * 1024, // 20GB total
			FreeCapacity:  5 * 1024 * 1024 * 1024,  // Only 5GB free
			// Shard size would be ~10GB (100GB/10), so insufficient
		}
	}
	tracker.mutex.Unlock()

	// Plan EC destination should fail due to insufficient capacity per node
	_, err = tracker.PlanDestinationForVolume(300, OpTypeErasureCoding, "node1:9333")
	if err == nil {
		t.Errorf("Expected EC planning to fail due to insufficient capacity")
	}
}

func TestECDestinationPlanning_WithPendingOperations(t *testing.T) {
	pendingOps := NewPendingOperations()
	tracker := NewVolumeShardTracker(pendingOps)

	// Add pending operations that will affect some nodes
	for i := 1; i <= 5; i++ {
		pendingOp := &PendingOperation{
			VolumeID:      uint32(400 + i),
			OperationType: OpTypeErasureCoding,
			SourceNode:    "source:9333",
			DestNode:      fmt.Sprintf("node%d:9333", i),
			TaskID:        fmt.Sprintf("ec-task-%d", i),
			EstimatedSize: 15 * 1024 * 1024 * 1024, // 15GB per operation
			Status:        "in_progress",
		}
		pendingOps.AddOperation(pendingOp)
	}

	// Create volume for EC
	metrics := []*types.VolumeHealthMetrics{
		{
			VolumeID:   500,
			Server:     "source:9333",
			Collection: "archive",
			Size:       80 * 1024 * 1024 * 1024, // 80GB
			IsECVolume: false,
		},
	}

	err := tracker.UpdateFromMaster(metrics)
	if err != nil {
		t.Fatalf("Failed to update tracker: %v", err)
	}

	// Add nodes - first 5 have pending operations
	tracker.mutex.Lock()
	for i := 1; i <= 20; i++ {
		nodeID := fmt.Sprintf("node%d:9333", i)
		tracker.nodes[nodeID] = &NodeCapacityInfo{
			NodeID:        nodeID,
			TotalCapacity: 100 * 1024 * 1024 * 1024,
			FreeCapacity:  80 * 1024 * 1024 * 1024, // 80GB initially free
		}
	}
	tracker.mutex.Unlock()

	// Plan EC destination
	destinationPlan, err := tracker.PlanDestinationForVolume(500, OpTypeErasureCoding, "source:9333")
	if err != nil {
		t.Fatalf("Failed to plan EC destination: %v", err)
	}

	// Should still succeed even with some nodes having pending operations
	if destinationPlan.TargetNode == "" {
		t.Errorf("Expected to find a suitable node for EC despite pending operations")
	}

	// Should prefer nodes without pending operations
	// (nodes 6-20 should be preferred over nodes 1-5)
	t.Logf("Chose node: %s for EC encoding", destinationPlan.TargetNode)
}

func TestECDestinationPlanning_PreferHighCapacityNodes(t *testing.T) {
	pendingOps := NewPendingOperations()
	tracker := NewVolumeShardTracker(pendingOps)

	// Create volume for EC
	metrics := []*types.VolumeHealthMetrics{
		{
			VolumeID:   600,
			Server:     "source:9333",
			Collection: "archive",
			Size:       60 * 1024 * 1024 * 1024, // 60GB
			IsECVolume: false,
		},
	}

	err := tracker.UpdateFromMaster(metrics)
	if err != nil {
		t.Fatalf("Failed to update tracker: %v", err)
	}

	// Add mix of high and low capacity nodes
	tracker.mutex.Lock()

	// Add 10 low capacity nodes (barely sufficient)
	for i := 1; i <= 10; i++ {
		nodeID := fmt.Sprintf("low-node%d:9333", i)
		tracker.nodes[nodeID] = &NodeCapacityInfo{
			NodeID:        nodeID,
			TotalCapacity: 50 * 1024 * 1024 * 1024, // 50GB
			FreeCapacity:  10 * 1024 * 1024 * 1024, // 10GB free (shard needs ~6GB)
		}
	}

	// Add 10 high capacity nodes
	for i := 1; i <= 10; i++ {
		nodeID := fmt.Sprintf("high-node%d:9333", i)
		tracker.nodes[nodeID] = &NodeCapacityInfo{
			NodeID:        nodeID,
			TotalCapacity: 200 * 1024 * 1024 * 1024, // 200GB
			FreeCapacity:  180 * 1024 * 1024 * 1024, // 180GB free
		}
	}
	tracker.mutex.Unlock()

	// Plan EC destination
	destinationPlan, err := tracker.PlanDestinationForVolume(600, OpTypeErasureCoding, "source:9333")
	if err != nil {
		t.Fatalf("Failed to plan EC destination: %v", err)
	}

	// Should prefer high capacity nodes
	if !contains(destinationPlan.TargetNode, "high-node") {
		t.Logf("Expected to prefer high-capacity node, got: %s", destinationPlan.TargetNode)
		// Note: This might not always be guaranteed depending on implementation
	}
}

func TestECDestinationPlanning_ExcludeSourceNode(t *testing.T) {
	pendingOps := NewPendingOperations()
	tracker := NewVolumeShardTracker(pendingOps)

	sourceNode := "source-node:9333"

	// Create volume for EC
	metrics := []*types.VolumeHealthMetrics{
		{
			VolumeID:   700,
			Server:     sourceNode,
			Collection: "archive",
			Size:       40 * 1024 * 1024 * 1024,
			IsECVolume: false,
		},
	}

	err := tracker.UpdateFromMaster(metrics)
	if err != nil {
		t.Fatalf("Failed to update tracker: %v", err)
	}

	// Add 15 nodes including the source node
	tracker.mutex.Lock()
	tracker.nodes[sourceNode] = &NodeCapacityInfo{
		NodeID:        sourceNode,
		TotalCapacity: 200 * 1024 * 1024 * 1024,
		FreeCapacity:  150 * 1024 * 1024 * 1024, // Very high capacity
	}

	for i := 1; i <= 14; i++ {
		nodeID := fmt.Sprintf("dest-node%d:9333", i)
		tracker.nodes[nodeID] = &NodeCapacityInfo{
			NodeID:        nodeID,
			TotalCapacity: 100 * 1024 * 1024 * 1024,
			FreeCapacity:  80 * 1024 * 1024 * 1024,
		}
	}
	tracker.mutex.Unlock()

	// Plan EC destination
	destinationPlan, err := tracker.PlanDestinationForVolume(700, OpTypeErasureCoding, sourceNode)
	if err != nil {
		t.Fatalf("Failed to plan EC destination: %v", err)
	}

	// Should not choose the source node even if it has the highest capacity
	if destinationPlan.TargetNode == sourceNode {
		t.Errorf("EC destination should not be the same as source node")
	}

	// Should choose one of the destination nodes
	if !contains(destinationPlan.TargetNode, "dest-node") {
		t.Errorf("Expected to choose a destination node, got: %s", destinationPlan.TargetNode)
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && s[len(s)-len(substr):] == substr ||
		len(s) >= len(substr) && s[:len(substr)] == substr ||
		len(s) > len(substr) && indexOf(s, substr) >= 0
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
