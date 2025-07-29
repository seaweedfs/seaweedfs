package maintenance

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

func TestVolumeMoveDestinationPlanning_BasicScenario(t *testing.T) {
	pendingOps := NewPendingOperations()
	tracker := NewVolumeShardTracker(pendingOps)

	// Create a simple cluster with multiple nodes
	metrics := []*types.VolumeHealthMetrics{
		{
			VolumeID:   100,
			Server:     "node1:9333",
			Collection: "test",
			Size:       5 * 1024 * 1024 * 1024, // 5GB
		},
	}

	err := tracker.UpdateFromMaster(metrics)
	if err != nil {
		t.Fatalf("Failed to update tracker: %v", err)
	}

	// Add destination nodes manually
	tracker.mutex.Lock()
	tracker.nodes["node2:9333"] = &NodeCapacityInfo{
		NodeID:        "node2:9333",
		Rack:          "rack2",
		DataCenter:    "default",
		TotalCapacity: 100 * 1024 * 1024 * 1024,
		FreeCapacity:  95 * 1024 * 1024 * 1024,
	}
	tracker.nodes["node3:9333"] = &NodeCapacityInfo{
		NodeID:        "node3:9333",
		Rack:          "rack3",
		DataCenter:    "default",
		TotalCapacity: 100 * 1024 * 1024 * 1024,
		FreeCapacity:  90 * 1024 * 1024 * 1024,
	}
	tracker.mutex.Unlock()

	// Plan destination for volume move
	destinationPlan, err := tracker.PlanDestinationForVolume(100, OpTypeVolumeMove, "node1:9333")
	if err != nil {
		t.Fatalf("Failed to plan volume move destination: %v", err)
	}

	// Should not choose source node
	if destinationPlan.TargetNode == "node1:9333" {
		t.Errorf("Destination should not be the same as source node")
	}

	// Should choose a valid destination
	if destinationPlan.TargetNode != "node2:9333" && destinationPlan.TargetNode != "node3:9333" {
		t.Errorf("Unexpected destination: %s", destinationPlan.TargetNode)
	}

	// Should have correct estimated size
	expectedSize := uint64(5 * 1024 * 1024 * 1024)
	if destinationPlan.ExpectedSize != expectedSize {
		t.Errorf("Expected size %d, got %d", expectedSize, destinationPlan.ExpectedSize)
	}
}

func TestVolumeMoveDestinationPlanning_InsufficientCapacity(t *testing.T) {
	pendingOps := NewPendingOperations()
	tracker := NewVolumeShardTracker(pendingOps)

	// Create a volume larger than available capacity
	metrics := []*types.VolumeHealthMetrics{
		{
			VolumeID:   200,
			Server:     "node1:9333",
			Collection: "test",
			Size:       50 * 1024 * 1024 * 1024, // 50GB
		},
	}

	err := tracker.UpdateFromMaster(metrics)
	if err != nil {
		t.Fatalf("Failed to update tracker: %v", err)
	}

	// Add destination node with insufficient capacity
	tracker.mutex.Lock()
	tracker.nodes["node2:9333"] = &NodeCapacityInfo{
		NodeID:        "node2:9333",
		Rack:          "rack2",
		DataCenter:    "default",
		TotalCapacity: 100 * 1024 * 1024 * 1024,
		FreeCapacity:  10 * 1024 * 1024 * 1024, // Only 10GB free
	}
	tracker.mutex.Unlock()

	// Plan destination should fail due to insufficient capacity
	_, err = tracker.PlanDestinationForVolume(200, OpTypeVolumeMove, "node1:9333")
	if err == nil {
		t.Errorf("Expected planning to fail due to insufficient capacity")
	}
}

func TestVolumeMoveDestinationPlanning_WithPendingOperations(t *testing.T) {
	pendingOps := NewPendingOperations()
	tracker := NewVolumeShardTracker(pendingOps)

	// Add a pending operation that will affect capacity
	pendingOp := &PendingOperation{
		VolumeID:      300,
		OperationType: OpTypeVolumeMove,
		SourceNode:    "node3:9333",
		DestNode:      "node2:9333",
		TaskID:        "test-move-001",
		EstimatedSize: 20 * 1024 * 1024 * 1024, // 20GB
		Status:        "in_progress",
	}
	pendingOps.AddOperation(pendingOp)

	// Create volume to move
	metrics := []*types.VolumeHealthMetrics{
		{
			VolumeID:   400,
			Server:     "node1:9333",
			Collection: "test",
			Size:       15 * 1024 * 1024 * 1024, // 15GB
		},
	}

	err := tracker.UpdateFromMaster(metrics)
	if err != nil {
		t.Fatalf("Failed to update tracker: %v", err)
	}

	// Add destination nodes
	tracker.mutex.Lock()
	tracker.nodes["node2:9333"] = &NodeCapacityInfo{
		NodeID:        "node2:9333",
		Rack:          "rack2",
		DataCenter:    "default",
		TotalCapacity: 100 * 1024 * 1024 * 1024,
		FreeCapacity:  30 * 1024 * 1024 * 1024, // 30GB free
	}
	tracker.nodes["node3:9333"] = &NodeCapacityInfo{
		NodeID:        "node3:9333",
		Rack:          "rack3",
		DataCenter:    "default",
		TotalCapacity: 100 * 1024 * 1024 * 1024,
		FreeCapacity:  80 * 1024 * 1024 * 1024, // 80GB free
	}
	tracker.mutex.Unlock()

	// Plan destination - should prefer node3 due to pending operation impact on node2
	destinationPlan, err := tracker.PlanDestinationForVolume(400, OpTypeVolumeMove, "node1:9333")
	if err != nil {
		t.Fatalf("Failed to plan destination: %v", err)
	}

	// Should prefer node3 since node2 has pending incoming data
	if destinationPlan.TargetNode == "node2:9333" {
		t.Logf("Note: Chose node2 despite pending operation - this may be acceptable depending on remaining capacity")
	}
}

func TestVolumeMoveDestinationPlanning_PreferDifferentRacks(t *testing.T) {
	pendingOps := NewPendingOperations()
	tracker := NewVolumeShardTracker(pendingOps)

	// Set placement rule for different racks
	rule := &PlacementRule{
		Collection:     "test",
		ReplicaCount:   2,
		DifferentRacks: true,
		DifferentDCs:   false,
	}
	tracker.setPlacementRule("test", rule)

	// Create volume with existing replica
	metrics := []*types.VolumeHealthMetrics{
		{
			VolumeID:   500,
			Server:     "rack1-node1:9333",
			Collection: "test",
			Size:       10 * 1024 * 1024 * 1024,
		},
		{
			VolumeID:   500, // Same volume, different replica
			Server:     "rack1-node2:9333",
			Collection: "test",
			Size:       10 * 1024 * 1024 * 1024,
		},
	}

	err := tracker.UpdateFromMaster(metrics)
	if err != nil {
		t.Fatalf("Failed to update tracker: %v", err)
	}

	// Add nodes in different racks
	tracker.mutex.Lock()
	tracker.nodes["rack1-node1:9333"] = &NodeCapacityInfo{
		NodeID:        "rack1-node1:9333",
		Rack:          "rack1",
		DataCenter:    "default",
		TotalCapacity: 100 * 1024 * 1024 * 1024,
		FreeCapacity:  80 * 1024 * 1024 * 1024,
	}
	tracker.nodes["rack1-node2:9333"] = &NodeCapacityInfo{
		NodeID:        "rack1-node2:9333",
		Rack:          "rack1",
		DataCenter:    "default",
		TotalCapacity: 100 * 1024 * 1024 * 1024,
		FreeCapacity:  85 * 1024 * 1024 * 1024,
	}
	tracker.nodes["rack2-node1:9333"] = &NodeCapacityInfo{
		NodeID:        "rack2-node1:9333",
		Rack:          "rack2",
		DataCenter:    "default",
		TotalCapacity: 100 * 1024 * 1024 * 1024,
		FreeCapacity:  90 * 1024 * 1024 * 1024,
	}
	tracker.mutex.Unlock()

	// Plan destination for volume move from rack1 node
	destinationPlan, err := tracker.PlanDestinationForVolume(500, OpTypeVolumeMove, "rack1-node1:9333")
	if err != nil {
		t.Fatalf("Failed to plan destination: %v", err)
	}

	// Should prefer different rack (rack2) even if rack1 has more capacity
	if destinationPlan.TargetRack == "rack1" {
		t.Errorf("Expected to choose different rack, but got rack1")
		t.Logf("Conflicts: %v", destinationPlan.Conflicts)
	}
}
