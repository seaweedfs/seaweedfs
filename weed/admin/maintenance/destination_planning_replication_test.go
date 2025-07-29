package maintenance

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

func TestReplicationDestinationPlanning_AddNewReplica(t *testing.T) {
	pendingOps := NewPendingOperations()
	tracker := NewVolumeShardTracker(pendingOps)

	// Set placement rule requiring 3 replicas
	rule := &PlacementRule{
		Collection:     "critical",
		ReplicaCount:   3,
		DifferentRacks: true,
		DifferentDCs:   false,
	}
	tracker.setPlacementRule("critical", rule)

	// Create volume with only 2 replicas (needs 1 more)
	metrics := []*types.VolumeHealthMetrics{
		{
			VolumeID:   100,
			Server:     "rack1-node1:9333",
			Collection: "critical",
			Size:       10 * 1024 * 1024 * 1024,
		},
		{
			VolumeID:   100,
			Server:     "rack2-node1:9333",
			Collection: "critical",
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
	tracker.nodes["rack2-node1:9333"] = &NodeCapacityInfo{
		NodeID:        "rack2-node1:9333",
		Rack:          "rack2",
		DataCenter:    "default",
		TotalCapacity: 100 * 1024 * 1024 * 1024,
		FreeCapacity:  85 * 1024 * 1024 * 1024,
	}
	tracker.nodes["rack3-node1:9333"] = &NodeCapacityInfo{
		NodeID:        "rack3-node1:9333",
		Rack:          "rack3",
		DataCenter:    "default",
		TotalCapacity: 100 * 1024 * 1024 * 1024,
		FreeCapacity:  90 * 1024 * 1024 * 1024,
	}
	tracker.nodes["rack4-node1:9333"] = &NodeCapacityInfo{
		NodeID:        "rack4-node1:9333",
		Rack:          "rack4",
		DataCenter:    "default",
		TotalCapacity: 100 * 1024 * 1024 * 1024,
		FreeCapacity:  95 * 1024 * 1024 * 1024,
	}
	tracker.mutex.Unlock()

	// Plan destination for replication
	destinationPlan, err := tracker.PlanDestinationForVolume(100, OpTypeReplication, "")
	if err != nil {
		t.Fatalf("Failed to plan replication destination: %v", err)
	}

	// Should choose a rack different from existing replicas (rack3 or rack4)
	if destinationPlan.TargetRack == "rack1" || destinationPlan.TargetRack == "rack2" {
		t.Errorf("Expected to choose different rack for replication, got %s", destinationPlan.TargetRack)
	}

	// Should have correct estimated size
	expectedSize := uint64(10 * 1024 * 1024 * 1024)
	if destinationPlan.ExpectedSize != expectedSize {
		t.Errorf("Expected size %d, got %d", expectedSize, destinationPlan.ExpectedSize)
	}
}

func TestReplicationDestinationPlanning_AlreadyMaxReplicas(t *testing.T) {
	pendingOps := NewPendingOperations()
	tracker := NewVolumeShardTracker(pendingOps)

	// Set placement rule requiring only 2 replicas
	rule := &PlacementRule{
		Collection:     "standard",
		ReplicaCount:   2,
		DifferentRacks: true,
		DifferentDCs:   false,
	}
	tracker.setPlacementRule("standard", rule)

	// Create volume with already 2 replicas (max reached)
	metrics := []*types.VolumeHealthMetrics{
		{
			VolumeID:   200,
			Server:     "rack1-node1:9333",
			Collection: "standard",
			Size:       5 * 1024 * 1024 * 1024,
		},
		{
			VolumeID:   200,
			Server:     "rack2-node1:9333",
			Collection: "standard",
			Size:       5 * 1024 * 1024 * 1024,
		},
	}

	err := tracker.UpdateFromMaster(metrics)
	if err != nil {
		t.Fatalf("Failed to update tracker: %v", err)
	}

	// Plan destination should fail because we already have max replicas
	_, err = tracker.PlanDestinationForVolume(200, OpTypeReplication, "")
	if err == nil {
		t.Errorf("Expected planning to fail when already at max replicas")
	}
}

func TestReplicationDestinationPlanning_DCDistribution(t *testing.T) {
	pendingOps := NewPendingOperations()
	tracker := NewVolumeShardTracker(pendingOps)

	// Set placement rule requiring different DCs
	rule := &PlacementRule{
		Collection:     "geo-distributed",
		ReplicaCount:   3,
		DifferentRacks: false,
		DifferentDCs:   true,
	}
	tracker.setPlacementRule("geo-distributed", rule)

	// Create volume with replica in DC1
	metrics := []*types.VolumeHealthMetrics{
		{
			VolumeID:   300,
			Server:     "dc1-node1:9333",
			Collection: "geo-distributed",
			Size:       20 * 1024 * 1024 * 1024,
		},
	}

	err := tracker.UpdateFromMaster(metrics)
	if err != nil {
		t.Fatalf("Failed to update tracker: %v", err)
	}

	// Add nodes in different DCs
	tracker.mutex.Lock()
	tracker.nodes["dc1-node1:9333"] = &NodeCapacityInfo{
		NodeID:        "dc1-node1:9333",
		Rack:          "rack1",
		DataCenter:    "dc1",
		TotalCapacity: 100 * 1024 * 1024 * 1024,
		FreeCapacity:  70 * 1024 * 1024 * 1024,
	}
	tracker.nodes["dc1-node2:9333"] = &NodeCapacityInfo{
		NodeID:        "dc1-node2:9333",
		Rack:          "rack2",
		DataCenter:    "dc1", // Same DC as existing replica
		TotalCapacity: 100 * 1024 * 1024 * 1024,
		FreeCapacity:  90 * 1024 * 1024 * 1024,
	}
	tracker.nodes["dc2-node1:9333"] = &NodeCapacityInfo{
		NodeID:        "dc2-node1:9333",
		Rack:          "rack1",
		DataCenter:    "dc2", // Different DC
		TotalCapacity: 100 * 1024 * 1024 * 1024,
		FreeCapacity:  95 * 1024 * 1024 * 1024,
	}
	tracker.mutex.Unlock()

	// Plan destination for replication
	destinationPlan, err := tracker.PlanDestinationForVolume(300, OpTypeReplication, "")
	if err != nil {
		t.Fatalf("Failed to plan replication destination: %v", err)
	}

	// Should choose dc2 (different DC)
	if destinationPlan.TargetDC == "dc1" {
		t.Errorf("Expected to choose different DC than dc1, got %s", destinationPlan.TargetDC)
		t.Logf("Placement score: %f, Conflicts: %v", destinationPlan.PlacementScore, destinationPlan.Conflicts)
	}
}

func TestReplicationDestinationPlanning_InsufficientCapacity(t *testing.T) {
	pendingOps := NewPendingOperations()
	tracker := NewVolumeShardTracker(pendingOps)

	// Set placement rule requiring 2 replicas
	rule := &PlacementRule{
		Collection:     "large-files",
		ReplicaCount:   2,
		DifferentRacks: true,
		DifferentDCs:   false,
	}
	tracker.setPlacementRule("large-files", rule)

	// Create large volume with one replica
	metrics := []*types.VolumeHealthMetrics{
		{
			VolumeID:   400,
			Server:     "node1:9333",
			Collection: "large-files",
			Size:       80 * 1024 * 1024 * 1024, // 80GB
		},
	}

	err := tracker.UpdateFromMaster(metrics)
	if err != nil {
		t.Fatalf("Failed to update tracker: %v", err)
	}

	// Add nodes with insufficient capacity
	tracker.mutex.Lock()
	tracker.nodes["node1:9333"] = &NodeCapacityInfo{
		NodeID:        "node1:9333",
		Rack:          "rack1",
		DataCenter:    "default",
		TotalCapacity: 100 * 1024 * 1024 * 1024,
		FreeCapacity:  10 * 1024 * 1024 * 1024, // Only 10GB free
	}
	tracker.nodes["node2:9333"] = &NodeCapacityInfo{
		NodeID:        "node2:9333",
		Rack:          "rack2",
		DataCenter:    "default",
		TotalCapacity: 100 * 1024 * 1024 * 1024,
		FreeCapacity:  50 * 1024 * 1024 * 1024, // Only 50GB free (insufficient for 80GB volume)
	}
	tracker.mutex.Unlock()

	// Plan destination should fail due to insufficient capacity
	_, err = tracker.PlanDestinationForVolume(400, OpTypeReplication, "")
	if err == nil {
		t.Errorf("Expected planning to fail due to insufficient capacity")
	}
}

func TestReplicationDestinationPlanning_WithPendingOperations(t *testing.T) {
	pendingOps := NewPendingOperations()
	tracker := NewVolumeShardTracker(pendingOps)

	// Add a pending operation that affects capacity
	pendingOp := &PendingOperation{
		VolumeID:      500,
		OperationType: OpTypeReplication,
		SourceNode:    "",
		DestNode:      "node2:9333",
		TaskID:        "test-replication-001",
		EstimatedSize: 30 * 1024 * 1024 * 1024, // 30GB
		Status:        "in_progress",
	}
	pendingOps.AddOperation(pendingOp)

	// Set placement rule
	rule := &PlacementRule{
		Collection:     "pending-test",
		ReplicaCount:   2,
		DifferentRacks: true,
		DifferentDCs:   false,
	}
	tracker.setPlacementRule("pending-test", rule)

	// Create volume needing replication
	metrics := []*types.VolumeHealthMetrics{
		{
			VolumeID:   600,
			Server:     "node1:9333",
			Collection: "pending-test",
			Size:       25 * 1024 * 1024 * 1024, // 25GB
		},
	}

	err := tracker.UpdateFromMaster(metrics)
	if err != nil {
		t.Fatalf("Failed to update tracker: %v", err)
	}

	// Add nodes
	tracker.mutex.Lock()
	tracker.nodes["node1:9333"] = &NodeCapacityInfo{
		NodeID:        "node1:9333",
		Rack:          "rack1",
		DataCenter:    "default",
		TotalCapacity: 100 * 1024 * 1024 * 1024,
		FreeCapacity:  70 * 1024 * 1024 * 1024,
	}
	tracker.nodes["node2:9333"] = &NodeCapacityInfo{
		NodeID:        "node2:9333",
		Rack:          "rack2",
		DataCenter:    "default",
		TotalCapacity: 100 * 1024 * 1024 * 1024,
		FreeCapacity:  40 * 1024 * 1024 * 1024, // 40GB free
	}
	tracker.nodes["node3:9333"] = &NodeCapacityInfo{
		NodeID:        "node3:9333",
		Rack:          "rack3",
		DataCenter:    "default",
		TotalCapacity: 100 * 1024 * 1024 * 1024,
		FreeCapacity:  80 * 1024 * 1024 * 1024, // More free space
	}
	tracker.mutex.Unlock()

	// Plan destination for replication
	destinationPlan, err := tracker.PlanDestinationForVolume(600, OpTypeReplication, "")
	if err != nil {
		t.Fatalf("Failed to plan replication destination: %v", err)
	}

	// Should prefer node3 since node2 has pending incoming data
	// (40GB free - 30GB pending = 10GB, which is less than the 25GB needed)
	if destinationPlan.TargetNode == "node2:9333" {
		t.Logf("Warning: Chose node2 despite pending operation affecting capacity")
	}

	// Log the choice for debugging
	t.Logf("Chose node: %s", destinationPlan.TargetNode)
}
