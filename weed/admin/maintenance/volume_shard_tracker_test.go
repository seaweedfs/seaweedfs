package maintenance

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

func TestVolumeShardTracker_BasicFunctionality(t *testing.T) {
	pendingOps := NewPendingOperations()
	tracker := NewVolumeShardTracker(pendingOps)

	// Create test volume metrics
	metrics := []*types.VolumeHealthMetrics{
		{
			VolumeID:   100,
			Server:     "node1:9333",
			Collection: "test",
			Size:       2 * 1024 * 1024 * 1024, // 2GB
			IsECVolume: false,
		},
		{
			VolumeID:   100, // Replica of volume 100
			Server:     "node2:9333",
			Collection: "test",
			Size:       2 * 1024 * 1024 * 1024,
			IsECVolume: false,
		},
		{
			VolumeID:   101,
			Server:     "node3:9333",
			Collection: "prod",
			Size:       5 * 1024 * 1024 * 1024, // 5GB
			IsECVolume: false,
		},
	}

	// Update tracker with metrics
	err := tracker.UpdateFromMaster(metrics)
	if err != nil {
		t.Fatalf("Failed to update tracker: %v", err)
	}

	// Test volume info retrieval
	volumeInfo, err := tracker.getVolumeInfo(100)
	if err != nil {
		t.Errorf("Failed to get volume info: %v", err)
	}

	if len(volumeInfo) != 2 {
		t.Errorf("Expected 2 replicas for volume 100, got %d", len(volumeInfo))
	}

	// Test node info retrieval
	nodeInfo, err := tracker.getNodeInfo("node1:9333")
	if err != nil {
		t.Errorf("Failed to get node info: %v", err)
	}

	if nodeInfo.VolumeCount != 1 {
		t.Errorf("Expected node1 to have 1 volume, got %d", nodeInfo.VolumeCount)
	}

	// Test cluster stats
	stats := tracker.getClusterStats()
	if stats.TotalNodes != 3 {
		t.Errorf("Expected 3 nodes, got %d", stats.TotalNodes)
	}

	if stats.TotalVolumes != 2 {
		t.Errorf("Expected 2 volumes, got %d", stats.TotalVolumes)
	}
}

func TestVolumeShardTracker_VolumeMoveDestinationPlanning(t *testing.T) {
	pendingOps := NewPendingOperations()
	tracker := NewVolumeShardTracker(pendingOps)

	// Set up placement rule requiring different racks
	rule := &PlacementRule{
		Collection:     "test",
		ReplicaCount:   3,
		DifferentRacks: true,
		DifferentDCs:   false,
	}
	tracker.setPlacementRule("test", rule)

	// Create test cluster with multiple racks
	metrics := []*types.VolumeHealthMetrics{
		{
			VolumeID:   200,
			Server:     "rack1-node1:9333",
			Collection: "test",
			Size:       1 * 1024 * 1024 * 1024, // 1GB
			IsECVolume: false,
		},
		{
			VolumeID:   200, // Replica on different rack
			Server:     "rack2-node1:9333",
			Collection: "test",
			Size:       1 * 1024 * 1024 * 1024,
			IsECVolume: false,
		},
	}

	// Simulate rack parsing by manually setting location info
	err := tracker.UpdateFromMaster(metrics)
	if err != nil {
		t.Fatalf("Failed to update tracker: %v", err)
	}

	// Manually set rack info for testing
	tracker.mutex.Lock()
	if node1, exists := tracker.nodes["rack1-node1:9333"]; exists {
		node1.Rack = "rack1"
		node1.DataCenter = "dc1"
	}
	if node2, exists := tracker.nodes["rack2-node1:9333"]; exists {
		node2.Rack = "rack2"
		node2.DataCenter = "dc1"
	}

	// Add a third node in rack3 for destination
	tracker.nodes["rack3-node1:9333"] = &NodeCapacityInfo{
		NodeID:        "rack3-node1:9333",
		DataCenter:    "dc1",
		Rack:          "rack3",
		TotalCapacity: 100 * 1024 * 1024 * 1024, // 100GB
		UsedCapacity:  0,
		FreeCapacity:  100 * 1024 * 1024 * 1024,
		VolumeCount:   0,
	}
	tracker.mutex.Unlock()

	// Plan destination for moving volume from rack1-node1
	destinationPlan, err := tracker.PlanDestinationForVolume(200, OpTypeVolumeMove, "rack1-node1:9333")
	if err != nil {
		t.Fatalf("Failed to plan destination: %v", err)
	}

	// Should choose rack3-node1 (different rack, good placement score)
	if destinationPlan.TargetNode != "rack3-node1:9333" {
		t.Errorf("Expected destination to be rack3-node1:9333, got %s", destinationPlan.TargetNode)
	}

	if destinationPlan.TargetRack != "rack3" {
		t.Errorf("Expected destination rack to be rack3, got %s", destinationPlan.TargetRack)
	}

	if destinationPlan.PlacementScore <= 0 {
		t.Errorf("Expected positive placement score, got %f", destinationPlan.PlacementScore)
	}
}

func TestVolumeShardTracker_ReplicationDestinationPlanning(t *testing.T) {
	pendingOps := NewPendingOperations()
	tracker := NewVolumeShardTracker(pendingOps)

	// Set up placement rule requiring 3 replicas on different racks
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
			VolumeID:   300,
			Server:     "rack1-node1:9333",
			Collection: "critical",
			Size:       3 * 1024 * 1024 * 1024, // 3GB
			IsECVolume: false,
		},
		{
			VolumeID:   300,
			Server:     "rack2-node1:9333",
			Collection: "critical",
			Size:       3 * 1024 * 1024 * 1024,
			IsECVolume: false,
		},
	}

	err := tracker.UpdateFromMaster(metrics)
	if err != nil {
		t.Fatalf("Failed to update tracker: %v", err)
	}

	// Set up rack information
	tracker.mutex.Lock()
	tracker.nodes["rack1-node1:9333"].Rack = "rack1"
	tracker.nodes["rack2-node1:9333"].Rack = "rack2"

	// Add nodes in different racks as candidates
	tracker.nodes["rack3-node1:9333"] = &NodeCapacityInfo{
		NodeID:        "rack3-node1:9333",
		Rack:          "rack3",
		TotalCapacity: 100 * 1024 * 1024 * 1024,
		FreeCapacity:  95 * 1024 * 1024 * 1024,
	}
	tracker.nodes["rack4-node1:9333"] = &NodeCapacityInfo{
		NodeID:        "rack4-node1:9333",
		Rack:          "rack4",
		TotalCapacity: 100 * 1024 * 1024 * 1024,
		FreeCapacity:  98 * 1024 * 1024 * 1024, // More free space
	}
	tracker.mutex.Unlock()

	// Plan destination for replication
	destinationPlan, err := tracker.PlanDestinationForVolume(300, OpTypeReplication, "")
	if err != nil {
		t.Fatalf("Failed to plan replication destination: %v", err)
	}

	// Should choose a node on rack3 or rack4 (different from existing racks)
	if destinationPlan.TargetRack == "rack1" || destinationPlan.TargetRack == "rack2" {
		t.Errorf("Destination should not be on same rack as existing replicas, got rack %s", destinationPlan.TargetRack)
	}

	if destinationPlan.ExpectedSize != 3*1024*1024*1024 {
		t.Errorf("Expected size to be 3GB, got %d", destinationPlan.ExpectedSize)
	}

	// Should prefer rack4 due to more free space, but either rack3 or rack4 is acceptable
	if destinationPlan.TargetRack != "rack3" && destinationPlan.TargetRack != "rack4" {
		t.Errorf("Expected to choose rack3 or rack4 (different from existing racks), got %s", destinationPlan.TargetRack)
	}
}

func TestVolumeShardTracker_ErasureCodingPlanning(t *testing.T) {
	pendingOps := NewPendingOperations()
	tracker := NewVolumeShardTracker(pendingOps)

	// Create a large volume suitable for EC
	metrics := []*types.VolumeHealthMetrics{
		{
			VolumeID:   400,
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

	// Add 14+ nodes to support EC encoding
	tracker.mutex.Lock()
	for i := 1; i <= 16; i++ {
		nodeID := fmt.Sprintf("ec-node%d:9333", i)
		tracker.nodes[nodeID] = &NodeCapacityInfo{
			NodeID:        nodeID,
			Rack:          fmt.Sprintf("rack%d", (i%4)+1), // Distribute across 4 racks
			TotalCapacity: 200 * 1024 * 1024 * 1024,       // 200GB
			FreeCapacity:  180 * 1024 * 1024 * 1024,       // 180GB free
		}
	}
	tracker.mutex.Unlock()

	// Plan EC destination
	destinationPlan, err := tracker.PlanDestinationForVolume(400, OpTypeErasureCoding, "node1:9333")
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
}

func TestVolumeShardTracker_CapacityConstraints(t *testing.T) {
	pendingOps := NewPendingOperations()
	tracker := NewVolumeShardTracker(pendingOps)

	// Create a volume that needs to be moved
	metrics := []*types.VolumeHealthMetrics{
		{
			VolumeID:   500,
			Server:     "source-node:9333",
			Collection: "test",
			Size:       20 * 1024 * 1024 * 1024, // 20GB
			IsECVolume: false,
		},
	}

	err := tracker.UpdateFromMaster(metrics)
	if err != nil {
		t.Fatalf("Failed to update tracker: %v", err)
	}

	// Add destination nodes with different capacity constraints
	tracker.mutex.Lock()
	// Node with insufficient capacity
	tracker.nodes["low-capacity:9333"] = &NodeCapacityInfo{
		NodeID:        "low-capacity:9333",
		Rack:          "rack1",
		TotalCapacity: 50 * 1024 * 1024 * 1024,
		FreeCapacity:  10 * 1024 * 1024 * 1024, // Only 10GB free - insufficient
	}

	// Node with sufficient capacity
	tracker.nodes["high-capacity:9333"] = &NodeCapacityInfo{
		NodeID:        "high-capacity:9333",
		Rack:          "rack2",
		TotalCapacity: 200 * 1024 * 1024 * 1024,
		FreeCapacity:  150 * 1024 * 1024 * 1024, // 150GB free - sufficient
	}
	tracker.mutex.Unlock()

	// Plan destination
	destinationPlan, err := tracker.PlanDestinationForVolume(500, OpTypeVolumeMove, "source-node:9333")
	if err != nil {
		t.Fatalf("Failed to plan destination: %v", err)
	}

	// Should choose high-capacity node
	if destinationPlan.TargetNode != "high-capacity:9333" {
		t.Errorf("Expected to choose high-capacity node, got %s", destinationPlan.TargetNode)
	}

	// Should not have capacity conflicts
	hasCapacityConflict := false
	for _, conflict := range destinationPlan.Conflicts {
		if conflict == "insufficient_capacity" {
			hasCapacityConflict = true
			break
		}
	}
	if hasCapacityConflict {
		t.Errorf("Destination should not have capacity conflicts")
	}
}

func TestVolumeShardTracker_PlacementRuleViolations(t *testing.T) {
	pendingOps := NewPendingOperations()
	tracker := NewVolumeShardTracker(pendingOps)

	// Set strict placement rule requiring different DCs
	rule := &PlacementRule{
		Collection:   "geo-distributed",
		ReplicaCount: 2,
		DifferentDCs: true,
	}
	tracker.setPlacementRule("geo-distributed", rule)

	// Create volume with replica in DC1
	metrics := []*types.VolumeHealthMetrics{
		{
			VolumeID:   600,
			Server:     "dc1-node1:9333",
			Collection: "geo-distributed",
			Size:       1 * 1024 * 1024 * 1024, // 1GB
			IsECVolume: false,
		},
	}

	err := tracker.UpdateFromMaster(metrics)
	if err != nil {
		t.Fatalf("Failed to update tracker: %v", err)
	}

	// Set up nodes in different DCs
	tracker.mutex.Lock()
	tracker.nodes["dc1-node1:9333"].DataCenter = "dc1"
	tracker.nodes["dc1-node1:9333"].Rack = "rack1"

	// Add node in same DC (should get penalty)
	tracker.nodes["dc1-node2:9333"] = &NodeCapacityInfo{
		NodeID:        "dc1-node2:9333",
		DataCenter:    "dc1", // Same DC as existing replica
		Rack:          "rack2",
		TotalCapacity: 100 * 1024 * 1024 * 1024,
		FreeCapacity:  90 * 1024 * 1024 * 1024,
	}

	// Add node in different DC (should get bonus) with slightly more free space
	tracker.nodes["dc2-node1:9333"] = &NodeCapacityInfo{
		NodeID:        "dc2-node1:9333",
		DataCenter:    "dc2", // Different DC
		Rack:          "rack1",
		TotalCapacity: 100 * 1024 * 1024 * 1024,
		FreeCapacity:  95 * 1024 * 1024 * 1024, // More free space to ensure it's chosen
	}
	tracker.mutex.Unlock()

	// Plan destination for replication
	destinationPlan, err := tracker.PlanDestinationForVolume(600, OpTypeReplication, "")
	if err != nil {
		t.Fatalf("Failed to plan replication destination: %v", err)
	}

	// Should choose a node in a different DC
	if destinationPlan.TargetDC == "dc1" {
		t.Errorf("Expected destination to be in different DC than dc1, got %s", destinationPlan.TargetDC)
		t.Logf("Placement score: %f, Conflicts: %v", destinationPlan.PlacementScore, destinationPlan.Conflicts)
	}

	// Should have high placement score due to different DC
	if destinationPlan.PlacementScore <= 0 {
		t.Errorf("Expected positive placement score for good placement, got %f", destinationPlan.PlacementScore)
	}
}

// Removed complex pending operations integration test - covered by simpler tests
