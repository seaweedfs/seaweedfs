package gateway

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/protocol"
)

func TestCoordinatorRegistry_DeterministicNodeID(t *testing.T) {
	// Test that node IDs are deterministic and stable
	addr1 := "gateway1:9092"
	addr2 := "gateway2:9092"
	
	id1a := generateDeterministicNodeID(addr1)
	id1b := generateDeterministicNodeID(addr1)
	id2 := generateDeterministicNodeID(addr2)
	
	if id1a != id1b {
		t.Errorf("Node ID should be deterministic: %d != %d", id1a, id1b)
	}
	
	if id1a == id2 {
		t.Errorf("Different addresses should have different node IDs: %d == %d", id1a, id2)
	}
	
	if id1a <= 0 || id2 <= 0 {
		t.Errorf("Node IDs should be positive: %d, %d", id1a, id2)
	}
}

func TestCoordinatorRegistry_BasicOperations(t *testing.T) {
	// Create a test registry without actual filer connection
	registry := &CoordinatorRegistry{
		assignments:      make(map[string]*protocol.CoordinatorAssignment),
		activeGateways:   make(map[string]*GatewayInfo),
		gatewayAddress:   "test-gateway:9092",
		stopChan:         make(chan struct{}),
		leadershipChange: make(chan string, 10),
		isLeader:         true, // Simulate being leader for tests
	}
	
	// Test gateway registration
	gatewayAddr := "test-gateway:9092"
	registry.registerGateway(gatewayAddr)
	
	if len(registry.activeGateways) != 1 {
		t.Errorf("Expected 1 gateway, got %d", len(registry.activeGateways))
	}
	
	gateway, exists := registry.activeGateways[gatewayAddr]
	if !exists {
		t.Error("Gateway should be registered")
	}
	
	if gateway.NodeID <= 0 {
		t.Errorf("Gateway should have positive node ID, got %d", gateway.NodeID)
	}
	
	// Test gateway health check
	if !registry.isGatewayHealthyUnsafe(gatewayAddr) {
		t.Error("Newly registered gateway should be healthy")
	}
	
	// Test node ID retrieval
	nodeID := registry.getGatewayNodeIDUnsafe(gatewayAddr)
	if nodeID != gateway.NodeID {
		t.Errorf("Expected node ID %d, got %d", gateway.NodeID, nodeID)
	}
}

func TestCoordinatorRegistry_AssignCoordinator(t *testing.T) {
	registry := &CoordinatorRegistry{
		assignments:      make(map[string]*protocol.CoordinatorAssignment),
		activeGateways:   make(map[string]*GatewayInfo),
		gatewayAddress:   "test-gateway:9092",
		stopChan:         make(chan struct{}),
		leadershipChange: make(chan string, 10),
		isLeader:         true,
	}
	
	// Register a gateway
	gatewayAddr := "test-gateway:9092"
	registry.registerGateway(gatewayAddr)
	
	// Test coordinator assignment when not leader
	registry.isLeader = false
	_, err := registry.AssignCoordinator("test-group", gatewayAddr)
	if err == nil {
		t.Error("Should fail when not leader")
	}
	
	// Test coordinator assignment when leader
	registry.isLeader = true
	assignment, err := registry.AssignCoordinator("test-group", gatewayAddr)
	if err != nil {
		t.Errorf("Failed to assign coordinator: %v", err)
	}
	
	if assignment.ConsumerGroup != "test-group" {
		t.Errorf("Expected group 'test-group', got '%s'", assignment.ConsumerGroup)
	}
	
	if assignment.CoordinatorAddr != gatewayAddr {
		t.Errorf("Expected coordinator '%s', got '%s'", gatewayAddr, assignment.CoordinatorAddr)
	}
	
	// Test getting existing assignment
	retrieved, err := registry.GetCoordinator("test-group")
	if err != nil {
		t.Errorf("Failed to get coordinator: %v", err)
	}
	
	if retrieved.ConsumerGroup != assignment.ConsumerGroup {
		t.Error("Retrieved assignment should match original")
	}
	
	// Test getting non-existent assignment
	_, err = registry.GetCoordinator("non-existent-group")
	if err == nil {
		t.Error("Should fail for non-existent group")
	}
}

func TestCoordinatorRegistry_HealthyGateways(t *testing.T) {
	registry := &CoordinatorRegistry{
		assignments:      make(map[string]*protocol.CoordinatorAssignment),
		activeGateways:   make(map[string]*GatewayInfo),
		gatewayAddress:   "test-gateway:9092",
		stopChan:         make(chan struct{}),
		leadershipChange: make(chan string, 10),
		isLeader:         true,
	}
	
	// Register multiple gateways
	gateways := []string{"gateway1:9092", "gateway2:9092", "gateway3:9092"}
	for _, addr := range gateways {
		registry.registerGateway(addr)
	}
	
	// All should be healthy initially
	healthy := registry.getHealthyGatewaysSorted()
	if len(healthy) != len(gateways) {
		t.Errorf("Expected %d healthy gateways, got %d", len(gateways), len(healthy))
	}
	
	// Make one gateway stale
	registry.activeGateways["gateway2:9092"].LastHeartbeat = time.Now().Add(-2 * GatewayTimeout)
	
	healthy = registry.getHealthyGatewaysSorted()
	if len(healthy) != len(gateways)-1 {
		t.Errorf("Expected %d healthy gateways after one became stale, got %d", len(gateways)-1, len(healthy))
	}
	
	// Check that results are sorted
	for i := 1; i < len(healthy); i++ {
		if healthy[i-1] >= healthy[i] {
			t.Errorf("Healthy gateways should be sorted: %v", healthy)
			break
		}
	}
}

func TestCoordinatorRegistry_ConsistentHashing(t *testing.T) {
	registry := &CoordinatorRegistry{
		assignments:      make(map[string]*protocol.CoordinatorAssignment),
		activeGateways:   make(map[string]*GatewayInfo),
		gatewayAddress:   "test-gateway:9092",
		stopChan:         make(chan struct{}),
		leadershipChange: make(chan string, 10),
		isLeader:         true,
	}
	
	// Register multiple gateways
	gateways := []string{"gateway1:9092", "gateway2:9092", "gateway3:9092"}
	for _, addr := range gateways {
		registry.registerGateway(addr)
	}
	
	// Test that same group always gets same coordinator
	group := "test-group"
	addr1, nodeID1, err1 := registry.chooseCoordinatorAddrForGroup(group)
	addr2, nodeID2, err2 := registry.chooseCoordinatorAddrForGroup(group)
	
	if err1 != nil || err2 != nil {
		t.Errorf("Failed to choose coordinator: %v, %v", err1, err2)
	}
	
	if addr1 != addr2 || nodeID1 != nodeID2 {
		t.Errorf("Consistent hashing should return same result: (%s,%d) != (%s,%d)", 
			addr1, nodeID1, addr2, nodeID2)
	}
	
	// Test that different groups can get different coordinators
	groups := []string{"group1", "group2", "group3", "group4", "group5"}
	coordinators := make(map[string]bool)
	
	for _, g := range groups {
		addr, _, err := registry.chooseCoordinatorAddrForGroup(g)
		if err != nil {
			t.Errorf("Failed to choose coordinator for %s: %v", g, err)
		}
		coordinators[addr] = true
	}
	
	// With multiple groups and gateways, we should see some distribution
	// (though not guaranteed due to hashing)
	if len(coordinators) == 1 && len(gateways) > 1 {
		t.Log("Warning: All groups mapped to same coordinator (possible but unlikely)")
	}
}

func TestCoordinatorRegistry_CleanupStaleEntries(t *testing.T) {
	registry := &CoordinatorRegistry{
		assignments:      make(map[string]*protocol.CoordinatorAssignment),
		activeGateways:   make(map[string]*GatewayInfo),
		gatewayAddress:   "test-gateway:9092",
		stopChan:         make(chan struct{}),
		leadershipChange: make(chan string, 10),
		isLeader:         true,
	}
	
	// Register gateways and create assignments
	gateway1 := "gateway1:9092"
	gateway2 := "gateway2:9092"
	
	registry.registerGateway(gateway1)
	registry.registerGateway(gateway2)
	
	// Create assignments
	registry.assignments["group1"] = &protocol.CoordinatorAssignment{
		ConsumerGroup:   "group1",
		CoordinatorAddr: gateway1,
	}
	registry.assignments["group2"] = &protocol.CoordinatorAssignment{
		ConsumerGroup:   "group2", 
		CoordinatorAddr: gateway2,
	}
	
	// Make gateway2 stale
	registry.activeGateways[gateway2].LastHeartbeat = time.Now().Add(-2 * GatewayTimeout)
	
	// Run cleanup
	registry.cleanupStaleEntries()
	
	// Check that stale gateway was removed
	if _, exists := registry.activeGateways[gateway2]; exists {
		t.Error("Stale gateway should be removed")
	}
	
	// Check that assignment for stale gateway was removed
	if _, exists := registry.assignments["group2"]; exists {
		t.Error("Assignment for stale gateway should be removed")
	}
	
	// Check that healthy gateway and its assignment remain
	if _, exists := registry.activeGateways[gateway1]; !exists {
		t.Error("Healthy gateway should remain")
	}
	
	if _, exists := registry.assignments["group1"]; !exists {
		t.Error("Assignment for healthy gateway should remain")
	}
}

func TestCoordinatorRegistry_GetStats(t *testing.T) {
	registry := &CoordinatorRegistry{
		assignments:      make(map[string]*protocol.CoordinatorAssignment),
		activeGateways:   make(map[string]*GatewayInfo),
		gatewayAddress:   "test-gateway:9092",
		stopChan:         make(chan struct{}),
		leadershipChange: make(chan string, 10),
		isLeader:         true,
	}
	
	// Add some data
	registry.registerGateway("gateway1:9092")
	registry.registerGateway("gateway2:9092")
	
	registry.assignments["group1"] = &protocol.CoordinatorAssignment{
		ConsumerGroup:   "group1",
		CoordinatorAddr: "gateway1:9092",
	}
	
	stats := registry.GetStats()
	
	if stats["active_gateways"] != 2 {
		t.Errorf("Expected 2 active gateways, got %v", stats["active_gateways"])
	}
	
	if stats["assignments"] != 1 {
		t.Errorf("Expected 1 assignment, got %v", stats["assignments"])
	}
	
	if stats["gateway_address"] != "test-gateway:9092" {
		t.Errorf("Expected gateway address 'test-gateway:9092', got %v", stats["gateway_address"])
	}
	
	if stats["is_leader"] != true {
		t.Errorf("Expected is_leader to be true, got %v", stats["is_leader"])
	}
}

func TestCoordinatorRegistry_HeartbeatGateway(t *testing.T) {
	registry := &CoordinatorRegistry{
		assignments:      make(map[string]*protocol.CoordinatorAssignment),
		activeGateways:   make(map[string]*GatewayInfo),
		gatewayAddress:   "test-gateway:9092",
		stopChan:         make(chan struct{}),
		leadershipChange: make(chan string, 10),
		isLeader:         true,
	}
	
	gatewayAddr := "test-gateway:9092"
	
	// Test heartbeat for non-existent gateway (should auto-register)
	err := registry.HeartbeatGateway(gatewayAddr)
	if err != nil {
		t.Errorf("Heartbeat should succeed and auto-register: %v", err)
	}
	
	if len(registry.activeGateways) != 1 {
		t.Errorf("Gateway should be auto-registered")
	}
	
	// Test heartbeat for existing gateway
	originalTime := registry.activeGateways[gatewayAddr].LastHeartbeat
	time.Sleep(10 * time.Millisecond) // Ensure time difference
	
	err = registry.HeartbeatGateway(gatewayAddr)
	if err != nil {
		t.Errorf("Heartbeat should succeed: %v", err)
	}
	
	newTime := registry.activeGateways[gatewayAddr].LastHeartbeat
	if !newTime.After(originalTime) {
		t.Error("Heartbeat should update LastHeartbeat time")
	}
	
	// Test heartbeat when not leader
	registry.isLeader = false
	err = registry.HeartbeatGateway(gatewayAddr)
	if err == nil {
		t.Error("Heartbeat should fail when not leader")
	}
}
