package gateway

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/protocol"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"google.golang.org/grpc"
)

// CoordinatorRegistry manages consumer group coordinator assignments
// Only the gateway leader maintains this registry
type CoordinatorRegistry struct {
	// Leader election
	leaderLock       *cluster.LiveLock
	isLeader         bool
	leaderMutex      sync.RWMutex
	leadershipChange chan string // Notifies when leadership changes

	// Coordinator assignments
	assignments      map[string]*protocol.CoordinatorAssignment // consumerGroup -> assignment
	assignmentsMutex sync.RWMutex

	// Gateway registry
	activeGateways map[string]*GatewayInfo // gatewayAddress -> info
	gatewaysMutex  sync.RWMutex

	// Configuration
	gatewayAddress string
	lockClient     *cluster.LockClient

	// Control
	stopChan chan struct{}
	wg       sync.WaitGroup
}

// Remove local CoordinatorAssignment - use protocol.CoordinatorAssignment instead

// GatewayInfo represents an active gateway instance
type GatewayInfo struct {
	Address       string
	NodeID        int32
	RegisteredAt  time.Time
	LastHeartbeat time.Time
	IsHealthy     bool
}

const (
	GatewayLeaderLockKey = "kafka-gateway-leader"
	HeartbeatInterval    = 10 * time.Second
	GatewayTimeout       = 30 * time.Second
)

// NewCoordinatorRegistry creates a new coordinator registry
func NewCoordinatorRegistry(gatewayAddress string, seedFiler pb.ServerAddress, grpcDialOption grpc.DialOption) *CoordinatorRegistry {
	lockClient := cluster.NewLockClient(grpcDialOption, seedFiler)

	registry := &CoordinatorRegistry{
		assignments:      make(map[string]*protocol.CoordinatorAssignment),
		activeGateways:   make(map[string]*GatewayInfo),
		gatewayAddress:   gatewayAddress,
		lockClient:       lockClient,
		stopChan:         make(chan struct{}),
		leadershipChange: make(chan string, 10), // Buffered channel for leadership notifications
	}

	return registry
}

// Start begins the coordinator registry operations
func (cr *CoordinatorRegistry) Start() error {
	glog.V(1).Infof("Starting coordinator registry for gateway %s", cr.gatewayAddress)

	// Start leader election
	cr.startLeaderElection()

	// Start cleanup goroutine
	cr.startCleanupLoop()

	// Register this gateway
	cr.registerGateway(cr.gatewayAddress)

	return nil
}

// Stop shuts down the coordinator registry
func (cr *CoordinatorRegistry) Stop() error {
	glog.V(1).Infof("Stopping coordinator registry for gateway %s", cr.gatewayAddress)

	close(cr.stopChan)
	cr.wg.Wait()

	// Release leader lock if held
	if cr.leaderLock != nil {
		cr.leaderLock.Stop()
	}

	return nil
}

// startLeaderElection starts the leader election process
func (cr *CoordinatorRegistry) startLeaderElection() {
	cr.wg.Add(1)
	go func() {
		defer cr.wg.Done()

		// Start long-lived lock for leader election
		cr.leaderLock = cr.lockClient.StartLongLivedLock(
			GatewayLeaderLockKey,
			cr.gatewayAddress,
			cr.onLeadershipChange,
		)

		// Wait for shutdown
		<-cr.stopChan

		// The leader lock will be stopped when Stop() is called
	}()
}

// onLeadershipChange handles leadership changes
func (cr *CoordinatorRegistry) onLeadershipChange(newLeader string) {
	cr.leaderMutex.Lock()
	defer cr.leaderMutex.Unlock()

	wasLeader := cr.isLeader
	cr.isLeader = (newLeader == cr.gatewayAddress)

	if cr.isLeader && !wasLeader {
		glog.V(0).Infof("Gateway %s became the coordinator registry leader", cr.gatewayAddress)
		cr.onBecameLeader()
	} else if !cr.isLeader && wasLeader {
		glog.V(0).Infof("Gateway %s lost coordinator registry leadership to %s", cr.gatewayAddress, newLeader)
		cr.onLostLeadership()
	}

	// Notify waiting goroutines about leadership change
	select {
	case cr.leadershipChange <- newLeader:
		// Notification sent
	default:
		// Channel full, skip notification (shouldn't happen with buffered channel)
	}
}

// onBecameLeader handles becoming the leader
func (cr *CoordinatorRegistry) onBecameLeader() {
	// Clear stale assignments and gateways
	cr.assignmentsMutex.Lock()
	cr.assignments = make(map[string]*protocol.CoordinatorAssignment)
	cr.assignmentsMutex.Unlock()

	cr.gatewaysMutex.Lock()
	cr.activeGateways = make(map[string]*GatewayInfo)
	cr.gatewaysMutex.Unlock()

	// Re-register this gateway
	cr.registerGateway(cr.gatewayAddress)
}

// onLostLeadership handles losing leadership
func (cr *CoordinatorRegistry) onLostLeadership() {
	// Clear local state since we're no longer the leader
	cr.assignmentsMutex.Lock()
	cr.assignments = make(map[string]*protocol.CoordinatorAssignment)
	cr.assignmentsMutex.Unlock()
}

// IsLeader returns whether this gateway is the coordinator registry leader
func (cr *CoordinatorRegistry) IsLeader() bool {
	cr.leaderMutex.RLock()
	defer cr.leaderMutex.RUnlock()
	return cr.isLeader
}

// GetLeaderAddress returns the current leader's address
func (cr *CoordinatorRegistry) GetLeaderAddress() string {
	if cr.leaderLock != nil {
		return cr.leaderLock.LockOwner()
	}
	return ""
}

// WaitForLeader waits for a leader to be elected, with timeout
func (cr *CoordinatorRegistry) WaitForLeader(timeout time.Duration) (string, error) {
	// Check if there's already a leader
	if leader := cr.GetLeaderAddress(); leader != "" {
		return leader, nil
	}

	// Check if this instance is the leader
	if cr.IsLeader() {
		return cr.gatewayAddress, nil
	}

	// Wait for leadership change notification
	deadline := time.Now().Add(timeout)
	for {
		select {
		case leader := <-cr.leadershipChange:
			if leader != "" {
				return leader, nil
			}
		case <-time.After(time.Until(deadline)):
			return "", fmt.Errorf("timeout waiting for leader election after %v", timeout)
		}

		// Double-check in case we missed a notification
		if leader := cr.GetLeaderAddress(); leader != "" {
			return leader, nil
		}
		if cr.IsLeader() {
			return cr.gatewayAddress, nil
		}

		if time.Now().After(deadline) {
			break
		}
	}

	return "", fmt.Errorf("timeout waiting for leader election after %v", timeout)
}

// AssignCoordinator assigns a coordinator for a consumer group using a balanced strategy.
// The coordinator is selected deterministically via consistent hashing of the
// consumer group across the set of healthy gateways. This spreads groups evenly
// and avoids hot-spotting on the first requester.
func (cr *CoordinatorRegistry) AssignCoordinator(consumerGroup string, requestingGateway string) (*protocol.CoordinatorAssignment, error) {
	if !cr.IsLeader() {
		return nil, fmt.Errorf("not the coordinator registry leader")
	}

	// First check if requesting gateway is healthy without holding assignments lock
	if !cr.isGatewayHealthy(requestingGateway) {
		return nil, fmt.Errorf("requesting gateway %s is not healthy", requestingGateway)
	}

	// Lock assignments mutex first (consistent lock order: assignments -> gateways)
	cr.assignmentsMutex.Lock()
	defer cr.assignmentsMutex.Unlock()

	// Check if coordinator already assigned
	if existing, exists := cr.assignments[consumerGroup]; exists {
		// Check health with separate lock acquisition to avoid nested locks
		cr.assignmentsMutex.Unlock()
		isHealthy := cr.isGatewayHealthy(existing.CoordinatorAddr)
		cr.assignmentsMutex.Lock()
		
		// Re-check assignment still exists after re-acquiring lock
		if existing, stillExists := cr.assignments[consumerGroup]; stillExists && isHealthy {
			glog.V(2).Infof("Consumer group %s already has coordinator %s", consumerGroup, existing.CoordinatorAddr)
			return existing, nil
		} else if stillExists && !isHealthy {
			glog.V(1).Infof("Existing coordinator %s for group %s is unhealthy, reassigning", existing.CoordinatorAddr, consumerGroup)
			delete(cr.assignments, consumerGroup)
		}
	}

	// Choose a balanced coordinator via consistent hashing across healthy gateways
	// This will acquire gatewaysMutex internally
	cr.assignmentsMutex.Unlock()
	chosenAddr, nodeID, err := cr.chooseCoordinatorAddrForGroup(consumerGroup)
	cr.assignmentsMutex.Lock()
	
	if err != nil {
		return nil, err
	}

	assignment := &protocol.CoordinatorAssignment{
		ConsumerGroup:     consumerGroup,
		CoordinatorAddr:   chosenAddr,
		CoordinatorNodeID: nodeID,
		AssignedAt:        time.Now(),
		LastHeartbeat:     time.Now(),
	}

	cr.assignments[consumerGroup] = assignment

	glog.V(1).Infof("Assigned coordinator %s (node %d) for consumer group %s via consistent hashing", chosenAddr, nodeID, consumerGroup)
	return assignment, nil
}

// GetCoordinator returns the coordinator for a consumer group
func (cr *CoordinatorRegistry) GetCoordinator(consumerGroup string) (*protocol.CoordinatorAssignment, error) {
	if !cr.IsLeader() {
		return nil, fmt.Errorf("not the coordinator registry leader")
	}

	cr.assignmentsMutex.RLock()
	defer cr.assignmentsMutex.RUnlock()

	assignment, exists := cr.assignments[consumerGroup]
	if !exists {
		return nil, fmt.Errorf("no coordinator assigned for consumer group %s", consumerGroup)
	}

	return assignment, nil
}

// RegisterGateway registers a gateway instance
func (cr *CoordinatorRegistry) RegisterGateway(gatewayAddress string) error {
	if !cr.IsLeader() {
		return fmt.Errorf("not the coordinator registry leader")
	}

	cr.registerGateway(gatewayAddress)
	return nil
}

// registerGateway internal method to register a gateway
func (cr *CoordinatorRegistry) registerGateway(gatewayAddress string) {
	cr.gatewaysMutex.Lock()
	defer cr.gatewaysMutex.Unlock()

	nodeID := generateDeterministicNodeID(gatewayAddress)

	cr.activeGateways[gatewayAddress] = &GatewayInfo{
		Address:       gatewayAddress,
		NodeID:        nodeID,
		RegisteredAt:  time.Now(),
		LastHeartbeat: time.Now(),
		IsHealthy:     true,
	}

	glog.V(1).Infof("Registered gateway %s with deterministic node ID %d", gatewayAddress, nodeID)
}

// HeartbeatGateway updates the heartbeat for a gateway
func (cr *CoordinatorRegistry) HeartbeatGateway(gatewayAddress string) error {
	if !cr.IsLeader() {
		return fmt.Errorf("not the coordinator registry leader")
	}

	cr.gatewaysMutex.Lock()
	
	if gateway, exists := cr.activeGateways[gatewayAddress]; exists {
		gateway.LastHeartbeat = time.Now()
		gateway.IsHealthy = true
		cr.gatewaysMutex.Unlock()
		glog.V(3).Infof("Updated heartbeat for gateway %s", gatewayAddress)
	} else {
		// Auto-register unknown gateway - unlock first to avoid double unlock
		cr.gatewaysMutex.Unlock()
		cr.registerGateway(gatewayAddress)
	}

	return nil
}

// isGatewayHealthy checks if a gateway is healthy
func (cr *CoordinatorRegistry) isGatewayHealthy(gatewayAddress string) bool {
	cr.gatewaysMutex.RLock()
	defer cr.gatewaysMutex.RUnlock()

	return cr.isGatewayHealthyUnsafe(gatewayAddress)
}

// isGatewayHealthyUnsafe checks if a gateway is healthy without acquiring locks
// Caller must hold gatewaysMutex.RLock() or gatewaysMutex.Lock()
func (cr *CoordinatorRegistry) isGatewayHealthyUnsafe(gatewayAddress string) bool {
	gateway, exists := cr.activeGateways[gatewayAddress]
	if !exists {
		return false
	}

	return gateway.IsHealthy && time.Since(gateway.LastHeartbeat) < GatewayTimeout
}

// getGatewayNodeID returns the node ID for a gateway
func (cr *CoordinatorRegistry) getGatewayNodeID(gatewayAddress string) int32 {
	cr.gatewaysMutex.RLock()
	defer cr.gatewaysMutex.RUnlock()

	return cr.getGatewayNodeIDUnsafe(gatewayAddress)
}

// getGatewayNodeIDUnsafe returns the node ID for a gateway without acquiring locks
// Caller must hold gatewaysMutex.RLock() or gatewaysMutex.Lock()
func (cr *CoordinatorRegistry) getGatewayNodeIDUnsafe(gatewayAddress string) int32 {
	if gateway, exists := cr.activeGateways[gatewayAddress]; exists {
		return gateway.NodeID
	}

	return 1 // Default node ID
}

// getHealthyGatewaysSorted returns a stable-sorted list of healthy gateway addresses.
func (cr *CoordinatorRegistry) getHealthyGatewaysSorted() []string {
	cr.gatewaysMutex.RLock()
	defer cr.gatewaysMutex.RUnlock()

	addresses := make([]string, 0, len(cr.activeGateways))
	for addr, info := range cr.activeGateways {
		if info.IsHealthy && time.Since(info.LastHeartbeat) < GatewayTimeout {
			addresses = append(addresses, addr)
		}
	}

	sort.Strings(addresses)
	return addresses
}

// chooseCoordinatorAddrForGroup selects a coordinator address using consistent hashing.
func (cr *CoordinatorRegistry) chooseCoordinatorAddrForGroup(consumerGroup string) (string, int32, error) {
	healthy := cr.getHealthyGatewaysSorted()
	if len(healthy) == 0 {
		return "", 0, fmt.Errorf("no healthy gateways available for coordinator assignment")
	}
	idx := hashStringToIndex(consumerGroup, len(healthy))
	addr := healthy[idx]
	return addr, cr.getGatewayNodeID(addr), nil
}

// hashStringToIndex hashes a string to an index in [0, modulo).
func hashStringToIndex(s string, modulo int) int {
	if modulo <= 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return int(h.Sum32() % uint32(modulo))
}

// generateDeterministicNodeID generates a stable node ID based on gateway address
func generateDeterministicNodeID(gatewayAddress string) int32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(gatewayAddress))
	// Use only positive values and avoid 0
	return int32(h.Sum32()&0x7fffffff) + 1
}

// startCleanupLoop starts the cleanup loop for stale assignments and gateways
func (cr *CoordinatorRegistry) startCleanupLoop() {
	cr.wg.Add(1)
	go func() {
		defer cr.wg.Done()

		ticker := time.NewTicker(HeartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-cr.stopChan:
				return
			case <-ticker.C:
				if cr.IsLeader() {
					cr.cleanupStaleEntries()
				}
			}
		}
	}()
}

// cleanupStaleEntries removes stale gateways and assignments
func (cr *CoordinatorRegistry) cleanupStaleEntries() {
	now := time.Now()

	// First, identify stale gateways
	var staleGateways []string
	cr.gatewaysMutex.Lock()
	for addr, gateway := range cr.activeGateways {
		if now.Sub(gateway.LastHeartbeat) > GatewayTimeout {
			staleGateways = append(staleGateways, addr)
		}
	}
	// Remove stale gateways
	for _, addr := range staleGateways {
		glog.V(1).Infof("Removing stale gateway %s", addr)
		delete(cr.activeGateways, addr)
	}
	cr.gatewaysMutex.Unlock()

	// Then, identify assignments with unhealthy coordinators
	var staleAssignments []string
	cr.assignmentsMutex.Lock()
	for group, assignment := range cr.assignments {
		// Check health without nested locks by temporarily releasing assignments lock
		cr.assignmentsMutex.Unlock()
		isHealthy := cr.isGatewayHealthy(assignment.CoordinatorAddr)
		cr.assignmentsMutex.Lock()
		
		// Re-check assignment still exists and is still unhealthy
		if currentAssignment, exists := cr.assignments[group]; exists && 
			currentAssignment.CoordinatorAddr == assignment.CoordinatorAddr && !isHealthy {
			staleAssignments = append(staleAssignments, group)
		}
	}
	// Remove stale assignments
	for _, group := range staleAssignments {
		if assignment, exists := cr.assignments[group]; exists {
			glog.V(1).Infof("Removing assignment for group %s (unhealthy coordinator %s)", group, assignment.CoordinatorAddr)
			delete(cr.assignments, group)
		}
	}
	cr.assignmentsMutex.Unlock()
}

// GetStats returns registry statistics
func (cr *CoordinatorRegistry) GetStats() map[string]interface{} {
	// Read counts separately to avoid holding locks while calling IsLeader()
	cr.gatewaysMutex.RLock()
	gatewayCount := len(cr.activeGateways)
	cr.gatewaysMutex.RUnlock()
	
	cr.assignmentsMutex.RLock()
	assignmentCount := len(cr.assignments)
	cr.assignmentsMutex.RUnlock()

	return map[string]interface{}{
		"is_leader":       cr.IsLeader(),
		"leader_address":  cr.GetLeaderAddress(),
		"active_gateways": gatewayCount,
		"assignments":     assignmentCount,
		"gateway_address": cr.gatewayAddress,
	}
}
