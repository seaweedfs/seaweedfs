package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer_client"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/protocol"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
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

	// No in-memory assignments - read/write directly to filer
	// assignmentsMutex still needed for coordinating file operations
	assignmentsMutex sync.RWMutex

	// Gateway registry
	activeGateways map[string]*GatewayInfo // gatewayAddress -> info
	gatewaysMutex  sync.RWMutex

	// Configuration
	gatewayAddress        string
	lockClient            *cluster.LockClient
	filerClientAccessor   *filer_client.FilerClientAccessor
	filerDiscoveryService *filer_client.FilerDiscoveryService

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

	// Filer paths for coordinator assignment persistence
	CoordinatorAssignmentsDir = "/topics/kafka/.meta/coordinators"
)

// NewCoordinatorRegistry creates a new coordinator registry
func NewCoordinatorRegistry(gatewayAddress string, masters []pb.ServerAddress, grpcDialOption grpc.DialOption) *CoordinatorRegistry {
	// Create filer discovery service that will periodically refresh filers from all masters
	filerDiscoveryService := filer_client.NewFilerDiscoveryService(masters, grpcDialOption)

	// Manually discover filers from each master until we find one
	var seedFiler pb.ServerAddress
	for _, master := range masters {
		// Use the same discovery logic as filer_discovery.go
		grpcAddr := master.ToGrpcAddress()
		conn, err := grpc.NewClient(grpcAddr, grpcDialOption)
		if err != nil {
			continue
		}

		client := master_pb.NewSeaweedClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := client.ListClusterNodes(ctx, &master_pb.ListClusterNodesRequest{
			ClientType: cluster.FilerType,
		})
		cancel()
		conn.Close()

		if err == nil && len(resp.ClusterNodes) > 0 {
			// Found a filer - use its HTTP address (WithFilerClient will convert to gRPC automatically)
			seedFiler = pb.ServerAddress(resp.ClusterNodes[0].Address)
			glog.V(1).Infof("Using filer %s as seed for distributed locking (discovered from master %s)", seedFiler, master)
			break
		}
	}

	lockClient := cluster.NewLockClient(grpcDialOption, seedFiler)

	registry := &CoordinatorRegistry{
		activeGateways:        make(map[string]*GatewayInfo),
		gatewayAddress:        gatewayAddress,
		lockClient:            lockClient,
		stopChan:              make(chan struct{}),
		leadershipChange:      make(chan string, 10), // Buffered channel for leadership notifications
		filerDiscoveryService: filerDiscoveryService,
	}

	// Create filer client accessor that uses dynamic filer discovery
	registry.filerClientAccessor = &filer_client.FilerClientAccessor{
		GetGrpcDialOption: func() grpc.DialOption {
			return grpcDialOption
		},
		GetFilers: func() []pb.ServerAddress {
			return registry.filerDiscoveryService.GetFilers()
		},
	}

	return registry
}

// Start begins the coordinator registry operations
func (cr *CoordinatorRegistry) Start() error {
	glog.V(1).Infof("Starting coordinator registry for gateway %s", cr.gatewayAddress)

	// Start filer discovery service first
	if err := cr.filerDiscoveryService.Start(); err != nil {
		return fmt.Errorf("failed to start filer discovery service: %w", err)
	}

	// Start leader election
	cr.startLeaderElection()

	// Start heartbeat loop to keep this gateway healthy
	cr.startHeartbeatLoop()

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

	// Stop filer discovery service
	if err := cr.filerDiscoveryService.Stop(); err != nil {
		glog.Warningf("Failed to stop filer discovery service: %v", err)
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
			lock_manager.RenewInterval,
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
	// Assignments are now read directly from files - no need to load into memory
	glog.V(1).Info("Leader election complete - coordinator assignments will be read from filer as needed")

	// Clear gateway registry since it's ephemeral (gateways need to re-register)
	cr.gatewaysMutex.Lock()
	cr.activeGateways = make(map[string]*GatewayInfo)
	cr.gatewaysMutex.Unlock()

	// Re-register this gateway
	cr.registerGateway(cr.gatewayAddress)
}

// onLostLeadership handles losing leadership
func (cr *CoordinatorRegistry) onLostLeadership() {
	// No in-memory assignments to clear - assignments are stored in filer
	glog.V(1).Info("Lost leadership - no longer managing coordinator assignments")
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

	// Lock assignments mutex to coordinate file operations
	cr.assignmentsMutex.Lock()
	defer cr.assignmentsMutex.Unlock()

	// Check if coordinator already assigned by trying to load from file
	existing, err := cr.loadCoordinatorAssignment(consumerGroup)
	if err == nil && existing != nil {
		// Assignment exists, check if coordinator is still healthy
		if cr.isGatewayHealthy(existing.CoordinatorAddr) {
			glog.V(2).Infof("Consumer group %s already has healthy coordinator %s", consumerGroup, existing.CoordinatorAddr)
			return existing, nil
		} else {
			glog.V(1).Infof("Existing coordinator %s for group %s is unhealthy, reassigning", existing.CoordinatorAddr, consumerGroup)
			// Delete the existing assignment file
			if delErr := cr.deleteCoordinatorAssignment(consumerGroup); delErr != nil {
				glog.Warningf("Failed to delete stale assignment for group %s: %v", consumerGroup, delErr)
			}
		}
	}

	// Choose a balanced coordinator via consistent hashing across healthy gateways
	chosenAddr, nodeID, err := cr.chooseCoordinatorAddrForGroup(consumerGroup)
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

	// Persist the new assignment to individual file
	if err := cr.saveCoordinatorAssignment(consumerGroup, assignment); err != nil {
		return nil, fmt.Errorf("failed to persist coordinator assignment for group %s: %w", consumerGroup, err)
	}

	glog.V(1).Infof("Assigned coordinator %s (node %d) for consumer group %s via consistent hashing", chosenAddr, nodeID, consumerGroup)
	return assignment, nil
}

// GetCoordinator returns the coordinator for a consumer group
func (cr *CoordinatorRegistry) GetCoordinator(consumerGroup string) (*protocol.CoordinatorAssignment, error) {
	if !cr.IsLeader() {
		return nil, fmt.Errorf("not the coordinator registry leader")
	}

	// Load assignment directly from file
	assignment, err := cr.loadCoordinatorAssignment(consumerGroup)
	if err != nil {
		return nil, fmt.Errorf("no coordinator assigned for consumer group %s: %w", consumerGroup, err)
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

// startHeartbeatLoop starts the heartbeat loop for this gateway
func (cr *CoordinatorRegistry) startHeartbeatLoop() {
	cr.wg.Add(1)
	go func() {
		defer cr.wg.Done()

		ticker := time.NewTicker(HeartbeatInterval / 2) // Send heartbeats more frequently than timeout
		defer ticker.Stop()

		for {
			select {
			case <-cr.stopChan:
				return
			case <-ticker.C:
				if cr.IsLeader() {
					// Send heartbeat for this gateway to keep it healthy
					if err := cr.HeartbeatGateway(cr.gatewayAddress); err != nil {
						glog.V(2).Infof("Failed to send heartbeat for gateway %s: %v", cr.gatewayAddress, err)
					}
				}
			}
		}
	}()
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

	// Then, identify assignments with unhealthy coordinators and reassign them
	cr.assignmentsMutex.Lock()
	defer cr.assignmentsMutex.Unlock()

	// Get list of all consumer groups with assignments
	consumerGroups, err := cr.listAllCoordinatorAssignments()
	if err != nil {
		glog.Warningf("Failed to list coordinator assignments during cleanup: %v", err)
		return
	}

	for _, group := range consumerGroups {
		// Load assignment from file
		assignment, err := cr.loadCoordinatorAssignment(group)
		if err != nil {
			glog.Warningf("Failed to load assignment for group %s during cleanup: %v", group, err)
			continue
		}

		// Check if coordinator is healthy
		if !cr.isGatewayHealthy(assignment.CoordinatorAddr) {
			glog.V(1).Infof("Coordinator %s for group %s is unhealthy, attempting reassignment", assignment.CoordinatorAddr, group)

			// Try to reassign to a healthy gateway
			newAddr, newNodeID, err := cr.chooseCoordinatorAddrForGroup(group)
			if err != nil {
				// No healthy gateways available, remove the assignment for now
				glog.Warningf("No healthy gateways available for reassignment of group %s, removing assignment", group)
				if delErr := cr.deleteCoordinatorAssignment(group); delErr != nil {
					glog.Warningf("Failed to delete assignment for group %s: %v", group, delErr)
				}
			} else if newAddr != assignment.CoordinatorAddr {
				// Reassign to the new healthy coordinator
				newAssignment := &protocol.CoordinatorAssignment{
					ConsumerGroup:     group,
					CoordinatorAddr:   newAddr,
					CoordinatorNodeID: newNodeID,
					AssignedAt:        time.Now(),
					LastHeartbeat:     time.Now(),
				}

				// Save new assignment to file
				if saveErr := cr.saveCoordinatorAssignment(group, newAssignment); saveErr != nil {
					glog.Warningf("Failed to save reassignment for group %s: %v", group, saveErr)
				} else {
					glog.V(0).Infof("Reassigned coordinator for group %s from unhealthy %s to healthy %s",
						group, assignment.CoordinatorAddr, newAddr)
				}
			}
		}
	}
}

// GetStats returns registry statistics
func (cr *CoordinatorRegistry) GetStats() map[string]interface{} {
	// Read counts separately to avoid holding locks while calling IsLeader()
	cr.gatewaysMutex.RLock()
	gatewayCount := len(cr.activeGateways)
	cr.gatewaysMutex.RUnlock()

	// Count assignments from files
	var assignmentCount int
	if cr.IsLeader() {
		consumerGroups, err := cr.listAllCoordinatorAssignments()
		if err != nil {
			glog.Warningf("Failed to count coordinator assignments: %v", err)
			assignmentCount = -1 // Indicate error
		} else {
			assignmentCount = len(consumerGroups)
		}
	} else {
		assignmentCount = 0 // Non-leader doesn't track assignments
	}

	return map[string]interface{}{
		"is_leader":       cr.IsLeader(),
		"leader_address":  cr.GetLeaderAddress(),
		"active_gateways": gatewayCount,
		"assignments":     assignmentCount,
		"gateway_address": cr.gatewayAddress,
	}
}

// Persistence methods for coordinator assignments

// saveCoordinatorAssignment saves a single coordinator assignment to its individual file
func (cr *CoordinatorRegistry) saveCoordinatorAssignment(consumerGroup string, assignment *protocol.CoordinatorAssignment) error {
	if !cr.IsLeader() {
		// Only leader should save assignments
		return nil
	}

	return cr.filerClientAccessor.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// Convert assignment to JSON
		assignmentData, err := json.Marshal(assignment)
		if err != nil {
			return fmt.Errorf("failed to marshal assignment for group %s: %w", consumerGroup, err)
		}

		// Save to individual file: /topics/kafka/.meta/coordinators/<consumer-group>_assignments.json
		fileName := fmt.Sprintf("%s_assignments.json", consumerGroup)
		return filer.SaveInsideFiler(client, CoordinatorAssignmentsDir, fileName, assignmentData)
	})
}

// loadCoordinatorAssignment loads a single coordinator assignment from its individual file
func (cr *CoordinatorRegistry) loadCoordinatorAssignment(consumerGroup string) (*protocol.CoordinatorAssignment, error) {
	return cr.loadCoordinatorAssignmentWithClient(consumerGroup, cr.filerClientAccessor)
}

// loadCoordinatorAssignmentWithClient loads a single coordinator assignment using provided client
func (cr *CoordinatorRegistry) loadCoordinatorAssignmentWithClient(consumerGroup string, clientAccessor *filer_client.FilerClientAccessor) (*protocol.CoordinatorAssignment, error) {
	var assignment *protocol.CoordinatorAssignment

	err := clientAccessor.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// Load from individual file: /topics/kafka/.meta/coordinators/<consumer-group>_assignments.json
		fileName := fmt.Sprintf("%s_assignments.json", consumerGroup)
		data, err := filer.ReadInsideFiler(client, CoordinatorAssignmentsDir, fileName)
		if err != nil {
			return fmt.Errorf("assignment file not found for group %s: %w", consumerGroup, err)
		}

		// Parse JSON
		if err := json.Unmarshal(data, &assignment); err != nil {
			return fmt.Errorf("failed to unmarshal assignment for group %s: %w", consumerGroup, err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return assignment, nil
}

// listAllCoordinatorAssignments lists all coordinator assignment files
func (cr *CoordinatorRegistry) listAllCoordinatorAssignments() ([]string, error) {
	var consumerGroups []string

	err := cr.filerClientAccessor.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.ListEntriesRequest{
			Directory: CoordinatorAssignmentsDir,
		}

		stream, streamErr := client.ListEntries(context.Background(), request)
		if streamErr != nil {
			// Directory might not exist yet, that's okay
			return nil
		}

		for {
			resp, recvErr := stream.Recv()
			if recvErr != nil {
				if recvErr == io.EOF {
					break
				}
				return fmt.Errorf("failed to receive entry: %v", recvErr)
			}

			// Only include assignment files (ending with _assignments.json)
			if resp.Entry != nil && !resp.Entry.IsDirectory &&
				strings.HasSuffix(resp.Entry.Name, "_assignments.json") {
				// Extract consumer group name by removing _assignments.json suffix
				consumerGroup := strings.TrimSuffix(resp.Entry.Name, "_assignments.json")
				consumerGroups = append(consumerGroups, consumerGroup)
			}
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to list coordinator assignments: %w", err)
	}

	return consumerGroups, nil
}

// deleteCoordinatorAssignment removes a coordinator assignment file
func (cr *CoordinatorRegistry) deleteCoordinatorAssignment(consumerGroup string) error {
	if !cr.IsLeader() {
		return nil
	}

	return cr.filerClientAccessor.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		fileName := fmt.Sprintf("%s_assignments.json", consumerGroup)
		filePath := fmt.Sprintf("%s/%s", CoordinatorAssignmentsDir, fileName)

		_, err := client.DeleteEntry(context.Background(), &filer_pb.DeleteEntryRequest{
			Directory: CoordinatorAssignmentsDir,
			Name:      fileName,
		})

		if err != nil {
			return fmt.Errorf("failed to delete assignment file %s: %w", filePath, err)
		}

		return nil
	})
}

// ReassignCoordinator manually reassigns a coordinator for a consumer group
// This can be called when a coordinator gateway becomes unavailable
func (cr *CoordinatorRegistry) ReassignCoordinator(consumerGroup string) (*protocol.CoordinatorAssignment, error) {
	if !cr.IsLeader() {
		return nil, fmt.Errorf("not the coordinator registry leader")
	}

	cr.assignmentsMutex.Lock()
	defer cr.assignmentsMutex.Unlock()

	// Check if assignment exists by loading from file
	existing, err := cr.loadCoordinatorAssignment(consumerGroup)
	if err != nil {
		return nil, fmt.Errorf("no existing assignment for consumer group %s: %w", consumerGroup, err)
	}

	// Choose a new coordinator
	newAddr, newNodeID, err := cr.chooseCoordinatorAddrForGroup(consumerGroup)
	if err != nil {
		return nil, fmt.Errorf("failed to choose new coordinator: %w", err)
	}

	// Create new assignment
	newAssignment := &protocol.CoordinatorAssignment{
		ConsumerGroup:     consumerGroup,
		CoordinatorAddr:   newAddr,
		CoordinatorNodeID: newNodeID,
		AssignedAt:        time.Now(),
		LastHeartbeat:     time.Now(),
	}

	// Persist the new assignment to individual file
	if err := cr.saveCoordinatorAssignment(consumerGroup, newAssignment); err != nil {
		return nil, fmt.Errorf("failed to persist coordinator reassignment for group %s: %w", consumerGroup, err)
	}

	glog.V(0).Infof("Manually reassigned coordinator for group %s from %s to %s",
		consumerGroup, existing.CoordinatorAddr, newAddr)

	return newAssignment, nil
}
