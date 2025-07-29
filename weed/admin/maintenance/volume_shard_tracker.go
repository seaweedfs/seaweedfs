package maintenance

import (
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// VolumeShardInfo represents complete information about a volume or shard
type VolumeShardInfo struct {
	VolumeID     uint32 `json:"volume_id"`
	ShardIndex   int    `json:"shard_index"` // -1 for regular volumes, 0-13 for EC shards
	Collection   string `json:"collection"`
	Size         uint64 `json:"size"`
	Server       string `json:"server"`
	DataCenter   string `json:"data_center"`
	Rack         string `json:"rack"`
	IsECVolume   bool   `json:"is_ec_volume"`
	IsReadOnly   bool   `json:"is_read_only"`
	ReplicaIndex int    `json:"replica_index"` // 0, 1, 2 for replicas
}

// PlacementRule represents replication placement requirements
type PlacementRule struct {
	Collection       string `json:"collection"`
	ReplicaCount     int    `json:"replica_count"`
	DifferentRacks   bool   `json:"different_racks"`
	DifferentDCs     bool   `json:"different_dcs"`
	PreferSameRack   bool   `json:"prefer_same_rack"`    // For EC shards
	MaxShardsPerNode int    `json:"max_shards_per_node"` // For EC shards
}

// NodeCapacityInfo represents node capacity and usage
type NodeCapacityInfo struct {
	NodeID        string `json:"node_id"`
	DataCenter    string `json:"data_center"`
	Rack          string `json:"rack"`
	TotalCapacity uint64 `json:"total_capacity"`
	UsedCapacity  uint64 `json:"used_capacity"`
	FreeCapacity  uint64 `json:"free_capacity"`
	VolumeCount   int    `json:"volume_count"`
	ShardCount    int    `json:"shard_count"`
}

// DestinationPlan represents a planned destination for a volume/shard operation (simplified)
type DestinationPlan struct {
	TargetNode     string   `json:"target_node"`
	TargetRack     string   `json:"target_rack"` // Keep for placement logic
	TargetDC       string   `json:"target_dc"`   // Keep for placement logic
	ExpectedSize   uint64   `json:"expected_size"`
	PlacementScore float64  `json:"placement_score"` // Keep for comparison
	Conflicts      []string `json:"conflicts"`       // Any conflicts or issues found
}

// VolumeShardTracker combines pending operations with master volume data
type VolumeShardTracker struct {
	// Volume and shard information from master
	volumes map[uint32][]*VolumeShardInfo // VolumeID -> list of replicas/shards
	nodes   map[string]*NodeCapacityInfo  // NodeID -> capacity info

	// Placement rules
	placementRules map[string]*PlacementRule // Collection -> rules

	// Integration with pending operations
	pendingOps *PendingOperations

	// Cache and synchronization
	lastUpdated time.Time
	mutex       sync.RWMutex
}

// NewVolumeShardTracker creates a new enhanced tracker
func NewVolumeShardTracker(pendingOps *PendingOperations) *VolumeShardTracker {
	return &VolumeShardTracker{
		volumes:        make(map[uint32][]*VolumeShardInfo),
		nodes:          make(map[string]*NodeCapacityInfo),
		placementRules: make(map[string]*PlacementRule),
		pendingOps:     pendingOps,
	}
}

// UpdateFromMaster updates the tracker with latest data from master
func (vst *VolumeShardTracker) UpdateFromMaster(volumeMetrics []*types.VolumeHealthMetrics) error {
	vst.mutex.Lock()
	defer vst.mutex.Unlock()

	// Clear existing data
	vst.volumes = make(map[uint32][]*VolumeShardInfo)
	vst.nodes = make(map[string]*NodeCapacityInfo)

	// Process volume metrics from master
	for _, metric := range volumeMetrics {
		volumeInfo := &VolumeShardInfo{
			VolumeID:     metric.VolumeID,
			ShardIndex:   -1, // Regular volume
			Collection:   metric.Collection,
			Size:         metric.Size,
			Server:       metric.Server,
			IsECVolume:   metric.IsECVolume,
			IsReadOnly:   metric.IsReadOnly,
			ReplicaIndex: 0, // Will be determined by counting replicas
		}

		// Parse server info to extract data center and rack
		// Format: "server:port" or "dc:rack:server:port"
		vst.parseServerLocation(volumeInfo)

		// Add to volume tracking
		vst.volumes[metric.VolumeID] = append(vst.volumes[metric.VolumeID], volumeInfo)

		// Update node capacity info
		vst.updateNodeCapacity(volumeInfo)
	}

	// Set replica indexes based on order
	for volumeID, replicas := range vst.volumes {
		for i, replica := range replicas {
			replica.ReplicaIndex = i
		}
		glog.V(3).Infof("Volume %d has %d replicas", volumeID, len(replicas))
	}

	vst.lastUpdated = time.Now()
	glog.V(1).Infof("Updated volume/shard tracker with %d volumes across %d nodes",
		len(vst.volumes), len(vst.nodes))

	return nil
}

// parseServerLocation sets default location info (simplified)
func (vst *VolumeShardTracker) parseServerLocation(info *VolumeShardInfo) {
	// Use simple defaults - can be enhanced later if needed
	info.DataCenter = "default"
	info.Rack = "default"
}

// updateNodeCapacity updates capacity information for a node
func (vst *VolumeShardTracker) updateNodeCapacity(info *VolumeShardInfo) {
	nodeInfo, exists := vst.nodes[info.Server]
	if !exists {
		nodeInfo = &NodeCapacityInfo{
			NodeID:        info.Server,
			DataCenter:    info.DataCenter,
			Rack:          info.Rack,
			TotalCapacity: 100 * 1024 * 1024 * 1024, // Default 100GB - should come from master
			UsedCapacity:  0,
			VolumeCount:   0,
			ShardCount:    0,
		}
		vst.nodes[info.Server] = nodeInfo
	}

	// Update usage
	nodeInfo.UsedCapacity += info.Size
	if info.IsECVolume && info.ShardIndex >= 0 {
		nodeInfo.ShardCount++
	} else {
		nodeInfo.VolumeCount++
	}

	// Calculate free capacity including pending operations
	vst.updateNodeFreeCapacity(nodeInfo)
}

// updateNodeFreeCapacity recalculates free capacity including pending operations
func (vst *VolumeShardTracker) updateNodeFreeCapacity(nodeInfo *NodeCapacityInfo) {
	incoming, outgoing := vst.pendingOps.GetPendingCapacityImpactForNode(nodeInfo.NodeID)
	nodeInfo.FreeCapacity = nodeInfo.TotalCapacity - nodeInfo.UsedCapacity - incoming + outgoing
}

// setPlacementRule sets replication placement rule for a collection (test helper)
func (vst *VolumeShardTracker) setPlacementRule(collection string, rule *PlacementRule) {
	vst.mutex.Lock()
	defer vst.mutex.Unlock()

	vst.placementRules[collection] = rule
	glog.V(2).Infof("Set placement rule for collection %s: replicas=%d, different_racks=%v",
		collection, rule.ReplicaCount, rule.DifferentRacks)
}

// PlanDestinationForVolume plans optimal destination for volume operations
func (vst *VolumeShardTracker) PlanDestinationForVolume(volumeID uint32, operation PendingOperationType, sourceNode string) (*DestinationPlan, error) {
	vst.mutex.RLock()
	defer vst.mutex.RUnlock()

	volumeReplicas, exists := vst.volumes[volumeID]
	if !exists || len(volumeReplicas) == 0 {
		return nil, fmt.Errorf("volume %d not found", volumeID)
	}

	// Get volume info and placement rules
	volumeInfo := volumeReplicas[0] // Use first replica for basic info
	placementRule := vst.getPlacementRule(volumeInfo.Collection)

	switch operation {
	case OpTypeVolumeMove, OpTypeVolumeBalance:
		return vst.planVolumeMoveDestination(volumeID, volumeReplicas, sourceNode, placementRule)
	case OpTypeReplication:
		return vst.planReplicationDestination(volumeID, volumeReplicas, placementRule)
	case OpTypeErasureCoding:
		return vst.planErasureCodingDestination(volumeID, volumeReplicas, sourceNode, placementRule)
	default:
		return &DestinationPlan{
			TargetNode: sourceNode, // Stay on same node for operations like vacuum
			Conflicts:  []string{},
		}, nil
	}
}

// planVolumeMoveDestination plans destination for volume move/balance operations (internal)
func (vst *VolumeShardTracker) planVolumeMoveDestination(volumeID uint32, replicas []*VolumeShardInfo, sourceNode string, rule *PlacementRule) (*DestinationPlan, error) {
	volumeSize := replicas[0].Size

	// Get all candidate nodes (excluding source and nodes with existing replicas)
	excludeNodes := make(map[string]bool)
	excludeNodes[sourceNode] = true

	usedRacks := make(map[string]bool)
	usedDCs := make(map[string]bool)

	for _, replica := range replicas {
		if replica.Server != sourceNode {
			excludeNodes[replica.Server] = true
			usedRacks[replica.Rack] = true
			usedDCs[replica.DataCenter] = true
		}
	}

	// Find best destination
	var bestPlan *DestinationPlan
	bestScore := -1.0

	for nodeID, nodeInfo := range vst.nodes {
		if excludeNodes[nodeID] {
			continue
		}

		plan := &DestinationPlan{
			TargetNode:   nodeID,
			TargetRack:   nodeInfo.Rack,
			TargetDC:     nodeInfo.DataCenter,
			ExpectedSize: volumeSize,
			Conflicts:    []string{},
		}

		// Check capacity
		if nodeInfo.FreeCapacity < volumeSize {
			plan.Conflicts = append(plan.Conflicts, "insufficient_capacity")
			continue
		}

		// Check placement rules
		score := vst.calculatePlacementScore(plan, usedRacks, usedDCs, rule)
		plan.PlacementScore = score

		if score > bestScore {
			bestScore = score
			bestPlan = plan
		}
	}

	if bestPlan == nil {
		return nil, fmt.Errorf("no suitable destination found for volume %d", volumeID)
	}

	return bestPlan, nil
}

// planReplicationDestination plans destination for adding new replica
func (vst *VolumeShardTracker) planReplicationDestination(volumeID uint32, replicas []*VolumeShardInfo, rule *PlacementRule) (*DestinationPlan, error) {
	volumeSize := replicas[0].Size

	// Check if we need more replicas
	if len(replicas) >= rule.ReplicaCount {
		return nil, fmt.Errorf("volume %d already has %d replicas (max: %d)", volumeID, len(replicas), rule.ReplicaCount)
	}

	// Get nodes to exclude and placement constraints
	excludeNodes := make(map[string]bool)
	usedRacks := make(map[string]bool)
	usedDCs := make(map[string]bool)

	for _, replica := range replicas {
		excludeNodes[replica.Server] = true
		usedRacks[replica.Rack] = true
		usedDCs[replica.DataCenter] = true
	}

	// Find best location for new replica
	var bestPlan *DestinationPlan
	bestScore := -1.0

	for nodeID, nodeInfo := range vst.nodes {
		if excludeNodes[nodeID] {
			continue
		}

		plan := &DestinationPlan{
			TargetNode:   nodeID,
			TargetRack:   nodeInfo.Rack,
			TargetDC:     nodeInfo.DataCenter,
			ExpectedSize: volumeSize,
			Conflicts:    []string{},
		}

		// Check capacity
		if nodeInfo.FreeCapacity < volumeSize {
			plan.Conflicts = append(plan.Conflicts, "insufficient_capacity")
			continue
		}

		// Calculate placement score
		score := vst.calculatePlacementScore(plan, usedRacks, usedDCs, rule)
		plan.PlacementScore = score

		if score > bestScore {
			bestScore = score
			bestPlan = plan
		}
	}

	if bestPlan == nil {
		return nil, fmt.Errorf("no suitable destination found for replicating volume %d", volumeID)
	}

	return bestPlan, nil
}

// planErasureCodingDestination plans destination for EC encoding (simplified)
func (vst *VolumeShardTracker) planErasureCodingDestination(volumeID uint32, replicas []*VolumeShardInfo, sourceNode string, rule *PlacementRule) (*DestinationPlan, error) {
	volumeSize := replicas[0].Size

	// EC uses fixed shards from erasure_coding package
	shardSize := volumeSize / uint64(erasure_coding.DataShardsCount)
	glog.V(1).Infof("EC primary destination planning for volume %d: volume_size=%d bytes, shard_size=%d bytes, source_node=%s",
		volumeID, volumeSize, shardSize, sourceNode)

	// Simple check: ensure we have enough nodes with sufficient capacity
	availableNodes := 0
	var bestNode *NodeCapacityInfo
	var insufficientNodes []string

	for _, nodeInfo := range vst.nodes {
		// Skip source node
		if nodeInfo.NodeID == sourceNode {
			glog.V(2).Infof("EC primary destination planning for volume %d: skipping source node %s", volumeID, nodeInfo.NodeID)
			continue
		}

		if nodeInfo.FreeCapacity >= shardSize {
			availableNodes++
			if bestNode == nil || nodeInfo.FreeCapacity > bestNode.FreeCapacity {
				bestNode = nodeInfo
			}
			glog.V(2).Infof("EC primary destination planning for volume %d: node %s is suitable (free: %d bytes)",
				volumeID, nodeInfo.NodeID, nodeInfo.FreeCapacity)
		} else {
			insufficientNodes = append(insufficientNodes, fmt.Sprintf("%s(free:%d)", nodeInfo.NodeID, nodeInfo.FreeCapacity))
			glog.V(2).Infof("EC primary destination planning for volume %d: node %s has insufficient capacity (free: %d bytes, need: %d bytes)",
				volumeID, nodeInfo.NodeID, nodeInfo.FreeCapacity, shardSize)
		}
	}

	if availableNodes < erasure_coding.TotalShardsCount {
		glog.Warningf("EC primary destination planning failed for volume %d: insufficient nodes for EC encoding (need %d, have %d). "+
			"Insufficient nodes: %v", volumeID, erasure_coding.TotalShardsCount, availableNodes, insufficientNodes)
		return nil, fmt.Errorf("insufficient nodes for EC encoding: need %d, have %d", erasure_coding.TotalShardsCount, availableNodes)
	}

	if bestNode == nil {
		glog.Warningf("EC primary destination planning failed for volume %d: no best node found despite having %d available nodes",
			volumeID, availableNodes)
		return nil, fmt.Errorf("no suitable primary destination found for EC encoding")
	}

	glog.Infof("EC primary destination planning succeeded for volume %d: selected primary node %s (free: %d bytes, shard_size: %d bytes)",
		volumeID, bestNode.NodeID, bestNode.FreeCapacity, shardSize)

	return &DestinationPlan{
		TargetNode:     bestNode.NodeID,
		TargetRack:     bestNode.Rack,
		TargetDC:       bestNode.DataCenter,
		ExpectedSize:   shardSize,
		PlacementScore: 1.0,
		Conflicts:      []string{},
	}, nil
}

// calculatePlacementScore calculates how well a destination fits placement rules (simplified)
func (vst *VolumeShardTracker) calculatePlacementScore(plan *DestinationPlan, usedRacks, usedDCs map[string]bool, rule *PlacementRule) float64 {
	// Simple binary scoring: good placement gets 1.0, violations get 0.0
	score := 1.0

	// Check DC constraint (highest priority)
	if rule.DifferentDCs && usedDCs[plan.TargetDC] {
		plan.Conflicts = append(plan.Conflicts, "same_dc_as_existing_replica")
		score = 0.0 // Immediate disqualification
	}

	// Check rack constraint (if DC constraint passed)
	if score > 0 && rule.DifferentRacks && usedRacks[plan.TargetRack] {
		plan.Conflicts = append(plan.Conflicts, "same_rack_as_existing_replica")
		score = 0.5 // Reduced but not disqualified
	}

	// Add small capacity preference (0.1 bonus for more free space)
	if score > 0 {
		if nodeInfo, exists := vst.nodes[plan.TargetNode]; exists && nodeInfo.TotalCapacity > 0 {
			capacityRatio := float64(nodeInfo.FreeCapacity) / float64(nodeInfo.TotalCapacity)
			score += capacityRatio * 0.1 // Small bonus for more free space
		}
	}

	return score
}

// getPlacementRule returns placement rule for collection or default
func (vst *VolumeShardTracker) getPlacementRule(collection string) *PlacementRule {
	if rule, exists := vst.placementRules[collection]; exists {
		return rule
	}

	// Default placement rule
	return &PlacementRule{
		Collection:       collection,
		ReplicaCount:     3,
		DifferentRacks:   true,
		DifferentDCs:     false,
		PreferSameRack:   false,
		MaxShardsPerNode: 2,
	}
}

// getVolumeInfo returns complete information about a volume (test helper)
func (vst *VolumeShardTracker) getVolumeInfo(volumeID uint32) ([]*VolumeShardInfo, error) {
	vst.mutex.RLock()
	defer vst.mutex.RUnlock()

	if replicas, exists := vst.volumes[volumeID]; exists {
		return replicas, nil
	}

	return nil, fmt.Errorf("volume %d not found", volumeID)
}

// getNodeInfo returns information about a node (test helper)
func (vst *VolumeShardTracker) getNodeInfo(nodeID string) (*NodeCapacityInfo, error) {
	vst.mutex.Lock() // Use write lock to allow capacity update
	defer vst.mutex.Unlock()

	if nodeInfo, exists := vst.nodes[nodeID]; exists {
		// Update free capacity to include latest pending operations
		vst.updateNodeFreeCapacity(nodeInfo)
		return nodeInfo, nil
	}

	return nil, fmt.Errorf("node %s not found", nodeID)
}

// getClusterStats returns overall cluster statistics (test helper)
func (vst *VolumeShardTracker) getClusterStats() ClusterStats {
	vst.mutex.RLock()
	defer vst.mutex.RUnlock()

	stats := ClusterStats{
		TotalNodes:   len(vst.nodes),
		TotalVolumes: len(vst.volumes),
		LastUpdated:  vst.lastUpdated,
	}

	var totalCapacity, usedCapacity uint64
	for _, nodeInfo := range vst.nodes {
		totalCapacity += nodeInfo.TotalCapacity
		usedCapacity += nodeInfo.UsedCapacity
		stats.TotalShards += nodeInfo.ShardCount
	}

	stats.TotalCapacity = totalCapacity
	stats.UsedCapacity = usedCapacity
	stats.FreeCapacity = totalCapacity - usedCapacity

	if totalCapacity > 0 {
		stats.UsageRatio = float64(usedCapacity) / float64(totalCapacity)
	}

	return stats
}

// ClusterStats represents overall cluster statistics
type ClusterStats struct {
	TotalNodes    int       `json:"total_nodes"`
	TotalVolumes  int       `json:"total_volumes"`
	TotalShards   int       `json:"total_shards"`
	TotalCapacity uint64    `json:"total_capacity"`
	UsedCapacity  uint64    `json:"used_capacity"`
	FreeCapacity  uint64    `json:"free_capacity"`
	UsageRatio    float64   `json:"usage_ratio"`
	LastUpdated   time.Time `json:"last_updated"`
}
