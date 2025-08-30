package ml

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
)

// DistributedTrainingRole represents different roles in distributed training
type DistributedTrainingRole int

const (
	RoleUnknown DistributedTrainingRole = iota
	RoleParameterServer                   // Parameter server in PS architecture
	RoleWorker                           // Worker node in distributed training
	RoleChief                            // Chief worker (coordinator)
	RoleEvaluator                        // Evaluation worker
	RoleAllReduce                        // All-reduce participant (Horovod style)
	RoleMaster                           // Master node for coordination
)

// DistributedTrainingTopology represents the training cluster topology
type DistributedTrainingTopology int

const (
	TopologyUnknown DistributedTrainingTopology = iota
	TopologyParameterServer                       // Parameter Server + Workers
	TopologyAllReduce                             // All-Reduce (Ring, Tree, etc.)
	TopologyHierarchical                          // Hierarchical (multi-level)
	TopologyFederatedLearning                     // Federated learning setup
	TopologyDataParallel                          // Data parallel training
	TopologyModelParallel                         // Model parallel training
)

// ClusterNode represents a node in the distributed training cluster
type ClusterNode struct {
	sync.RWMutex
	
	// Node identity
	NodeID        string                    `json:"node_id"`
	Address       pb.ServerAddress          `json:"address"`
	Role          DistributedTrainingRole   `json:"role"`
	Zone          string                    `json:"zone"`          // Availability zone or rack
	Region        string                    `json:"region"`        // Geographic region
	
	// Hardware capabilities
	GPUCount      int                       `json:"gpu_count"`
	GPUMemory     uint64                    `json:"gpu_memory"`    // Total GPU memory in bytes
	SystemMemory  uint64                    `json:"system_memory"` // Total system memory in bytes
	NetworkBandwidth uint64                 `json:"network_bandwidth"` // Network bandwidth in bytes/sec
	StorageBandwidth uint64                 `json:"storage_bandwidth"` // Storage bandwidth in bytes/sec
	
	// Current state
	Status        NodeStatus                `json:"status"`
	LastHeartbeat time.Time                 `json:"last_heartbeat"`
	LoadAverage   float64                   `json:"load_average"`
	
	// Training state
	CurrentEpoch  int                       `json:"current_epoch"`
	BatchesProcessed int64                  `json:"batches_processed"`
	TrainingSpeed float64                   `json:"training_speed"` // Batches per second
	
	// Data access patterns
	DataLocality  map[string]float64        `json:"data_locality"`  // Dataset -> locality score (0-1)
	CacheHitRate  float64                   `json:"cache_hit_rate"`
	PrefetchAccuracy float64                `json:"prefetch_accuracy"`
}

// NodeStatus represents the status of a cluster node
type NodeStatus int

const (
	NodeStatusUnknown NodeStatus = iota
	NodeStatusHealthy
	NodeStatusBusy
	NodeStatusOverloaded
	NodeStatusUnhealthy
	NodeStatusOffline
)

// DistributedTrainingJob represents a distributed training job
type DistributedTrainingJob struct {
	sync.RWMutex
	
	// Job identity
	JobID         string                       `json:"job_id"`
	JobName       string                       `json:"job_name"`
	Topology      DistributedTrainingTopology  `json:"topology"`
	
	// Training configuration
	TotalEpochs   int                          `json:"total_epochs"`
	BatchSize     int                          `json:"batch_size"`
	LearningRate  float64                      `json:"learning_rate"`
	
	// Dataset information
	DatasetPath   string                       `json:"dataset_path"`
	DatasetSize   uint64                       `json:"dataset_size"`
	ShardStrategy DataShardStrategy            `json:"shard_strategy"`
	
	// Cluster state
	Nodes         map[string]*ClusterNode      `json:"nodes"`
	MasterNode    string                       `json:"master_node"`
	
	// Training progress
	CurrentEpoch  int                          `json:"current_epoch"`
	StartTime     time.Time                    `json:"start_time"`
	EstimatedETA  time.Time                    `json:"estimated_eta"`
	
	// Coordination state
	SynchronizationBarriers map[int]time.Time   `json:"sync_barriers"` // Epoch -> sync time
	StragglerNodes         []string             `json:"straggler_nodes"`
	FailedNodes           []string             `json:"failed_nodes"`
}

// DataShardStrategy represents how data is sharded across nodes
type DataShardStrategy int

const (
	ShardStrategyUnknown DataShardStrategy = iota
	ShardStrategyRoundRobin                 // Round-robin assignment
	ShardStrategyLocalityAware              // Locality-aware sharding
	ShardStrategyHashBased                  // Hash-based sharding
	ShardStrategyRandom                     // Random sharding
	ShardStrategyCustom                     // Custom sharding logic
)

// DistributedCoordinator manages coordination for distributed training
type DistributedCoordinator struct {
	sync.RWMutex
	
	// Configuration
	enabled           bool                     // Whether distributed coordination is enabled
	nodeID            string                   // This node's ID
	discoveryInterval time.Duration           // How often to discover other nodes
	heartbeatInterval time.Duration           // Heartbeat interval
	nodeTimeout       time.Duration           // When to consider a node offline
	
	// Cluster state
	localNode         *ClusterNode             // This node's information
	remoteNodes       map[string]*ClusterNode  // Remote nodes
	activeJobs        map[string]*DistributedTrainingJob // Active training jobs
	
	// Data coordination
	dataShards        map[string]*DataShard    // Data shards managed by this node
	shardAssignments  map[string][]string      // Job -> list of responsible nodes
	
	// Communication
	messageHandlers   map[string]MessageHandler // Message type -> handler
	
	// Background tasks
	ctx               context.Context
	cancel            context.CancelFunc
	
	// Metrics
	totalJobs         int64                    // Total jobs seen
	activeNodes       int64                    // Currently active nodes
	coordinationEvents int64                  // Total coordination events
	synchronizationLatency time.Duration      // Average sync latency
}

// DataShard represents a shard of training data
type DataShard struct {
	ShardID       string    `json:"shard_id"`
	JobID         string    `json:"job_id"`
	FilePath      string    `json:"file_path"`
	StartOffset   int64     `json:"start_offset"`
	EndOffset     int64     `json:"end_offset"`
	Size          int64     `json:"size"`
	ReplicationFactor int   `json:"replication_factor"`
	AssignedNodes []string  `json:"assigned_nodes"`
	AccessPattern AccessPattern `json:"access_pattern"`
	Priority      int       `json:"priority"`
}

// MessageHandler handles coordination messages
type MessageHandler func(nodeID string, message []byte) error

// CoordinationMessage represents a message between nodes
type CoordinationMessage struct {
	Type        string                 `json:"type"`
	Source      string                 `json:"source"`
	Target      string                 `json:"target"`    // Empty for broadcast
	JobID       string                 `json:"job_id"`
	Timestamp   time.Time              `json:"timestamp"`
	Payload     map[string]interface{} `json:"payload"`
}

// NewDistributedCoordinator creates a new distributed coordinator
func NewDistributedCoordinator(nodeID string, enabled bool) *DistributedCoordinator {
	ctx, cancel := context.WithCancel(context.Background())
	
	dc := &DistributedCoordinator{
		enabled:           enabled,
		nodeID:           nodeID,
		discoveryInterval: 30 * time.Second,  // Discover nodes every 30 seconds
		heartbeatInterval: 10 * time.Second,  // Heartbeat every 10 seconds
		nodeTimeout:      60 * time.Second,   // Node timeout after 60 seconds
		
		remoteNodes:      make(map[string]*ClusterNode),
		activeJobs:       make(map[string]*DistributedTrainingJob),
		dataShards:       make(map[string]*DataShard),
		shardAssignments: make(map[string][]string),
		messageHandlers:  make(map[string]MessageHandler),
		
		ctx:              ctx,
		cancel:           cancel,
	}
	
	// Initialize local node after struct creation
	dc.localNode = dc.createLocalNode(nodeID)
	
	// Initialize message handlers
	dc.initializeMessageHandlers()
	
	if enabled {
		// Start background coordination tasks
		go dc.discoveryLoop()
		go dc.heartbeatLoop()
		go dc.coordinationLoop()
		
		glog.V(1).Infof("Distributed coordinator started for node %s", nodeID)
	}
	
	return dc
}

// createLocalNode creates information for the local node
func (dc *DistributedCoordinator) createLocalNode(nodeID string) *ClusterNode {
	// Detect local node capabilities
	// This could query system information, GPU status, etc.
	
	return &ClusterNode{
		NodeID:           nodeID,
		Address:          pb.ServerAddress("localhost:8888"), // Would be detected
		Role:             RoleUnknown,
		Zone:             "default",
		Region:           "local",
		GPUCount:         0,  // Would be detected
		GPUMemory:        0,  // Would be detected
		SystemMemory:     0,  // Would be detected
		NetworkBandwidth: 0,  // Would be measured
		StorageBandwidth: 0,  // Would be measured
		Status:           NodeStatusHealthy,
		LastHeartbeat:    time.Now(),
		LoadAverage:      0.0,
		DataLocality:     make(map[string]float64),
	}
}

// initializeMessageHandlers sets up message handlers for different message types
func (dc *DistributedCoordinator) initializeMessageHandlers() {
	dc.messageHandlers["heartbeat"] = dc.handleHeartbeat
	dc.messageHandlers["job_start"] = dc.handleJobStart
	dc.messageHandlers["job_complete"] = dc.handleJobComplete
	dc.messageHandlers["epoch_complete"] = dc.handleEpochComplete
	dc.messageHandlers["synchronization_barrier"] = dc.handleSynchronizationBarrier
	dc.messageHandlers["data_request"] = dc.handleDataRequest
	dc.messageHandlers["straggler_detection"] = dc.handleStragglerDetection
	dc.messageHandlers["node_failure"] = dc.handleNodeFailure
}

// RegisterTrainingJob registers a new distributed training job
func (dc *DistributedCoordinator) RegisterTrainingJob(job *DistributedTrainingJob) error {
	dc.Lock()
	defer dc.Unlock()
	
	dc.activeJobs[job.JobID] = job
	dc.totalJobs++
	
	// Create data shards for the job
	if err := dc.createDataShards(job); err != nil {
		return fmt.Errorf("failed to create data shards: %w", err)
	}
	
	// Assign shards to nodes
	if err := dc.assignDataShards(job); err != nil {
		return fmt.Errorf("failed to assign data shards: %w", err)
	}
	
	// Notify other nodes about the new job
	dc.broadcastMessage("job_start", job.JobID, map[string]interface{}{
		"job_config": job,
	})
	
	glog.V(1).Infof("Registered distributed training job: %s with %d nodes", job.JobID, len(job.Nodes))
	return nil
}

// createDataShards creates data shards for a training job
func (dc *DistributedCoordinator) createDataShards(job *DistributedTrainingJob) error {
	// Simple sharding strategy - divide dataset by node count
	nodeCount := len(job.Nodes)
	if nodeCount == 0 {
		return fmt.Errorf("no nodes available for job %s", job.JobID)
	}
	
	shardSize := job.DatasetSize / uint64(nodeCount)
	
	nodes := make([]string, 0, len(job.Nodes))
	for nodeID := range job.Nodes {
		nodes = append(nodes, nodeID)
	}
	sort.Strings(nodes) // Ensure consistent ordering
	
	for i, nodeID := range nodes {
		startOffset := int64(i) * int64(shardSize)
		endOffset := startOffset + int64(shardSize)
		if i == nodeCount-1 {
			// Last shard gets any remainder
			endOffset = int64(job.DatasetSize)
		}
		
		shardID := fmt.Sprintf("%s_shard_%d", job.JobID, i)
		shard := &DataShard{
			ShardID:           shardID,
			JobID:             job.JobID,
			FilePath:          job.DatasetPath,
			StartOffset:       startOffset,
			EndOffset:         endOffset,
			Size:              endOffset - startOffset,
			ReplicationFactor: 1, // No replication by default
			AssignedNodes:     []string{nodeID},
			AccessPattern:     SequentialAccess,
			Priority:          10,
		}
		
		dc.dataShards[shardID] = shard
	}
	
	glog.V(2).Infof("Created %d data shards for job %s", len(nodes), job.JobID)
	return nil
}

// assignDataShards assigns data shards to nodes based on locality and load
func (dc *DistributedCoordinator) assignDataShards(job *DistributedTrainingJob) error {
	assignments := make([]string, 0)
	
	for _, shard := range dc.dataShards {
		if shard.JobID != job.JobID {
			continue
		}
		
		// Find best node for this shard based on locality and load
		bestNode := dc.findBestNodeForShard(shard, job)
		if bestNode != "" {
			shard.AssignedNodes = []string{bestNode}
			assignments = append(assignments, bestNode)
		}
	}
	
	dc.shardAssignments[job.JobID] = assignments
	
	glog.V(2).Infof("Assigned data shards for job %s to %d nodes", job.JobID, len(assignments))
	return nil
}

// findBestNodeForShard finds the best node to assign a data shard to
func (dc *DistributedCoordinator) findBestNodeForShard(shard *DataShard, job *DistributedTrainingJob) string {
	bestNode := ""
	bestScore := -1.0
	
	for nodeID, node := range job.Nodes {
		node.RLock()
		
		// Calculate assignment score based on:
		// 1. Data locality
		// 2. Current load
		// 3. Network distance
		// 4. Hardware capabilities
		
		localityScore := node.DataLocality[shard.FilePath]
		if localityScore == 0 {
			localityScore = 0.1 // Default low locality
		}
		
		loadScore := 1.0 - (node.LoadAverage / 10.0) // Assume max load of 10
		if loadScore < 0 {
			loadScore = 0
		}
		
		hardwareScore := float64(node.GPUCount) / 8.0 // Normalize by typical GPU count
		if hardwareScore > 1.0 {
			hardwareScore = 1.0
		}
		
		totalScore := localityScore*0.5 + loadScore*0.3 + hardwareScore*0.2
		
		node.RUnlock()
		
		if totalScore > bestScore {
			bestScore = totalScore
			bestNode = nodeID
		}
	}
	
	return bestNode
}

// OptimizeDataAccess optimizes data access patterns for distributed training
func (dc *DistributedCoordinator) OptimizeDataAccess(jobID string, filePatterns []string) *DataAccessOptimization {
	dc.RLock()
	job := dc.activeJobs[jobID]
	dc.RUnlock()
	
	if job == nil {
		return &DataAccessOptimization{
			RecommendedPrefetchSize: 64 * 1024,
			ShouldCache:            false,
			OptimalNodes:           []string{},
		}
	}
	
	job.RLock()
	defer job.RUnlock()
	
	optimization := &DataAccessOptimization{
		JobID:                   jobID,
		RecommendedPrefetchSize: 0,
		ShouldCache:            false,
		OptimalNodes:           make([]string, 0),
		ShardRecommendations:    make(map[string]*ShardRecommendation),
	}
	
	// Analyze access patterns across nodes
	totalNodes := len(job.Nodes)
	avgBatchSize := job.BatchSize
	
	// Calculate optimal prefetch size based on distributed training characteristics
	if job.Topology == TopologyAllReduce {
		// All-reduce benefits from larger prefetch to hide synchronization
		optimization.RecommendedPrefetchSize = int64(avgBatchSize) * 4 * 1024 // 4x batch size in KB
	} else if job.Topology == TopologyParameterServer {
		// Parameter server benefits from moderate prefetch
		optimization.RecommendedPrefetchSize = int64(avgBatchSize) * 2 * 1024 // 2x batch size in KB
	} else {
		// Default prefetch size
		optimization.RecommendedPrefetchSize = 256 * 1024 // 256KB
	}
	
	// Enable caching for frequently accessed files
	optimization.ShouldCache = totalNodes > 1 // Cache when multiple nodes
	
	// Recommend optimal nodes for file access based on data locality
	for nodeID, node := range job.Nodes {
		node.RLock()
		avgLocality := 0.0
		for _, locality := range node.DataLocality {
			avgLocality += locality
		}
		if len(node.DataLocality) > 0 {
			avgLocality /= float64(len(node.DataLocality))
		}
		node.RUnlock()
		
		if avgLocality > 0.7 { // High locality threshold
			optimization.OptimalNodes = append(optimization.OptimalNodes, nodeID)
		}
	}
	
	return optimization
}

// DataAccessOptimization holds recommendations for optimizing data access
type DataAccessOptimization struct {
	JobID                   string                           `json:"job_id"`
	RecommendedPrefetchSize int64                            `json:"recommended_prefetch_size"`
	ShouldCache             bool                             `json:"should_cache"`
	OptimalNodes            []string                         `json:"optimal_nodes"`
	ShardRecommendations    map[string]*ShardRecommendation  `json:"shard_recommendations"`
}

// ShardRecommendation holds recommendations for a specific data shard
type ShardRecommendation struct {
	ShardID         string  `json:"shard_id"`
	PreferredNode   string  `json:"preferred_node"`
	PrefetchSize    int64   `json:"prefetch_size"`
	CachingStrategy string  `json:"caching_strategy"`
	Priority        int     `json:"priority"`
}

// Message handling functions

func (dc *DistributedCoordinator) handleHeartbeat(nodeID string, message []byte) error {
	var heartbeat CoordinationMessage
	if err := json.Unmarshal(message, &heartbeat); err != nil {
		return err
	}
	
	dc.Lock()
	if node, exists := dc.remoteNodes[nodeID]; exists {
		node.LastHeartbeat = time.Now()
		if status, ok := heartbeat.Payload["status"].(float64); ok {
			node.Status = NodeStatus(status)
		}
		if load, ok := heartbeat.Payload["load_average"].(float64); ok {
			node.LoadAverage = load
		}
	}
	dc.Unlock()
	
	return nil
}

func (dc *DistributedCoordinator) handleJobStart(nodeID string, message []byte) error {
	glog.V(2).Infof("Received job start notification from node %s", nodeID)
	dc.coordinationEvents++
	return nil
}

func (dc *DistributedCoordinator) handleJobComplete(nodeID string, message []byte) error {
	glog.V(2).Infof("Received job completion notification from node %s", nodeID)
	dc.coordinationEvents++
	return nil
}

func (dc *DistributedCoordinator) handleEpochComplete(nodeID string, message []byte) error {
	var msg CoordinationMessage
	if err := json.Unmarshal(message, &msg); err != nil {
		return err
	}
	
	jobID := msg.JobID
	if epoch, ok := msg.Payload["epoch"].(float64); ok {
		dc.updateJobProgress(jobID, nodeID, int(epoch))
	}
	
	return nil
}

func (dc *DistributedCoordinator) handleSynchronizationBarrier(nodeID string, message []byte) error {
	// Handle synchronization barriers for distributed training
	glog.V(3).Infof("Synchronization barrier reached by node %s", nodeID)
	return nil
}

func (dc *DistributedCoordinator) handleDataRequest(nodeID string, message []byte) error {
	// Handle requests for data shards from other nodes
	glog.V(3).Infof("Data request received from node %s", nodeID)
	return nil
}

func (dc *DistributedCoordinator) handleStragglerDetection(nodeID string, message []byte) error {
	var msg CoordinationMessage
	if err := json.Unmarshal(message, &msg); err != nil {
		return err
	}
	
	if stragglerNode, ok := msg.Payload["straggler_node"].(string); ok {
		dc.markNodeAsStraggler(msg.JobID, stragglerNode)
	}
	
	return nil
}

func (dc *DistributedCoordinator) handleNodeFailure(nodeID string, message []byte) error {
	glog.V(1).Infof("Node failure reported: %s", nodeID)
	dc.markNodeAsUnhealthy(nodeID)
	return nil
}

// Background task loops

func (dc *DistributedCoordinator) discoveryLoop() {
	ticker := time.NewTicker(dc.discoveryInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-dc.ctx.Done():
			return
		case <-ticker.C:
			dc.discoverNodes()
		}
	}
}

func (dc *DistributedCoordinator) heartbeatLoop() {
	ticker := time.NewTicker(dc.heartbeatInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-dc.ctx.Done():
			return
		case <-ticker.C:
			dc.sendHeartbeat()
		}
	}
}

func (dc *DistributedCoordinator) coordinationLoop() {
	ticker := time.NewTicker(30 * time.Second) // Coordinate every 30 seconds
	defer ticker.Stop()
	
	for {
		select {
		case <-dc.ctx.Done():
			return
		case <-ticker.C:
			dc.performCoordination()
		}
	}
}

// Helper functions

func (dc *DistributedCoordinator) discoverNodes() {
	// Discovery logic would depend on the specific setup:
	// - Service discovery (Consul, etcd, Kubernetes)
	// - Multicast discovery
	// - Static configuration
	// For now, we'll use a simple placeholder
	
	glog.V(4).Infof("Discovering cluster nodes...")
}

func (dc *DistributedCoordinator) sendHeartbeat() {
	heartbeat := map[string]interface{}{
		"status":       dc.localNode.Status,
		"load_average": dc.localNode.LoadAverage,
		"timestamp":    time.Now(),
	}
	
	dc.broadcastMessage("heartbeat", "", heartbeat)
}

func (dc *DistributedCoordinator) broadcastMessage(msgType, jobID string, payload map[string]interface{}) {
	message := CoordinationMessage{
		Type:      msgType,
		Source:    dc.nodeID,
		Target:    "", // Broadcast
		JobID:     jobID,
		Timestamp: time.Now(),
		Payload:   payload,
	}
	
	// Message broadcasting would be implemented based on the communication mechanism
	// (gRPC, HTTP, message queue, etc.)
	glog.V(4).Infof("Broadcasting message type %s from %s", message.Type, message.Source)
}

func (dc *DistributedCoordinator) performCoordination() {
	// Perform coordination tasks:
	// 1. Check for straggler nodes
	// 2. Rebalance data shards if needed
	// 3. Handle failed nodes
	// 4. Optimize communication patterns
	
	dc.detectStragglers()
	dc.cleanupOfflineNodes()
}

func (dc *DistributedCoordinator) detectStragglers() {
	for jobID, job := range dc.activeJobs {
		job.RLock()
		
		// Calculate average progress across nodes
		totalProgress := 0
		nodeCount := 0
		for _, node := range job.Nodes {
			node.RLock()
			totalProgress += node.CurrentEpoch
			nodeCount++
			node.RUnlock()
		}
		
		if nodeCount > 0 {
			avgProgress := float64(totalProgress) / float64(nodeCount)
			
			// Identify stragglers (nodes significantly behind average)
			for nodeID, node := range job.Nodes {
				node.RLock()
				if float64(node.CurrentEpoch) < avgProgress*0.8 { // 20% behind
					dc.markNodeAsStraggler(jobID, nodeID)
				}
				node.RUnlock()
			}
		}
		
		job.RUnlock()
	}
}

func (dc *DistributedCoordinator) cleanupOfflineNodes() {
	now := time.Now()
	
	dc.Lock()
	for nodeID, node := range dc.remoteNodes {
		node.RLock()
		if now.Sub(node.LastHeartbeat) > dc.nodeTimeout {
			dc.markNodeAsOffline(nodeID)
		}
		node.RUnlock()
	}
	dc.Unlock()
}

func (dc *DistributedCoordinator) updateJobProgress(jobID, nodeID string, epoch int) {
	dc.RLock()
	job := dc.activeJobs[jobID]
	dc.RUnlock()
	
	if job == nil {
		return
	}
	
	job.Lock()
	if node, exists := job.Nodes[nodeID]; exists {
		node.Lock()
		node.CurrentEpoch = epoch
		node.LastHeartbeat = time.Now()
		node.Unlock()
	}
	job.Unlock()
}

func (dc *DistributedCoordinator) markNodeAsStraggler(jobID, nodeID string) {
	dc.RLock()
	job := dc.activeJobs[jobID]
	dc.RUnlock()
	
	if job == nil {
		return
	}
	
	job.Lock()
	// Add to straggler list if not already there
	for _, straggler := range job.StragglerNodes {
		if straggler == nodeID {
			job.Unlock()
			return
		}
	}
	job.StragglerNodes = append(job.StragglerNodes, nodeID)
	job.Unlock()
	
	glog.V(2).Infof("Marked node %s as straggler in job %s", nodeID, jobID)
}

func (dc *DistributedCoordinator) markNodeAsUnhealthy(nodeID string) {
	dc.Lock()
	if node, exists := dc.remoteNodes[nodeID]; exists {
		node.Lock()
		node.Status = NodeStatusUnhealthy
		node.Unlock()
	}
	dc.Unlock()
}

func (dc *DistributedCoordinator) markNodeAsOffline(nodeID string) {
	dc.Lock()
	if node, exists := dc.remoteNodes[nodeID]; exists {
		node.Lock()
		node.Status = NodeStatusOffline
		node.Unlock()
	}
	dc.Unlock()
	
	glog.V(2).Infof("Marked node %s as offline", nodeID)
}

// GetDistributedMetrics returns metrics for distributed coordination
func (dc *DistributedCoordinator) GetDistributedMetrics() DistributedCoordinationMetrics {
	dc.RLock()
	defer dc.RUnlock()
	
	return DistributedCoordinationMetrics{
		TotalJobs:              dc.totalJobs,
		ActiveJobs:             int64(len(dc.activeJobs)),
		ActiveNodes:            dc.activeNodes,
		TotalDataShards:        int64(len(dc.dataShards)),
		CoordinationEvents:     dc.coordinationEvents,
		SynchronizationLatency: dc.synchronizationLatency,
	}
}

// DistributedCoordinationMetrics holds metrics for distributed coordination
type DistributedCoordinationMetrics struct {
	TotalJobs              int64         `json:"total_jobs"`
	ActiveJobs             int64         `json:"active_jobs"`
	ActiveNodes            int64         `json:"active_nodes"`
	TotalDataShards        int64         `json:"total_data_shards"`
	CoordinationEvents     int64         `json:"coordination_events"`
	SynchronizationLatency time.Duration `json:"synchronization_latency"`
}

// Shutdown gracefully shuts down the distributed coordinator
func (dc *DistributedCoordinator) Shutdown() {
	if dc.cancel != nil {
		dc.cancel()
	}
	
	glog.V(1).Infof("Distributed coordinator shutdown complete")
}

// Helper functions for role and status string conversion

func (r DistributedTrainingRole) String() string {
	switch r {
	case RoleParameterServer:
		return "ParameterServer"
	case RoleWorker:
		return "Worker"
	case RoleChief:
		return "Chief"
	case RoleEvaluator:
		return "Evaluator"
	case RoleAllReduce:
		return "AllReduce"
	case RoleMaster:
		return "Master"
	default:
		return "Unknown"
	}
}

func (s NodeStatus) String() string {
	switch s {
	case NodeStatusHealthy:
		return "Healthy"
	case NodeStatusBusy:
		return "Busy"
	case NodeStatusOverloaded:
		return "Overloaded"
	case NodeStatusUnhealthy:
		return "Unhealthy"
	case NodeStatusOffline:
		return "Offline"
	default:
		return "Unknown"
	}
}

// hashString creates a consistent hash for string-based sharding
func hashString(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
