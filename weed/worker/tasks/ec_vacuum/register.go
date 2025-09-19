package ec_vacuum

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Global variable to hold the task definition for configuration updates
var globalTaskDef *base.TaskDefinition

// Auto-register this task when the package is imported
func init() {
	RegisterEcVacuumTask()

	// Register config updater
	tasks.AutoRegisterConfigUpdater(types.TaskType("ec_vacuum"), UpdateConfigFromPersistence)
}

// RegisterEcVacuumTask registers the EC vacuum task with the new architecture
func RegisterEcVacuumTask() {
	// Create configuration instance
	config := NewDefaultConfig()

	// Create complete task definition
	taskDef := &base.TaskDefinition{
		Type:         types.TaskType("ec_vacuum"),
		Name:         "ec_vacuum",
		DisplayName:  "EC Vacuum",
		Description:  "Cleans up deleted data from erasure coded volumes with intelligent multi-generation handling",
		Icon:         "fas fa-broom text-warning",
		Capabilities: []string{"ec_vacuum", "data_cleanup"},

		Config:     config,
		ConfigSpec: GetConfigSpec(),
		CreateTask: func(params *worker_pb.TaskParams) (types.Task, error) {
			if params == nil {
				return nil, fmt.Errorf("task parameters are required")
			}
			if params.VolumeId == 0 {
				return nil, fmt.Errorf("volume ID is required for EC vacuum task")
			}

			// Parse source nodes from task parameters
			glog.Infof("Creating EC vacuum task for volume %d with %d sources", params.VolumeId, len(params.Sources))

			// Log raw source data for debugging
			for i, source := range params.Sources {
				glog.Infof("Raw source %d: node=%s, shardIds=%v", i, source.Node, source.ShardIds)
			}

			sourceNodes := make(map[pb.ServerAddress]erasure_coding.ShardBits)

			// Populate source nodes from the task parameters
			for _, source := range params.Sources {
				if source.Node == "" {
					continue
				}

				serverAddr := pb.ServerAddress(source.Node)
				var shardBits erasure_coding.ShardBits

				// Convert shard IDs to ShardBits
				for _, shardId := range source.ShardIds {
					if shardId < erasure_coding.TotalShardsCount {
						shardBits = shardBits.AddShardId(erasure_coding.ShardId(shardId))
					}
				}

				if shardBits.ShardIdCount() > 0 {
					sourceNodes[serverAddr] = shardBits
				}
			}

			// Verify we have source nodes
			if len(sourceNodes) == 0 {
				return nil, fmt.Errorf("no valid source nodes found for EC vacuum task: sources=%d", len(params.Sources))
			}

			// Log detailed shard distribution for debugging
			shardDistribution := make(map[string][]int)
			for serverAddr, shardBits := range sourceNodes {
				shardDistribution[string(serverAddr)] = make([]int, 0)
				for shardId := 0; shardId < erasure_coding.TotalShardsCount; shardId++ {
					if shardBits.HasShardId(erasure_coding.ShardId(shardId)) {
						shardDistribution[string(serverAddr)] = append(shardDistribution[string(serverAddr)], shardId)
					}
				}
			}

			// Validate that we have all required data shards
			allShards := make(map[int]bool)
			for _, shardBits := range sourceNodes {
				for i := 0; i < erasure_coding.TotalShardsCount; i++ {
					if shardBits.HasShardId(erasure_coding.ShardId(i)) {
						allShards[i] = true
					}
				}
			}

			missingShards := make([]int, 0)
			for i := 0; i < erasure_coding.DataShardsCount; i++ {
				if !allShards[i] {
					missingShards = append(missingShards, i)
				}
			}

			if len(missingShards) > 0 {
				glog.Warningf("EC vacuum task for volume %d has missing data shards %v - this should not happen! Distribution: %+v",
					params.VolumeId, missingShards, shardDistribution)
			} else {
				glog.Infof("EC vacuum task created for volume %d with complete data shards. Distribution: %+v",
					params.VolumeId, shardDistribution)
			}

			glog.Infof("EC vacuum task for volume %d will determine generation during execution", params.VolumeId)

			task := NewEcVacuumTask(
				fmt.Sprintf("ec_vacuum-%d", params.VolumeId),
				params.VolumeId,
				params.Collection,
				sourceNodes,
			)

			// If task has a topology-linked TaskID, store it for lifecycle management
			if params.TaskId != "" {
				task.SetTopologyTaskID(params.TaskId)
				glog.V(2).Infof("EC vacuum task linked to topology task ID: %s", params.TaskId)
			}

			// Cleanup planning is now done during detection phase with topology access
			// The task will query master directly when needed for detailed generation info

			return task, nil
		},
		DetectionFunc:  Detection,
		ScanInterval:   24 * time.Hour, // Default scan every 24 hours
		SchedulingFunc: Scheduling,
		MaxConcurrent:  1,                  // Default max 1 concurrent
		RepeatInterval: 7 * 24 * time.Hour, // Repeat weekly for same volumes
	}

	// Store task definition globally for configuration updates
	globalTaskDef = taskDef

	// Register everything with a single function call!
	base.RegisterTask(taskDef)

	glog.V(1).Infof("✅ Registered EC vacuum task definition")
}

// UpdateConfigFromPersistence updates the EC vacuum configuration from persistence
func UpdateConfigFromPersistence(configPersistence interface{}) error {
	if globalTaskDef == nil {
		return fmt.Errorf("EC vacuum task not registered")
	}

	// Load configuration from persistence
	newConfig := LoadConfigFromPersistence(configPersistence)
	if newConfig == nil {
		return fmt.Errorf("failed to load configuration from persistence")
	}

	// Update the task definition's config
	globalTaskDef.Config = newConfig

	// Update scan interval from config
	globalTaskDef.ScanInterval = time.Duration(newConfig.ScanIntervalSeconds) * time.Second
	globalTaskDef.MaxConcurrent = newConfig.MaxConcurrent

	glog.V(1).Infof("Updated EC vacuum task configuration from persistence")
	return nil
}
