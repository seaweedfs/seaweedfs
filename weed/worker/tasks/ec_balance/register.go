package ec_balance

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Global variable to hold the task definition for configuration updates
var globalTaskDef *base.TaskDefinition

// Auto-register this task when the package is imported
func init() {
	RegisterECBalanceTask()

	// Register config updater
	tasks.AutoRegisterConfigUpdater(types.TaskTypeECBalance, UpdateConfigFromPersistence)
}

// RegisterECBalanceTask registers the EC balance task with the task architecture
func RegisterECBalanceTask() {
	cfg := NewDefaultConfig()

	// Create shared gRPC dial option using TLS configuration
	dialOpt := security.LoadClientTLS(util.GetViper(), "grpc.worker")

	// Create complete task definition
	taskDef := &base.TaskDefinition{
		Type:         types.TaskTypeECBalance,
		Name:         "ec_balance",
		DisplayName:  "EC Shard Balance",
		Description:  "Balances EC shard distribution across racks and servers",
		Icon:         "fas fa-balance-scale-left text-info",
		Capabilities: []string{"ec_balance", "erasure_coding"},

		Config:     cfg,
		ConfigSpec: GetConfigSpec(),
		CreateTask: func(params *worker_pb.TaskParams) (types.Task, error) {
			if params == nil {
				return nil, fmt.Errorf("task parameters are required")
			}
			return NewECBalanceTask(
				fmt.Sprintf("ec_balance-%d", params.VolumeId),
				params.VolumeId,
				params.Collection,
				dialOpt,
			), nil
		},
		DetectionFunc: func(metrics []*types.VolumeHealthMetrics, clusterInfo *types.ClusterInfo, config base.TaskConfig) ([]*types.TaskDetectionResult, error) {
			results, _, err := Detection(context.Background(), metrics, clusterInfo, config, 0)
			return results, err
		},
		ScanInterval:   1 * time.Hour,
		SchedulingFunc: Scheduling,
		MaxConcurrent:  1,
		RepeatInterval: 4 * time.Hour,
	}

	// Store task definition globally for configuration updates
	globalTaskDef = taskDef

	// Register everything with a single function call
	base.RegisterTask(taskDef)
}

// UpdateConfigFromPersistence updates the EC balance configuration from persistence
func UpdateConfigFromPersistence(configPersistence interface{}) error {
	if globalTaskDef == nil {
		return fmt.Errorf("ec balance task not registered")
	}

	newConfig := LoadConfigFromPersistence(configPersistence)
	if newConfig == nil {
		return fmt.Errorf("failed to load configuration from persistence")
	}

	globalTaskDef.Config = newConfig

	glog.V(1).Infof("Updated EC balance task configuration from persistence")
	return nil
}
