package delete_empty

import (
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

var globalTaskDef *base.TaskDefinition

func init() {
	RegisterDeleteEmptyTask()
	tasks.AutoRegisterConfigUpdater(types.TaskTypeCompaction, UpdateConfigFromPersistence)
}

// RegisterDeleteEmptyTask registers the delete_empty task with the worker architecture
func RegisterDeleteEmptyTask() {
	cfg := NewDefaultConfig()
	dialOpt := security.LoadClientTLS(util.GetViper(), "grpc.worker")

	taskDef := &base.TaskDefinition{
		Type:         types.TaskTypeCompaction,
		Name:         "compaction",
		DisplayName:  "Volume Compaction",
		Description:  "Compaction operations for volumes, including deletion of empty (zero-byte) volumes",
		Icon:         "fas fa-compress-alt text-info",
		Capabilities: []string{"compaction", "storage"},

		Config:     cfg,
		ConfigSpec: GetConfigSpec(),
		CreateTask: func(params *worker_pb.TaskParams) (types.Task, error) {
			if params == nil {
				return nil, fmt.Errorf("task parameters are required")
			}
			if len(params.Sources) == 0 {
				return nil, fmt.Errorf("at least one source is required for compaction task")
			}
			taskID := params.TaskId
			if taskID == "" {
				taskID = fmt.Sprintf("compaction-%d", params.VolumeId)
			}
			return NewDeleteEmptyTask(
				taskID,
				params.Sources[0].Node,
				params.VolumeId,
				params.Collection,
				dialOpt,
			), nil
		},
		DetectionFunc:  Detection,
		ScanInterval:   6 * time.Hour,
		SchedulingFunc: Scheduling,
		MaxConcurrent:  1,
		RepeatInterval: 6 * time.Hour,
	}

	globalTaskDef = taskDef
	base.RegisterTask(taskDef)
}

// UpdateConfigFromPersistence updates the compaction configuration from persistence
func UpdateConfigFromPersistence(configPersistence interface{}) error {
	if globalTaskDef == nil {
		return fmt.Errorf("compaction task not registered")
	}

	newConfig := LoadConfigFromPersistence(configPersistence)
	if newConfig == nil {
		return fmt.Errorf("failed to load configuration from persistence")
	}

	globalTaskDef.Config = newConfig
	// Sync the runtime scheduling fields read by GenericDetector/GenericScheduler
	// so that persisted changes to intervals and concurrency take effect.
	globalTaskDef.ScanInterval = time.Duration(newConfig.ScanIntervalSeconds) * time.Second
	globalTaskDef.MaxConcurrent = newConfig.MaxConcurrent
	globalTaskDef.RepeatInterval = time.Duration(newConfig.ScanIntervalSeconds) * time.Second
	glog.V(1).Infof("Updated compaction task configuration from persistence")
	return nil
}
