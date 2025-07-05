package replication

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// RegisterSimple registers the replication detector and scheduler with simplified system
func RegisterSimple(registry *types.TaskRegistry) {
	detector := NewSimpleDetector()
	scheduler := NewSimpleScheduler()

	registry.RegisterTask(detector, scheduler)

	glog.V(1).Infof("Registered simplified replication task")
}

// GetSimpleDetector returns the simple detector for configuration
func GetSimpleDetector(registry *types.TaskRegistry) *SimpleDetector {
	detector := registry.GetDetector(types.TaskTypeFixReplication)
	if detector != nil {
		if replDetector, ok := detector.(*SimpleDetector); ok {
			return replDetector
		}
	}
	return nil
}

// GetSimpleScheduler returns the simple scheduler for configuration
func GetSimpleScheduler(registry *types.TaskRegistry) *SimpleScheduler {
	scheduler := registry.GetScheduler(types.TaskTypeFixReplication)
	if scheduler != nil {
		if replScheduler, ok := scheduler.(*SimpleScheduler); ok {
			return replScheduler
		}
	}
	return nil
}
