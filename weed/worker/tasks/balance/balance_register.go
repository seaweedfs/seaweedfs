package balance

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// RegisterSimple registers the balance detector and scheduler with the registry
func RegisterSimple(registry *types.TaskRegistry) {
	detector := NewSimpleDetector()
	scheduler := NewSimpleScheduler()

	registry.RegisterTask(detector, scheduler)

	glog.V(1).Infof("Registered balance task")
}

// GetSimpleDetector returns the balance detector for configuration
func GetSimpleDetector(registry *types.TaskRegistry) *SimpleDetector {
	detector := registry.GetDetector(types.TaskTypeBalance)
	if detector != nil {
		if balanceDetector, ok := detector.(*SimpleDetector); ok {
			return balanceDetector
		}
	}
	return nil
}

// GetSimpleScheduler returns the balance scheduler for configuration
func GetSimpleScheduler(registry *types.TaskRegistry) *SimpleScheduler {
	scheduler := registry.GetScheduler(types.TaskTypeBalance)
	if scheduler != nil {
		if balanceScheduler, ok := scheduler.(*SimpleScheduler); ok {
			return balanceScheduler
		}
	}
	return nil
}
