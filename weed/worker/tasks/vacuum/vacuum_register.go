package vacuum

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// RegisterSimple registers the vacuum task components with the simplified registry
func RegisterSimple(registry *types.TaskRegistry) {
	detector := NewSimpleDetector()
	scheduler := NewSimpleScheduler()

	registry.RegisterTask(detector, scheduler)

	glog.V(1).Infof("✅ Registered simplified vacuum task (detector + scheduler)")
}

// GetDetector returns the vacuum detector for configuration
func GetDetector(registry *types.TaskRegistry) *SimpleDetector {
	detector := registry.GetDetector(types.TaskTypeVacuum)
	if detector == nil {
		return nil
	}

	if vacuumDetector, ok := detector.(*SimpleDetector); ok {
		return vacuumDetector
	}

	return nil
}

// GetScheduler returns the vacuum scheduler for configuration
func GetScheduler(registry *types.TaskRegistry) *SimpleScheduler {
	scheduler := registry.GetScheduler(types.TaskTypeVacuum)
	if scheduler == nil {
		return nil
	}

	if vacuumScheduler, ok := scheduler.(*SimpleScheduler); ok {
		return vacuumScheduler
	}

	return nil
}
