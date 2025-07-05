package erasure_coding

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// RegisterSimple registers the erasure coding detector and scheduler with the task registry
func RegisterSimple(registry *types.TaskRegistry) {
	detector := NewDetector()
	scheduler := NewScheduler()

	registry.RegisterTask(detector, scheduler)

	glog.V(1).Infof("Registered erasure coding task")
}

// GetDetector returns the detector for configuration
func GetDetector(registry *types.TaskRegistry) *Detector {
	detector := registry.GetDetector(types.TaskTypeErasureCoding)
	if detector != nil {
		if ecDetector, ok := detector.(*Detector); ok {
			return ecDetector
		}
	}
	return nil
}

// GetScheduler returns the scheduler for configuration
func GetScheduler(registry *types.TaskRegistry) *Scheduler {
	scheduler := registry.GetScheduler(types.TaskTypeErasureCoding)
	if scheduler != nil {
		if ecScheduler, ok := scheduler.(*Scheduler); ok {
			return ecScheduler
		}
	}
	return nil
}

// GetSimpleDetector returns the detector for configuration (backward compatibility)
func GetSimpleDetector(registry *types.TaskRegistry) *Detector {
	return GetDetector(registry)
}

// GetSimpleScheduler returns the scheduler for configuration (backward compatibility)
func GetSimpleScheduler(registry *types.TaskRegistry) *Scheduler {
	return GetScheduler(registry)
}
