package remote_upload

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// RegisterSimple registers the remote upload detector and scheduler with simplified system
func RegisterSimple(registry *types.TaskRegistry) {
	detector := NewSimpleDetector()
	scheduler := NewSimpleScheduler()

	registry.RegisterTask(detector, scheduler)

	glog.V(1).Infof("Registered simplified remote upload task")
}

// GetSimpleDetector returns the simple detector for configuration
func GetSimpleDetector(registry *types.TaskRegistry) *SimpleDetector {
	detector := registry.GetDetector(types.TaskTypeRemoteUpload)
	if detector != nil {
		if remoteDetector, ok := detector.(*SimpleDetector); ok {
			return remoteDetector
		}
	}
	return nil
}

// GetSimpleScheduler returns the simple scheduler for configuration
func GetSimpleScheduler(registry *types.TaskRegistry) *SimpleScheduler {
	scheduler := registry.GetScheduler(types.TaskTypeRemoteUpload)
	if scheduler != nil {
		if remoteScheduler, ok := scheduler.(*SimpleScheduler); ok {
			return remoteScheduler
		}
	}
	return nil
}
