package remote_upload

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// RegisterSimple registers the remote upload detector and scheduler with simplified system
func RegisterSimple(registry *types.TaskRegistry) {
	detector := NewRemoteUploadDetector()
	scheduler := NewRemoteUploadScheduler()

	registry.RegisterTask(detector, scheduler)

	glog.V(1).Infof("Registered simplified remote upload task")
}
