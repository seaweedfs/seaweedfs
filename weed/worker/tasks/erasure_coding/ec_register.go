package erasure_coding

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// RegisterSimple registers the erasure coding detector and scheduler with the task registry
func RegisterSimple(registry *types.TaskRegistry) {
	detector := NewEcDetector()
	scheduler := NewScheduler()

	registry.RegisterTask(detector, scheduler)

	glog.V(1).Infof("Registered erasure coding task")
}
