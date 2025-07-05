package balance

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// RegisterSimple registers the balance detector and scheduler with the registry
func RegisterSimple(registry *types.TaskRegistry) {
	detector := NewBalanceDetector()
	scheduler := NewBalanceScheduler()

	registry.RegisterTask(detector, scheduler)

	glog.V(1).Infof("Registered balance task")
}
