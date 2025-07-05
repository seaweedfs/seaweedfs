package vacuum

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// RegisterSimple registers the vacuum task components with the simplified registry
func RegisterSimple(registry *types.TaskRegistry) {
	detector := NewVacuumDetector()
	scheduler := NewVacuumScheduler()

	registry.RegisterTask(detector, scheduler)

	glog.V(1).Infof("✅ Registered simplified vacuum task (detector + scheduler)")
}
