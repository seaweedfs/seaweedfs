package cluster_replication

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// RegisterSimple registers the cluster replication detector and scheduler with simplified system
func RegisterSimple(registry *types.TaskRegistry) {
	detector := NewSimpleDetector()
	scheduler := NewSimpleScheduler()

	registry.RegisterTask(detector, scheduler)

	glog.V(1).Infof("Registered simplified cluster replication task")
}

// GetSimpleDetector returns the simple detector for configuration
func GetSimpleDetector(registry *types.TaskRegistry) *SimpleDetector {
	detector := registry.GetDetector(types.TaskTypeClusterReplication)
	if detector != nil {
		if clusterDetector, ok := detector.(*SimpleDetector); ok {
			return clusterDetector
		}
	}
	return nil
}

// GetSimpleScheduler returns the simple scheduler for configuration
func GetSimpleScheduler(registry *types.TaskRegistry) *SimpleScheduler {
	scheduler := registry.GetScheduler(types.TaskTypeClusterReplication)
	if scheduler != nil {
		if clusterScheduler, ok := scheduler.(*SimpleScheduler); ok {
			return clusterScheduler
		}
	}
	return nil
}
