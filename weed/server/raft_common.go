package weed_server

import (
	"time"

	"github.com/google/uuid"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

// EnsureTopologyId ensures that a TopologyId is generated and persisted if it's currently missing.
// It uses the provided checkLeaderFn to verify leadership and persistFn to save the new ID.
func EnsureTopologyId(topo *topology.Topology, checkLeaderFn func() bool, persistFn func(string) error) {
	if topo.GetTopologyId() != "" {
		return
	}

	topologyId := uuid.New().String()
	for {
		if !checkLeaderFn() {
			glog.V(0).Infof("lost leadership while saving topologyId")
			return
		}

		// Another concurrent operation may have set the ID between generation and now.
		if latestId := topo.GetTopologyId(); latestId != "" {
			glog.V(1).Infof("topologyId was set concurrently to %s, aborting generation", latestId)
			return
		}

		if err := persistFn(topologyId); err != nil {
			glog.Errorf("failed to save topologyId, will retry: %v", err)
			time.Sleep(time.Second)
			continue
		}

		// Verify that the topology ID was actually applied as expected.
		appliedId := topo.GetTopologyId()
		if appliedId != "" && appliedId != topologyId {
			glog.V(0).Infof("TopologyId generation race: expected %s, but current TopologyId is %s", topologyId, appliedId)
		} else {
			glog.V(0).Infof("TopologyId generated: %s", topologyId)
		}
		break
	}
}
