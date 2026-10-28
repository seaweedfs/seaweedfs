package topology

import (
	"encoding/json"
	"fmt"

	hashicorpRaft "github.com/hashicorp/raft"
	"github.com/seaweedfs/raft"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

type MaxVolumeIdCommand struct {
	MaxVolumeId needle.VolumeId `json:"maxVolumeId"`
	TopologyId  string          `json:"topologyId"`
	// GrowthFrozen replicates the volume-growth freeze through raft so a
	// leader change does not silently lift it. nil means no change.
	GrowthFrozen *bool `json:"growthFrozen,omitempty"`
	// GrowthFrozenAt pins the freeze instant so every master measures the
	// same recovery window and can tell one freeze generation from the next.
	// Zero on entries written before the field existed.
	GrowthFrozenAt int64 `json:"growthFrozenAt,omitempty"`
	// GrowthUnfreezeFor binds GrowthFrozen=false to the freeze generation
	// the submitter observed: the lift applies only while that exact
	// generation is armed, so a stale unfreeze cannot erase a freeze that
	// reached the log first. Zero lifts unconditionally, for entries
	// written before the binding existed; -1 binds the ageless generation
	// left by freeze entries written before instants existed.
	GrowthUnfreezeFor int64 `json:"growthUnfreezeFor,omitempty"`
}

func NewMaxVolumeIdCommand(value needle.VolumeId, topologyId string) *MaxVolumeIdCommand {
	return &MaxVolumeIdCommand{
		MaxVolumeId: value,
		TopologyId:  topologyId,
	}
}

func (c *MaxVolumeIdCommand) CommandName() string {
	return "MaxVolumeId"
}

// deprecatedCommandApply represents the old interface to apply a command to the server.
func (c *MaxVolumeIdCommand) Apply(server raft.Server) (interface{}, error) {
	topo := server.Context().(*Topology)
	before := topo.GetMaxVolumeId()
	topo.UpAdjustMaxVolumeId(c.MaxVolumeId)
	if c.TopologyId != "" {
		prevTopologyId := topo.GetTopologyId()
		topo.SetTopologyId(c.TopologyId)
		// Log when TopologyId is set for the first time, with different messages for leader and follower.
		if prevTopologyId == "" {
			if server.State() == raft.Leader {
				glog.V(0).Infof("TopologyId generated and applied on leader: %s", c.TopologyId)
			} else {
				glog.V(0).Infof("TopologyId applied on follower: %s", c.TopologyId)
			}
		}
	}
	if c.GrowthFrozen != nil {
		topo.SetGrowthFrozen(*c.GrowthFrozen, c.GrowthFrozenAt, c.GrowthUnfreezeFor)
	}
	if err := topo.PersistMaxVolumeId(c.MaxVolumeId); err != nil {
		glog.Warningf("persist max volume id %d: %v", c.MaxVolumeId, err)
	}
	glog.V(1).Infoln("max volume id", before, "==>", topo.GetMaxVolumeId())

	return nil, nil
}

func (s *MaxVolumeIdCommand) Persist(sink hashicorpRaft.SnapshotSink) error {
	b, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	_, err = sink.Write(b)
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("sink.Write(): %w", err)
	}
	return sink.Close()
}

func (s *MaxVolumeIdCommand) Release() {
}
