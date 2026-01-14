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
		topo.SetTopologyId(c.TopologyId)
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
