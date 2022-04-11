package topology

import (
	"encoding/json"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/hashicorp/raft"
)

type MaxVolumeIdCommand struct {
	MaxVolumeId needle.VolumeId `json:"maxVolumeId"`
}

func NewMaxVolumeIdCommand(value needle.VolumeId) *MaxVolumeIdCommand {
	return &MaxVolumeIdCommand{
		MaxVolumeId: value,
	}
}

func (c *MaxVolumeIdCommand) CommandName() string {
	return "MaxVolumeId"
}

func (s *MaxVolumeIdCommand) Persist(sink raft.SnapshotSink) error {
	b, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("marshal: %v", err)
	}
	_, err = sink.Write(b)
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("sink.Write(): %v", err)
	}
	return sink.Close()
}

func (s *MaxVolumeIdCommand) Release() {
}
