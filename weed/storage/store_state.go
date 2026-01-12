package storage

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/golang/protobuf/proto"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	StateFileName = "state.pb"
	StateFileMode = 0644
)

type State struct {
	FilePath string
	Pb       *volume_server_pb.VolumeServerState
}

func NewState(dir string) (*State, error) {
	state := &State{
		FilePath: filepath.Join(dir, StateFileName),
		Pb:       nil,
	}

	err := state.Load()
	return state, err
}

func (st *State) Load() error {
	st.Pb = &volume_server_pb.VolumeServerState{}

	if !util.FileExists(st.FilePath) {
		glog.V(1).Infof("No preexisting store state at %s", st.FilePath)
		return nil
	}

	binPb, err := os.ReadFile(st.FilePath)
	if err != nil {
		st.Pb = nil
		return fmt.Errorf("failed to read store state from %s : %v", st.FilePath, err)
	}
	if err := proto.Unmarshal(binPb, st.Pb); err != nil {
		st.Pb = nil
		return fmt.Errorf("failed to parse store state from %s : %v", st.FilePath, err)
	}

	glog.V(1).Infof("Got store state from %s: %v", st.FilePath, st.Pb)
	return nil
}

func (st *State) Save() error {
	if st.Pb == nil {
		st.Pb = &volume_server_pb.VolumeServerState{}
	}

	binPb, err := proto.Marshal(st.Pb)
	if err != nil {
		return fmt.Errorf("failed to serialize store state %v: %s", st.Pb, err)
	}
	if err := util.WriteFile(st.FilePath, binPb, StateFileMode); err != nil {
		return fmt.Errorf("failed to write store state to %s : %v", st.FilePath, err)
	}

	glog.V(1).Infof("Saved store state %v to %s", st.Pb, st.FilePath)
	return nil
}
