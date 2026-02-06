package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

const (
	StateFileName = "state.pb"
	StateFileMode = 0644
)

type State struct {
	filePath string
	pb       *volume_server_pb.VolumeServerState

	mu sync.Mutex
}

func NewState(dir string) (*State, error) {
	state := &State{
		filePath: filepath.Join(dir, StateFileName),
		pb:       nil,
	}

	err := state.Load()
	return state, err
}

func NewStateFromProto(filePath string, state *volume_server_pb.VolumeServerState) *State {
	pb := &volume_server_pb.VolumeServerState{}
	proto.Merge(pb, state)

	return &State{
		filePath: filePath,
		pb:       pb,
	}
}

func (st *State) Proto() *volume_server_pb.VolumeServerState {
	st.mu.Lock()
	defer st.mu.Unlock()

	return st.pb
}

func (st *State) Load() error {
	st.mu.Lock()
	defer st.mu.Unlock()

	st.pb = &volume_server_pb.VolumeServerState{}

	if !util.FileExists(st.filePath) {
		glog.V(1).Infof("No preexisting store state at %s", st.filePath)
		return nil
	}

	binPb, err := os.ReadFile(st.filePath)
	if err != nil {
		st.pb = nil
		return fmt.Errorf("failed to read store state from %s : %v", st.filePath, err)
	}
	if err := proto.Unmarshal(binPb, st.pb); err != nil {
		st.pb = nil
		return fmt.Errorf("failed to parse store state from %s : %v", st.filePath, err)
	}

	glog.V(1).Infof("Got store state from %s: %v", st.filePath, st.pb)
	return nil
}

func (st *State) save(locking bool) error {
	if locking {
		st.mu.Lock()
		defer st.mu.Unlock()
	}

	if st.pb == nil {
		st.pb = &volume_server_pb.VolumeServerState{}
	}

	binPb, err := proto.Marshal(st.pb)
	if err != nil {
		return fmt.Errorf("failed to serialize store state %v: %s", st.pb, err)
	}
	if err := util.WriteFile(st.filePath, binPb, StateFileMode); err != nil {
		return fmt.Errorf("failed to write store state to %s : %v", st.filePath, err)
	}

	glog.V(1).Infof("Saved store state %v to %s", st.pb, st.filePath)
	return nil
}

func (st *State) Save() error {
	return st.save(true)
}

func (st *State) Update(state *volume_server_pb.VolumeServerState) error {
	st.mu.Lock()
	defer st.mu.Unlock()

	if state == nil {
		return nil
	}
	if got, want := st.pb.GetVersion(), state.GetVersion(); got != want {
		return fmt.Errorf("version mismatch for VolumeServerState (got %d, want %d)", got, want)
	}

	origState := st.pb
	st.pb = &volume_server_pb.VolumeServerState{}
	proto.Merge(st.pb, state)
	st.pb.Version = st.pb.GetVersion() + 1

	err := st.save(false)
	if err != nil {
		// restore the original state upon save failures, to avoid skew between in-memory and disk state protos.
		st.pb = origState
	}

	return err
}
