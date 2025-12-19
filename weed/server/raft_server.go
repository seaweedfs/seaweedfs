package weed_server

import (
	"encoding/json"
	"errors"
	"io"
	"time"

	transport "github.com/Jille/raft-grpc-transport"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/pb"

	hashicorpRaft "github.com/hashicorp/raft"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

// NotLeaderError is returned when an operation requires the node to be the leader
var NotLeaderError = errors.New("not a leader")

type RaftServerOption struct {
	GrpcDialOption    grpc.DialOption
	Peers             map[string]pb.ServerAddress
	ServerAddr        pb.ServerAddress
	DataDir           string
	Topo              *topology.Topology
	RaftResumeState   bool
	HeartbeatInterval time.Duration
	ElectionTimeout   time.Duration
	RaftBootstrap     bool
}

type RaftServer struct {
	peers            map[string]pb.ServerAddress // initial peers to join with
	RaftHashicorp    *hashicorpRaft.Raft
	TransportManager *transport.Manager
	dataDir          string
	serverAddr       pb.ServerAddress
	topo             *topology.Topology
}

type StateMachine struct {
	topo *topology.Topology
}

var _ hashicorpRaft.FSM = &StateMachine{}

func (s StateMachine) Save() ([]byte, error) {
	state := topology.MaxVolumeIdCommand{
		MaxVolumeId: s.topo.GetMaxVolumeId(),
	}
	glog.V(1).Infof("Save raft state %+v", state)
	return json.Marshal(state)
}

func (s StateMachine) Recovery(data []byte) error {
	state := topology.MaxVolumeIdCommand{}
	err := json.Unmarshal(data, &state)
	if err != nil {
		return err
	}
	glog.V(1).Infof("Recovery raft state %+v", state)
	s.topo.UpAdjustMaxVolumeId(state.MaxVolumeId)
	return nil
}

func (s *StateMachine) Apply(l *hashicorpRaft.Log) interface{} {
	before := s.topo.GetMaxVolumeId()
	state := topology.MaxVolumeIdCommand{}
	err := json.Unmarshal(l.Data, &state)
	if err != nil {
		return err
	}
	s.topo.UpAdjustMaxVolumeId(state.MaxVolumeId)

	glog.V(1).Infoln("max volume id", before, "==>", s.topo.GetMaxVolumeId())
	return nil
}

func (s *StateMachine) Snapshot() (hashicorpRaft.FSMSnapshot, error) {
	return &topology.MaxVolumeIdCommand{
		MaxVolumeId: s.topo.GetMaxVolumeId(),
	}, nil
}

func (s *StateMachine) Restore(r io.ReadCloser) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if err := s.Recovery(b); err != nil {
		return err
	}
	return nil
}

func (s *RaftServer) Peers() (members []string) {
	if s.RaftHashicorp != nil {
		cfg := s.RaftHashicorp.GetConfiguration()
		for _, p := range cfg.Configuration().Servers {
			members = append(members, string(p.ID))
		}
	}
	return
}
