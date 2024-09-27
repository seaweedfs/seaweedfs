package weed_server

import (
	"encoding/json"
	"io"
	"math/rand/v2"
	"os"
	"path"
	"time"

	transport "github.com/Jille/raft-grpc-transport"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/pb"

	hashicorpRaft "github.com/hashicorp/raft"
	"github.com/seaweedfs/raft"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

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
	raftServer       raft.Server
	RaftHashicorp    *hashicorpRaft.Raft
	TransportManager *transport.Manager
	dataDir          string
	serverAddr       pb.ServerAddress
	topo             *topology.Topology
	*raft.GrpcServer
}

type StateMachine struct {
	raft.StateMachine
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

func NewRaftServer(option *RaftServerOption) (*RaftServer, error) {
	s := &RaftServer{
		peers:      option.Peers,
		serverAddr: option.ServerAddr,
		dataDir:    option.DataDir,
		topo:       option.Topo,
	}

	if glog.V(4) {
		raft.SetLogLevel(2)
	}

	raft.RegisterCommand(&topology.MaxVolumeIdCommand{})

	var err error
	transporter := raft.NewGrpcTransporter(option.GrpcDialOption)
	glog.V(0).Infof("Starting RaftServer with %v", option.ServerAddr)

	// always clear previous log to avoid server is promotable
	os.RemoveAll(path.Join(s.dataDir, "log"))
	if !option.RaftResumeState {
		// always clear previous metadata
		os.RemoveAll(path.Join(s.dataDir, "conf"))
		os.RemoveAll(path.Join(s.dataDir, "snapshot"))
	}
	if err := os.MkdirAll(path.Join(s.dataDir, "snapshot"), os.ModePerm); err != nil {
		return nil, err
	}

	stateMachine := StateMachine{topo: option.Topo}
	s.raftServer, err = raft.NewServer(string(s.serverAddr), s.dataDir, transporter, stateMachine, option.Topo, s.serverAddr.ToGrpcAddress())
	if err != nil {
		glog.V(0).Infoln(err)
		return nil, err
	}
	heartbeatInterval := time.Duration(float64(option.HeartbeatInterval) * (rand.Float64()*0.25 + 1))
	s.raftServer.SetHeartbeatInterval(heartbeatInterval)
	s.raftServer.SetElectionTimeout(option.ElectionTimeout)
	if err := s.raftServer.LoadSnapshot(); err != nil {
		return nil, err
	}
	if err := s.raftServer.Start(); err != nil {
		return nil, err
	}

	for name, peer := range s.peers {
		if err := s.raftServer.AddPeer(name, peer.ToGrpcAddress()); err != nil {
			return nil, err
		}
	}

	// Remove deleted peers
	for existsPeerName := range s.raftServer.Peers() {
		if existingPeer, found := s.peers[existsPeerName]; !found {
			if err := s.raftServer.RemovePeer(existsPeerName); err != nil {
				glog.V(0).Infoln(err)
				return nil, err
			} else {
				glog.V(0).Infof("removing old peer: %s", existingPeer)
			}
		}
	}

	s.GrpcServer = raft.NewGrpcServer(s.raftServer)

	glog.V(0).Infof("current cluster leader: %v", s.raftServer.Leader())

	return s, nil
}

func (s *RaftServer) Peers() (members []string) {
	if s.raftServer != nil {
		peers := s.raftServer.Peers()
		for _, p := range peers {
			members = append(members, p.Name)
		}
	} else if s.RaftHashicorp != nil {
		cfg := s.RaftHashicorp.GetConfiguration()
		for _, p := range cfg.Configuration().Servers {
			members = append(members, string(p.ID))
		}
	}
	return
}

func (s *RaftServer) DoJoinCommand() {

	glog.V(0).Infoln("Initializing new cluster")

	if _, err := s.raftServer.Do(&raft.DefaultJoinCommand{
		Name:             s.raftServer.Name(),
		ConnectionString: s.serverAddr.ToGrpcAddress(),
	}); err != nil {
		glog.Errorf("fail to send join command: %v", err)
	}

}
