package weed_server

import (
	"encoding/json"
	"math/rand"
	"os"
	"path"
	"time"

	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/pb"

	"github.com/chrislusf/raft"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/topology"
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
}

type RaftServer struct {
	peers      map[string]pb.ServerAddress // initial peers to join with
	raftServer raft.Server
	dataDir    string
	serverAddr pb.ServerAddress
	topo       *topology.Topology
	*raft.GrpcServer
}

type StateMachine struct {
	raft.StateMachine
	topo *topology.Topology
}

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
	if err := os.MkdirAll(path.Join(s.dataDir, "snapshot"), 0600); err != nil {
		return nil, err
	}

	stateMachine := StateMachine{topo: option.Topo}
	s.raftServer, err = raft.NewServer(string(s.serverAddr), s.dataDir, transporter, stateMachine, option.Topo, "")
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
	peers := s.raftServer.Peers()

	for _, p := range peers {
		members = append(members, p.Name)
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
