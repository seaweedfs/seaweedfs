package weed_server

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"sort"
	"time"

	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/pb"

	"github.com/chrislusf/raft"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/topology"
)

type RaftServer struct {
	peers      []string // initial peers to join with
	raftServer raft.Server
	dataDir    string
	serverAddr string
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

func NewRaftServer(grpcDialOption grpc.DialOption, peers []string, serverAddr, dataDir string, topo *topology.Topology, pulseSeconds int, cleanState bool) (*RaftServer, error) {
	s := &RaftServer{
		peers:      peers,
		serverAddr: serverAddr,
		dataDir:    dataDir,
		topo:       topo,
	}

	if glog.V(4) {
		raft.SetLogLevel(2)
	}

	raft.RegisterCommand(&topology.MaxVolumeIdCommand{})

	var err error
	transporter := raft.NewGrpcTransporter(grpcDialOption)
	glog.V(0).Infof("Starting RaftServer with %v", serverAddr)

	if cleanState {
		// always clear previous metadata
		os.RemoveAll(path.Join(s.dataDir, "conf"))
		os.RemoveAll(path.Join(s.dataDir, "log"))
		os.RemoveAll(path.Join(s.dataDir, "snapshot"))
	}
	if err := os.MkdirAll(path.Join(s.dataDir, "snapshot"), 0600); err != nil {
		return nil, err
	}

	// Clear old cluster configurations if peers are changed
	if oldPeers, changed := isPeersChanged(s.dataDir, serverAddr, s.peers); changed {
		glog.V(0).Infof("Peers Change: %v => %v", oldPeers, s.peers)
	}

	stateMachine := StateMachine{topo: topo}
	s.raftServer, err = raft.NewServer(s.serverAddr, s.dataDir, transporter, stateMachine, topo, "")
	if err != nil {
		glog.V(0).Infoln(err)
		return nil, err
	}
	s.raftServer.SetHeartbeatInterval(500 * time.Millisecond)
	s.raftServer.SetElectionTimeout(time.Duration(pulseSeconds) * 500 * time.Millisecond)
	if err := s.raftServer.LoadSnapshot(); err != nil {
		return nil, err
	}
	if err := s.raftServer.Start(); err != nil {
		return nil, err
	}

	for _, peer := range s.peers {
		if err := s.raftServer.AddPeer(peer, pb.ServerToGrpcAddress(peer)); err != nil {
			return nil, err
		}

	}

	s.GrpcServer = raft.NewGrpcServer(s.raftServer)

	if s.raftServer.IsLogEmpty() && isTheFirstOne(serverAddr, s.peers) {
		// Initialize the server by joining itself.
		glog.V(0).Infoln("Initializing new cluster")

		_, err := s.raftServer.Do(&raft.DefaultJoinCommand{
			Name:             s.raftServer.Name(),
			ConnectionString: pb.ServerToGrpcAddress(s.serverAddr),
		})

		if err != nil {
			glog.V(0).Infoln(err)
			return nil, err
		}
	}

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

func isPeersChanged(dir string, self string, peers []string) (oldPeers []string, changed bool) {
	confPath := path.Join(dir, "conf")
	// open conf file
	b, err := ioutil.ReadFile(confPath)
	if err != nil {
		return oldPeers, true
	}
	conf := &raft.Config{}
	if err = json.Unmarshal(b, conf); err != nil {
		return oldPeers, true
	}

	for _, p := range conf.Peers {
		oldPeers = append(oldPeers, p.Name)
	}
	oldPeers = append(oldPeers, self)

	if len(peers) == 0 && len(oldPeers) <= 1 {
		return oldPeers, false
	}

	sort.Strings(peers)
	sort.Strings(oldPeers)

	return oldPeers, !reflect.DeepEqual(peers, oldPeers)

}

func isTheFirstOne(self string, peers []string) bool {
	sort.Strings(peers)
	if len(peers) <= 0 {
		return true
	}
	return self == peers[0]
}
