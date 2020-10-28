package weed_server

import (
	"encoding/json"
	"math/rand"
	"os"
	"path"
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

func NewRaftServer(grpcDialOption grpc.DialOption, peers []string, serverAddr, dataDir string, topo *topology.Topology, raftResumeState bool) (*RaftServer, error) {
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

	if !raftResumeState {
		// always clear previous metadata
		os.RemoveAll(path.Join(s.dataDir, "conf"))
		os.RemoveAll(path.Join(s.dataDir, "log"))
		os.RemoveAll(path.Join(s.dataDir, "snapshot"))
	}
	if err := os.MkdirAll(path.Join(s.dataDir, "snapshot"), 0600); err != nil {
		return nil, err
	}

	stateMachine := StateMachine{topo: topo}
	s.raftServer, err = raft.NewServer(s.serverAddr, s.dataDir, transporter, stateMachine, topo, "")
	if err != nil {
		glog.V(0).Infoln(err)
		return nil, err
	}
	s.raftServer.SetHeartbeatInterval(time.Duration(300+rand.Intn(150)) * time.Millisecond)
	s.raftServer.SetElectionTimeout(10 * time.Second)
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

	// Remove deleted peers
	for existsPeerName := range s.raftServer.Peers() {
		exists, existingPeer := false, ""
		for _, peer := range s.peers {
			if pb.ServerToGrpcAddress(peer) == existsPeerName {
				exists, existingPeer = true, peer
				break
			}
		}
		if exists {
			if err := s.raftServer.RemovePeer(existsPeerName); err != nil {
				glog.V(0).Infoln(err)
				return nil, err
			} else {
				glog.V(0).Infof("removing old peer %s", existingPeer)
			}
		}
	}

	s.GrpcServer = raft.NewGrpcServer(s.raftServer)

	if s.raftServer.IsLogEmpty() && isTheFirstOne(serverAddr, s.peers) {
		// Initialize the server by joining itself.
		// s.DoJoinCommand()
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

func isTheFirstOne(self string, peers []string) bool {
	sort.Strings(peers)
	if len(peers) <= 0 {
		return true
	}
	return self == peers[0]
}

func (s *RaftServer) DoJoinCommand() {

	glog.V(0).Infoln("Initializing new cluster")

	if _, err := s.raftServer.Do(&raft.DefaultJoinCommand{
		Name:             s.raftServer.Name(),
		ConnectionString: pb.ServerToGrpcAddress(s.serverAddr),
	}); err != nil {
		glog.Errorf("fail to send join command: %v", err)
	}

}
