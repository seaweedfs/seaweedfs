package weed_server

import (
	"encoding/json"
	"github.com/chrislusf/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"sort"
	"time"

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

func NewRaftServer(grpcDialOption grpc.DialOption, peers []string, serverAddr string, dataDir string, topo *topology.Topology, pulseSeconds int) *RaftServer {
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

	// Clear old cluster configurations if peers are changed
	if oldPeers, changed := isPeersChanged(s.dataDir, serverAddr, s.peers); changed {
		glog.V(0).Infof("Peers Change: %v => %v", oldPeers, s.peers)
		os.RemoveAll(path.Join(s.dataDir, "conf"))
		os.RemoveAll(path.Join(s.dataDir, "log"))
		os.RemoveAll(path.Join(s.dataDir, "snapshot"))
	}

	s.raftServer, err = raft.NewServer(s.serverAddr, s.dataDir, transporter, nil, topo, "")
	if err != nil {
		glog.V(0).Infoln(err)
		return nil
	}
	s.raftServer.SetHeartbeatInterval(500 * time.Millisecond)
	s.raftServer.SetElectionTimeout(time.Duration(pulseSeconds) * 500 * time.Millisecond)
	s.raftServer.Start()

	for _, peer := range s.peers {
		s.raftServer.AddPeer(peer, util.ServerToGrpcAddress(peer, 19333))
	}

	s.GrpcServer = raft.NewGrpcServer(s.raftServer)

	if s.raftServer.IsLogEmpty() && isTheFirstOne(serverAddr, s.peers) {
		// Initialize the server by joining itself.
		glog.V(0).Infoln("Initializing new cluster")

		_, err := s.raftServer.Do(&raft.DefaultJoinCommand{
			Name:             s.raftServer.Name(),
			ConnectionString: util.ServerToGrpcAddress(s.serverAddr, 19333),
		})

		if err != nil {
			glog.V(0).Infoln(err)
			return nil
		}
	}

	glog.V(0).Infof("current cluster leader: %v", s.raftServer.Leader())

	return s
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
