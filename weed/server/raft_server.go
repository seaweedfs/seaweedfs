package weed_server

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path"
	"time"

	transport "github.com/Jille/raft-grpc-transport"
	boltdb "github.com/hashicorp/raft-boltdb/v2"

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

	// Also initialize hashicorp raft in parallel for seamless future migration
	if err := s.initHashicorpRaftForDualWrite(option); err != nil {
		glog.Warningf("failed to initialize hashicorp raft for dual-write, migration will require manual steps: %v", err)
	} else {
		glog.V(0).Infof("hashicorp raft initialized for dual-write migration")
	}

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

// initHashicorpRaftForDualWrite initializes hashicorp raft alongside seaweedfs/raft
// for seamless future migration. State changes are written to both implementations.
func (s *RaftServer) initHashicorpRaftForDualWrite(option *RaftServerOption) error {
	c := hashicorpRaft.DefaultConfig()
	c.LocalID = hashicorpRaft.ServerID(s.serverAddr)
	c.HeartbeatTimeout = time.Duration(float64(option.HeartbeatInterval) * (rand.Float64()*0.25 + 1))
	c.ElectionTimeout = option.ElectionTimeout
	if c.LeaderLeaseTimeout > c.HeartbeatTimeout {
		c.LeaderLeaseTimeout = c.HeartbeatTimeout
	}
	if glog.V(4) {
		c.LogLevel = "Debug"
	} else if glog.V(2) {
		c.LogLevel = "Info"
	} else if glog.V(1) {
		c.LogLevel = "Warn"
	} else if glog.V(0) {
		c.LogLevel = "Error"
	}

	if err := hashicorpRaft.ValidateConfig(c); err != nil {
		return fmt.Errorf("raft.ValidateConfig: %w", err)
	}

	// Use a subdirectory to keep hashicorp raft data separate from old raft
	hashicorpDataDir := path.Join(s.dataDir, "hashicorp")
	if option.RaftBootstrap {
		os.RemoveAll(path.Join(hashicorpDataDir, ldbFile))
		os.RemoveAll(path.Join(hashicorpDataDir, sdbFile))
		os.RemoveAll(path.Join(hashicorpDataDir, "snapshots"))
	}
	if err := os.MkdirAll(path.Join(hashicorpDataDir, "snapshots"), os.ModePerm); err != nil {
		return err
	}

	ldb, err := boltdb.NewBoltStore(path.Join(hashicorpDataDir, ldbFile))
	if err != nil {
		return fmt.Errorf("boltdb.NewBoltStore(%q): %v", path.Join(hashicorpDataDir, ldbFile), err)
	}

	sdb, err := boltdb.NewBoltStore(path.Join(hashicorpDataDir, sdbFile))
	if err != nil {
		return fmt.Errorf("boltdb.NewBoltStore(%q): %v", path.Join(hashicorpDataDir, sdbFile), err)
	}

	fss, err := hashicorpRaft.NewFileSnapshotStore(hashicorpDataDir, 3, os.Stderr)
	if err != nil {
		return fmt.Errorf("raft.NewFileSnapshotStore(%q, ...): %v", hashicorpDataDir, err)
	}

	s.TransportManager = transport.New(hashicorpRaft.ServerAddress(s.serverAddr), []grpc.DialOption{option.GrpcDialOption})

	stateMachine := StateMachine{topo: option.Topo}
	s.RaftHashicorp, err = hashicorpRaft.NewRaft(c, &stateMachine, ldb, sdb, fss, s.TransportManager.Transport())
	if err != nil {
		return fmt.Errorf("raft.NewRaft: %w", err)
	}

	if option.RaftBootstrap || len(s.RaftHashicorp.GetConfiguration().Configuration().Servers) == 0 {
		cfg := s.AddPeersConfiguration()
		peerIdx := getPeerIdx(s.serverAddr, s.peers)
		timeSleep := time.Duration(float64(c.LeaderLeaseTimeout) * (rand.Float64()*0.25 + 1) * float64(peerIdx))
		glog.V(0).Infof("Dual-write: Bootstrapping hashicorp raft idx: %d sleep: %v cluster: %+v", peerIdx, timeSleep, cfg)
		time.Sleep(timeSleep)
		f := s.RaftHashicorp.BootstrapCluster(cfg)
		if err := f.Error(); err != nil {
			return fmt.Errorf("raft.Raft.BootstrapCluster: %w", err)
		}
	}

	return nil
}

// SyncHashicorpRaftLeadership transfers hashicorp raft leadership to match seaweedfs/raft leader.
// This ensures both raft systems have the same leader for dual-write to work correctly.
func (s *RaftServer) SyncHashicorpRaftLeadership(seaweedfsRaftLeader string) {
	if s.RaftHashicorp == nil || seaweedfsRaftLeader == "" {
		return
	}

	hashicorpLeader, _ := s.RaftHashicorp.LeaderWithID()
	targetLeader := pb.ServerAddress(seaweedfsRaftLeader)

	// If hashicorp raft already has the same leader, nothing to do
	if string(hashicorpLeader) == targetLeader.ToGrpcAddress() {
		glog.V(1).Infof("Dual-write: hashicorp raft leader already matches seaweedfs/raft leader: %s", seaweedfsRaftLeader)
		return
	}

	// Only the current hashicorp raft leader can transfer leadership
	if s.RaftHashicorp.State() != hashicorpRaft.Leader {
		glog.V(1).Infof("Dual-write: not hashicorp raft leader, cannot transfer leadership to %s", seaweedfsRaftLeader)
		return
	}

	// Transfer leadership to the seaweedfs/raft leader
	glog.V(0).Infof("Dual-write: transferring hashicorp raft leadership to match seaweedfs/raft leader: %s", seaweedfsRaftLeader)
	future := s.RaftHashicorp.LeadershipTransferToServer(
		hashicorpRaft.ServerID(targetLeader),
		hashicorpRaft.ServerAddress(targetLeader.ToGrpcAddress()),
	)
	if err := future.Error(); err != nil {
		glog.Warningf("Dual-write: failed to transfer hashicorp raft leadership to %s: %v", seaweedfsRaftLeader, err)
	} else {
		glog.V(0).Infof("Dual-write: hashicorp raft leadership transferred to %s", seaweedfsRaftLeader)
	}
}
