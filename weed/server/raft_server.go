package weed_server

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand/v2"
	"os"
	"path"
	"sort"
	"sync"
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
	SingleMaster      bool
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
		TopologyId:  s.topo.GetTopologyId(),
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
	if state.TopologyId != "" {
		s.topo.SetTopologyId(state.TopologyId)
		glog.V(0).Infof("Recovered TopologyId: %s", state.TopologyId)
	}
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
	if state.TopologyId != "" {
		prevTopologyId := s.topo.GetTopologyId()
		s.topo.SetTopologyId(state.TopologyId)
		// Log when recovering TopologyId from Raft log replay, or setting it for the first time.
		if prevTopologyId == "" {
			glog.V(0).Infof("Set TopologyId from raft log: %s", state.TopologyId)
		}
	}

	glog.V(1).Infoln("max volume id", before, "==>", s.topo.GetMaxVolumeId())
	return nil
}

func (s *StateMachine) Snapshot() (hashicorpRaft.FSMSnapshot, error) {
	return &topology.MaxVolumeIdCommand{
		MaxVolumeId: s.topo.GetMaxVolumeId(),
		TopologyId:  s.topo.GetTopologyId(),
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

var registerMaxVolumeIdCommandOnce sync.Once

func registerMaxVolumeIdCommand() {
	registerMaxVolumeIdCommandOnce.Do(func() {
		raft.RegisterCommand(&topology.MaxVolumeIdCommand{})
	})
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

	registerMaxVolumeIdCommand()

	var err error
	transporter := raft.NewGrpcTransporter(option.GrpcDialOption)
	glog.V(0).Infof("Starting RaftServer with %v", option.ServerAddr)

	if !option.RaftResumeState {
		// Recover the TopologyId from the snapshot before clearing state.
		// The TopologyId is a cluster identity used for license validation
		// and must survive raft state cleanup.
		recoverTopologyIdFromSnapshot(s.dataDir, option.Topo)

		// clear previous log to ensure fresh start
		os.RemoveAll(path.Join(s.dataDir, "log"))
		// always clear previous metadata
		os.RemoveAll(path.Join(s.dataDir, "conf"))
		os.RemoveAll(path.Join(s.dataDir, "snapshot"))
		// clear persisted vote state (currentTerm/votedFor) so that stale
		// terms from previous runs cannot cause election conflicts when the
		// log has been wiped.
		os.Remove(path.Join(s.dataDir, "state"))
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

	// In single-master mode resuming state, the log is not empty so the
	// normal self-join path won't promote to leader. The server will
	// self-elect after the election timeout, so use a tiny timeout to
	// make this near-instant, then restore the original after election.
	fastResume := option.SingleMaster && option.RaftResumeState && !s.raftServer.IsLogEmpty()
	if fastResume {
		s.raftServer.SetElectionTimeout(time.Millisecond)
	}

	if err := s.raftServer.Start(); err != nil {
		return nil, err
	}

	if fastResume {
		go func() {
			defer s.raftServer.SetElectionTimeout(option.ElectionTimeout)
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			timeout := time.After(option.ElectionTimeout)
			for s.raftServer.Leader() == "" {
				select {
				case <-timeout:
					glog.Warningf("Fast resume timed out waiting for leader election, restoring election timeout to %v", option.ElectionTimeout)
					return
				case <-ticker.C:
				}
			}
			glog.V(0).Infof("Resumed as leader with election timeout restored to %v", option.ElectionTimeout)
		}()
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

// recoverTopologyIdFromSnapshot reads the TopologyId from the latest
// seaweedfs/raft snapshot before state cleanup.
func recoverTopologyIdFromSnapshot(dataDir string, topo *topology.Topology) {
	snapshotDir := path.Join(dataDir, "snapshot")
	dir, err := os.Open(snapshotDir)
	if err != nil {
		return
	}
	defer dir.Close()
	filenames, err := dir.Readdirnames(-1)
	if err != nil || len(filenames) == 0 {
		return
	}

	sort.Strings(filenames)
	file, err := os.Open(path.Join(snapshotDir, filenames[len(filenames)-1]))
	if err != nil {
		return
	}
	defer file.Close()

	// Snapshot format: 8-hex-digit CRC32 checksum, newline, JSON body.
	var checksum uint32
	if _, err := fmt.Fscanf(file, "%08x\n", &checksum); err != nil {
		return
	}
	b, err := io.ReadAll(file)
	if err != nil || crc32.ChecksumIEEE(b) != checksum {
		return
	}

	// The snapshot JSON wraps the FSM state in a "state" field.
	var snap struct {
		State json.RawMessage `json:"state"`
	}
	if err := json.Unmarshal(b, &snap); err != nil || len(snap.State) == 0 {
		return
	}
	recoverTopologyIdFromState(snap.State, topo)
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

