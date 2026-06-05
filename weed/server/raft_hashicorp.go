package weed_server

// https://yusufs.medium.com/creating-distributed-kv-database-by-implementing-raft-consensus-using-golang-d0884eef2e28
// https://github.com/Jille/raft-grpc-example/blob/cd5bcab0218f008e044fbeee4facdd01b06018ad/application.go#L18

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path"
	"path/filepath"
	"sort"
	"time"

	transport "github.com/Jille/raft-grpc-transport"
	"github.com/armon/go-metrics"
	"github.com/armon/go-metrics/prometheus"
	"github.com/hashicorp/raft"
	hashicorpRaft "github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/topology"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
	"google.golang.org/grpc"
)

const (
	ldbFile            = "logs.dat"
	sdbFile            = "stable.dat"
	updatePeersTimeout = 15 * time.Minute

	// legacyMigrationKey marks, in the hashicorp stable store, that this
	// master's hashicorp raft state has already absorbed any pre-existing
	// legacy seaweedfs/raft state. Its presence makes the import one-time so
	// later restarts never re-read stale legacy state.
	legacyMigrationKey = "seaweedfs.legacy.raft.migrated"
)

// migrateLegacyRaftStateIfNeeded performs the one-time legacy -> hashicorp raft
// migration. It first checks the migration marker in the hashicorp stable
// store; if already migrated it does nothing. Otherwise, when a legacy
// seaweedfs/raft state is present, it imports the cluster identity
// (TopologyId) and MaxVolumeId from it, then marks the migration done. With no
// legacy state it just records the marker. Either way the master ends up
// marked migrated, so the import runs at most once per cluster.
func migrateLegacyRaftStateIfNeeded(sdb *boltdb.BoltStore, dataDir string, topo *topology.Topology) {
	if _, err := sdb.Get([]byte(legacyMigrationKey)); err == nil {
		return // marker present: already migrated
	}
	if legacyRaftStateExists(dataDir) {
		glog.V(0).Infof("first hashicorp-raft start with legacy raft state in %s; migrating", dataDir)
		importLegacyRaftState(dataDir, topo)
	}
	if err := sdb.Set([]byte(legacyMigrationKey), []byte(version.Version())); err != nil {
		glog.Warningf("failed to record legacy raft migration marker: %v", err)
	}
}

// importLegacyRaftState seeds TopologyId and MaxVolumeId from the latest legacy
// snapshot. TopologyId is the cluster identity and must survive the engine
// switch; MaxVolumeId is otherwise rebuilt from volume heartbeats, but carrying
// it avoids reusing an id whose volume was deleted before the migration.
func importLegacyRaftState(dataDir string, topo *topology.Topology) {
	state, ok := readLegacyRaftSnapshotState(dataDir)
	if !ok {
		glog.V(0).Infof("legacy raft state present but no readable snapshot; " +
			"TopologyId/MaxVolumeId will be rebuilt from volume heartbeats")
		return
	}
	var cmd topology.MaxVolumeIdCommand
	if err := json.Unmarshal(state, &cmd); err != nil {
		glog.Warningf("failed to parse legacy raft snapshot state: %v", err)
		return
	}
	topo.UpAdjustMaxVolumeId(cmd.MaxVolumeId)
	if cmd.TopologyId != "" {
		topo.SetTopologyId(cmd.TopologyId)
	}
	glog.V(0).Infof("migrated legacy raft state: MaxVolumeId=%d TopologyId=%s", cmd.MaxVolumeId, cmd.TopologyId)
}

func getPeerIdx(self pb.ServerAddress, mapPeers map[string]pb.ServerAddress) int {
	peerIDs := make([]string, 0, len(mapPeers))
	seen := make(map[string]struct{}, len(mapPeers))
	for _, peer := range mapPeers {
		id := raftServerID(peer)
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		peerIDs = append(peerIDs, id)
	}
	sort.Strings(peerIDs)
	selfID := raftServerID(self)
	idx := sort.SearchStrings(peerIDs, selfID)
	if idx < len(peerIDs) && peerIDs[idx] == selfID {
		return idx
	}
	return -1
}

func raftServerID(server pb.ServerAddress) string {
	return server.ToHttpAddress()
}

// recoverTopologyIdFromHashicorpSnapshot reads the TopologyId from the latest
// hashicorp raft snapshot before state cleanup.
func recoverTopologyIdFromHashicorpSnapshot(dataDir string, topo *topology.Topology) {
	fss, err := raft.NewFileSnapshotStore(dataDir, 1, io.Discard)
	if err != nil {
		return
	}
	snapshots, err := fss.List()
	if err != nil || len(snapshots) == 0 {
		return
	}
	_, rc, err := fss.Open(snapshots[0].ID)
	if err != nil {
		return
	}
	defer rc.Close()

	if b, err := io.ReadAll(rc); err == nil {
		recoverTopologyIdFromState(b, topo)
	}
}

func (s *RaftServer) AddPeersConfiguration() (cfg raft.Configuration) {
	for _, peer := range s.peers {
		cfg.Servers = append(cfg.Servers, raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(raftServerID(peer)),
			Address:  raft.ServerAddress(peer.ToGrpcAddress()),
		})
	}
	return cfg
}

func (s *RaftServer) monitorLeaderLoop(updatePeers bool) {
	for {
		prevLeader, _ := s.RaftHashicorp.LeaderWithID()
		select {
		case isLeader := <-s.RaftHashicorp.LeaderCh():
			leader, _ := s.RaftHashicorp.LeaderWithID()
			s.topo.SetLastLeaderChangeTime(time.Now())
			if isLeader {

				if updatePeers {
					s.updatePeers()
					updatePeers = false
				}

				s.topo.DoBarrier()

				EnsureTopologyId(s.topo, func() bool {
					return s.RaftHashicorp.State() == hashicorpRaft.Leader
				}, func(topologyId string) error {
					command := topology.NewMaxVolumeIdCommand(s.topo.GetMaxVolumeId(), topologyId)
					b, err := json.Marshal(command)
					if err != nil {
						return err
					}
					return s.RaftHashicorp.Apply(b, 5*time.Second).Error()
				})

				stats.MasterLeaderChangeCounter.WithLabelValues(fmt.Sprintf("%+v", leader)).Inc()
			} else {
				s.topo.BarrierReset()
			}
			glog.V(0).Infof("is leader %+v change event: %+v => %+v", isLeader, prevLeader, leader)
			prevLeader = leader
		}
	}
}

func (s *RaftServer) updatePeers() {
	peerLeader := raftServerID(s.serverAddr)
	desiredPeers := make(map[string]pb.ServerAddress, len(s.peers))
	for _, peer := range s.peers {
		desiredPeers[raftServerID(peer)] = peer
	}

	existsPeerName := make(map[string]bool)
	for _, server := range s.RaftHashicorp.GetConfiguration().Configuration().Servers {
		if string(server.ID) == peerLeader {
			continue
		}
		existsPeerName[string(server.ID)] = true
	}
	for peerName, peer := range desiredPeers {
		if peerName == peerLeader || existsPeerName[peerName] {
			continue
		}
		glog.V(0).Infof("adding new peer: %s", peerName)
		s.RaftHashicorp.AddVoter(
			raft.ServerID(peerName), raft.ServerAddress(peer.ToGrpcAddress()), 0, 0)
	}
	for peer := range existsPeerName {
		if _, found := desiredPeers[peer]; !found {
			glog.V(0).Infof("removing old peer: %s", peer)
			s.RaftHashicorp.RemoveServer(raft.ServerID(peer), 0, 0)
		}
	}
	if _, found := desiredPeers[peerLeader]; !found {
		glog.V(0).Infof("removing old leader peer: %s", peerLeader)
		s.RaftHashicorp.RemoveServer(raft.ServerID(peerLeader), 0, 0)
	}
}

func NewHashicorpRaftServer(option *RaftServerOption) (*RaftServer, error) {
	s := &RaftServer{
		peers:      option.Peers,
		serverAddr: option.ServerAddr,
		dataDir:    option.DataDir,
		topo:       option.Topo,
	}

	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(raftServerID(s.serverAddr))
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

	if err := raft.ValidateConfig(c); err != nil {
		return nil, fmt.Errorf("raft.ValidateConfig: %w", err)
	}

	if option.RaftBootstrap {
		recoverTopologyIdFromHashicorpSnapshot(s.dataDir, option.Topo)

		os.RemoveAll(path.Join(s.dataDir, ldbFile))
		os.RemoveAll(path.Join(s.dataDir, sdbFile))
		os.RemoveAll(path.Join(s.dataDir, "snapshots"))
	}
	if err := os.MkdirAll(path.Join(s.dataDir, "snapshots"), os.ModePerm); err != nil {
		return nil, err
	}
	baseDir := s.dataDir

	ldb, err := boltdb.NewBoltStore(filepath.Join(baseDir, ldbFile))
	if err != nil {
		return nil, fmt.Errorf("boltdb.NewBoltStore(%q): %v", filepath.Join(baseDir, "logs.dat"), err)
	}

	sdb, err := boltdb.NewBoltStore(filepath.Join(baseDir, sdbFile))
	if err != nil {
		return nil, fmt.Errorf("boltdb.NewBoltStore(%q): %v", filepath.Join(baseDir, "stable.dat"), err)
	}

	// Carry cluster identity forward when upgrading from legacy seaweedfs/raft.
	// Runs once, before raft starts, so the seeded TopologyId is in place
	// before ensureTopologyId would otherwise generate a fresh one.
	migrateLegacyRaftStateIfNeeded(sdb, baseDir, option.Topo)

	fss, err := raft.NewFileSnapshotStore(baseDir, 3, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("raft.NewFileSnapshotStore(%q, ...): %v", baseDir, err)
	}

	s.TransportManager = transport.New(raft.ServerAddress(s.serverAddr), []grpc.DialOption{option.GrpcDialOption})

	stateMachine := StateMachine{topo: option.Topo}
	s.RaftHashicorp, err = raft.NewRaft(c, &stateMachine, ldb, sdb, fss, s.TransportManager.Transport())
	if err != nil {
		return nil, fmt.Errorf("raft.NewRaft: %w", err)
	}

	updatePeers := false
	if option.RaftBootstrap || len(s.RaftHashicorp.GetConfiguration().Configuration().Servers) == 0 {
		cfg := s.AddPeersConfiguration()
		// Need to get lock, in case all servers do this at the same time.
		peerIdx := getPeerIdx(s.serverAddr, s.peers)
		timeSleep := time.Duration(float64(c.LeaderLeaseTimeout) * (rand.Float64()*0.25 + 1) * float64(peerIdx))
		glog.V(0).Infof("Bootstrapping idx: %d sleep: %v new cluster: %+v", peerIdx, timeSleep, cfg)
		time.Sleep(timeSleep)
		f := s.RaftHashicorp.BootstrapCluster(cfg)
		if err := f.Error(); err != nil {
			return nil, fmt.Errorf("raft.Raft.BootstrapCluster: %w", err)
		}
	} else {
		updatePeers = true
	}

	go s.monitorLeaderLoop(updatePeers)

	ticker := time.NewTicker(c.HeartbeatTimeout * 10)
	if glog.V(4) {
		go func() {
			for {
				select {
				case <-ticker.C:
					cfuture := s.RaftHashicorp.GetConfiguration()
					if err = cfuture.Error(); err != nil {
						glog.Fatalf("error getting config: %s", err)
					}
					configuration := cfuture.Configuration()
					glog.V(4).Infof("Showing peers known by %s:\n%+v", s.RaftHashicorp.String(), configuration.Servers)
				}
			}
		}()
	}

	// Configure a prometheus sink as the raft metrics sink
	if sink, err := prometheus.NewPrometheusSinkFrom(prometheus.PrometheusOpts{
		Registerer: stats.Gather,
	}); err != nil {
		return nil, fmt.Errorf("NewPrometheusSink: %w", err)
	} else {
		metricsConf := metrics.DefaultConfig(stats.Namespace)
		metricsConf.EnableRuntimeMetrics = false
		if _, err = metrics.NewGlobal(metricsConf, sink); err != nil {
			return nil, fmt.Errorf("metrics.NewGlobal: %w", err)
		}
	}

	return s, nil
}
