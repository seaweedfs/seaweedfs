package weed_server

// https://yusufs.medium.com/creating-distributed-kv-database-by-implementing-raft-consensus-using-golang-d0884eef2e28
// https://github.com/Jille/raft-grpc-example/blob/cd5bcab0218f008e044fbeee4facdd01b06018ad/application.go#L18

import (
	"fmt"
	transport "github.com/Jille/raft-grpc-transport"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
	"google.golang.org/grpc"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	ldbFile            = "logs.dat"
	sdbFile            = "stable.dat"
	updatePeersTimeout = 15 * time.Minute
)

func getPeerIdx(self pb.ServerAddress, mapPeers map[string]pb.ServerAddress) int {
	peers := make([]pb.ServerAddress, 0, len(mapPeers))
	for _, peer := range mapPeers {
		peers = append(peers, peer)
	}
	sort.Slice(peers, func(i, j int) bool {
		return strings.Compare(string(peers[i]), string(peers[j])) < 0
	})
	for i, peer := range peers {
		if string(peer) == string(self) {
			return i
		}
	}
	return -1
}

func (s *RaftServer) AddPeersConfiguration() (cfg raft.Configuration) {
	for _, peer := range s.peers {
		cfg.Servers = append(cfg.Servers, raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(peer),
			Address:  raft.ServerAddress(peer.ToGrpcAddress()),
		})
	}
	return cfg
}

func (s *RaftServer) UpdatePeers() {
	for {
		select {
		case isLeader := <-s.RaftHashicorp.LeaderCh():
			if isLeader {
				peerLeader := string(s.serverAddr)
				existsPeerName := make(map[string]bool)
				for _, server := range s.RaftHashicorp.GetConfiguration().Configuration().Servers {
					if string(server.ID) == peerLeader {
						continue
					}
					existsPeerName[string(server.ID)] = true
				}
				for _, peer := range s.peers {
					peerName := string(peer)
					if peerName == peerLeader || existsPeerName[peerName] {
						continue
					}
					glog.V(0).Infof("adding new peer: %s", peerName)
					s.RaftHashicorp.AddVoter(
						raft.ServerID(peerName), raft.ServerAddress(peer.ToGrpcAddress()), 0, 0)
				}
				for peer, _ := range existsPeerName {
					if _, found := s.peers[peer]; !found {
						glog.V(0).Infof("removing old peer: %s", peer)
						s.RaftHashicorp.RemoveServer(raft.ServerID(peer), 0, 0)
					}
				}
				if _, found := s.peers[peerLeader]; !found {
					glog.V(0).Infof("removing old leader peer: %s", peerLeader)
					s.RaftHashicorp.RemoveServer(raft.ServerID(peerLeader), 0, 0)
				}
			}
			return
		case <-time.After(updatePeersTimeout):
			return
		}
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
	c.LocalID = raft.ServerID(s.serverAddr) // TODO maybee the IP:port address will change
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

	if option.RaftBootstrap {
		os.RemoveAll(path.Join(s.dataDir, ldbFile))
		os.RemoveAll(path.Join(s.dataDir, sdbFile))
		os.RemoveAll(path.Join(s.dataDir, "snapshot"))
	}
	baseDir := s.dataDir

	ldb, err := boltdb.NewBoltStore(filepath.Join(baseDir, ldbFile))
	if err != nil {
		return nil, fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, "logs.dat"), err)
	}

	sdb, err := boltdb.NewBoltStore(filepath.Join(baseDir, sdbFile))
	if err != nil {
		return nil, fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, "stable.dat"), err)
	}

	fss, err := raft.NewFileSnapshotStore(baseDir, 3, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf(`raft.NewFileSnapshotStore(%q, ...): %v`, baseDir, err)
	}

	s.TransportManager = transport.New(raft.ServerAddress(s.serverAddr), []grpc.DialOption{option.GrpcDialOption})

	stateMachine := StateMachine{topo: option.Topo}
	s.RaftHashicorp, err = raft.NewRaft(c, &stateMachine, ldb, sdb, fss, s.TransportManager.Transport())
	if err != nil {
		return nil, fmt.Errorf("raft.NewRaft: %v", err)
	}
	if option.RaftBootstrap || len(s.RaftHashicorp.GetConfiguration().Configuration().Servers) == 0 {
		cfg := s.AddPeersConfiguration()
		// Need to get lock, in case all servers do this at the same time.
		peerIdx := getPeerIdx(s.serverAddr, s.peers)
		timeSpeep := time.Duration(float64(c.LeaderLeaseTimeout) * (rand.Float64()*0.25 + 1) * float64(peerIdx))
		glog.V(0).Infof("Bootstrapping idx: %d sleep: %v new cluster: %+v", peerIdx, timeSpeep, cfg)
		time.Sleep(timeSpeep)
		f := s.RaftHashicorp.BootstrapCluster(cfg)
		if err := f.Error(); err != nil {
			return nil, fmt.Errorf("raft.Raft.BootstrapCluster: %v", err)
		}
	} else {
		go s.UpdatePeers()
	}

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

	return s, nil
}
