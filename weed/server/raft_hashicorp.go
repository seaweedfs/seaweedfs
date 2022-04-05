package weed_server

// https://yusufs.medium.com/creating-distributed-kv-database-by-implementing-raft-consensus-using-golang-d0884eef2e28
// https://github.com/Jille/raft-grpc-example/blob/cd5bcab0218f008e044fbeee4facdd01b06018ad/application.go#L18

import (
	"fmt"
	transport "github.com/Jille/raft-grpc-transport"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
	"google.golang.org/grpc"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

func (s *RaftServer) AddPeersConfiguration() (cfg raft.Configuration) {
	for _, peer := range s.peers {
		cfg.Servers = append(cfg.Servers, raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(peer.String()),
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
				peerLeader := s.serverAddr.String()
				existsPeerName := make(map[string]bool)
				for _, server := range s.RaftHashicorp.GetConfiguration().Configuration().Servers {
					if string(server.ID) == peerLeader {
						continue
					}
					existsPeerName[string(server.ID)] = true
				}
				for _, peer := range s.peers {
					if peer.String() == peerLeader || existsPeerName[peer.String()] {
						continue
					}
					glog.V(0).Infof("adding new peer: %s", peer.String())
					s.RaftHashicorp.AddVoter(
						raft.ServerID(peer.String()), raft.ServerAddress(peer.ToGrpcAddress()), 0, 0)
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
			break
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
	c.LocalID = raft.ServerID(s.serverAddr.String()) // TODO maybee the IP:port address will change
	c.NoSnapshotRestoreOnStart = option.RaftResumeState
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

	baseDir := s.dataDir

	ldb, err := boltdb.NewBoltStore(filepath.Join(baseDir, "logs.dat"))
	if err != nil {
		return nil, fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, "logs.dat"), err)
	}

	sdb, err := boltdb.NewBoltStore(filepath.Join(baseDir, "stable.dat"))
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
		glog.V(0).Infoln("Bootstrapping new cluster %+v", cfg)
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
