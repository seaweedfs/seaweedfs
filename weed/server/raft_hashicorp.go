package weed_server

// https://yusufs.medium.com/creating-distributed-kv-database-by-implementing-raft-consensus-using-golang-d0884eef2e28
// https://github.com/Jille/raft-grpc-example/blob/cd5bcab0218f008e044fbeee4facdd01b06018ad/application.go#L18

import (
	"context"
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

func NewHashicorpRaftServer(ctx context.Context, option *RaftServerOption) (*RaftServer, error) {
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
	if glog.V(4) {
		c.Logger.SetLevel(1)
	} else if glog.V(3) {
		c.Logger.SetLevel(2)
	} else if glog.V(2) {
		c.Logger.SetLevel(3)
	} else if glog.V(1) {
		c.Logger.SetLevel(4)
	} else if glog.V(0) {
		c.Logger.SetLevel(5)
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

	// s.GrpcServer = raft.NewGrpcServer(s.raftServer)
	s.TransportManager = transport.New(raft.ServerAddress(s.serverAddr), []grpc.DialOption{option.GrpcDialOption})

	stateMachine := StateMachine{topo: option.Topo}
	r, err := raft.NewRaft(c, &stateMachine, ldb, sdb, fss, s.TransportManager.Transport())
	if err != nil {
		return nil, fmt.Errorf("raft.NewRaft: %v", err)
	}

	if option.RaftBootstrap {
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       c.LocalID,
					Address:  raft.ServerAddress(s.serverAddr),
				},
			},
		}
		// Add known peers to bootstrap
		for _, node := range option.Peers {
			if node == option.ServerAddr {
				continue
			}
			cfg.Servers = append(cfg.Servers, raft.Server{
				Suffrage: raft.Voter,
				ID:       raft.ServerID(node),
				Address:  raft.ServerAddress(node),
			})
		}
		f := r.BootstrapCluster(cfg)
		if err := f.Error(); err != nil {
			return nil, fmt.Errorf("raft.Raft.BootstrapCluster: %v", err)
		}
	}

	return s, nil
}
