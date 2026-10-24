package weed_server

import (
	"testing"
	"time"

	hashicorpRaft "github.com/hashicorp/raft"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

func TestSingleMasterNeverAdmitsRaftPeers(t *testing.T) {
	ms := &MasterServer{option: &MasterOption{SingleMaster: true, Master: "localhost:9333"}}

	// A foreign master announcing itself must not touch the admission path:
	// ms.Topo is deliberately nil so any admission attempt panics the test.
	if done := ms.AdmitRaftPeer("10.0.0.1:9333"); done != nil {
		t.Fatal("single-master mode admitted a foreign raft peer")
	}
}

func TestClusteredLeaderAdmitsMissingPeer(t *testing.T) {
	topo := topology.NewTopology("topo", nil, 128*1024*1024, 0, false)
	leader, leaderTransport := newInmemRaftLeader(t, "localhost:9333")
	topo.HashicorpRaft = leader
	ms := &MasterServer{
		option: &MasterOption{Master: "localhost:9333"},
		Topo:   topo,
	}

	peer := pb.ServerAddress("10.0.0.1:9333")
	name, missing := ms.missingRaftPeerName(peer)
	if !missing || name != "10.0.0.1:9333" {
		t.Fatalf("clustered leader did not report the missing raft peer: %q %v", name, missing)
	}
	peerRaft := newInmemRaftFollower(t, peer, leaderTransport)

	done := ms.AdmitRaftPeer(peer)
	if done == nil {
		t.Fatal("clustered leader skipped admission of a missing raft peer")
	}
	if err := <-done; err != nil {
		t.Fatalf("raft peer admission failed: %v", err)
	}

	// The joined node's own configuration is the committed view: it only
	// learns the quorum it was admitted into through replication.
	for deadline := time.Now().Add(10 * time.Second); ; {
		if raftHasVoter(peerRaft, string(peer)) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("admitted peer never saw itself in the quorum: %v",
				peerRaft.GetConfiguration().Configuration().Servers)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func raftHasVoter(r *hashicorpRaft.Raft, id string) bool {
	for _, server := range r.GetConfiguration().Configuration().Servers {
		if server.Suffrage == hashicorpRaft.Voter && string(server.ID) == id {
			return true
		}
	}
	return false
}

func newInmemRaftFollower(t *testing.T, self pb.ServerAddress, peerTransport *hashicorpRaft.InmemTransport) *hashicorpRaft.Raft {
	t.Helper()
	conf := hashicorpRaft.DefaultConfig()
	conf.LocalID = hashicorpRaft.ServerID(self)
	addr, transport := hashicorpRaft.NewInmemTransport(hashicorpRaft.ServerAddress(self.ToGrpcAddress()))
	transport.Connect(peerTransport.LocalAddr(), peerTransport)
	peerTransport.Connect(addr, transport)
	r, err := hashicorpRaft.NewRaft(conf,
		&StateMachine{topo: topology.NewTopology("fsm", nil, 128*1024*1024, 0, false)},
		hashicorpRaft.NewInmemStore(), hashicorpRaft.NewInmemStore(),
		hashicorpRaft.NewInmemSnapshotStore(), transport)
	if err != nil {
		t.Fatalf("new raft: %v", err)
	}
	t.Cleanup(func() { r.Shutdown() })
	return r
}

func newInmemRaftLeader(t *testing.T, self string) (*hashicorpRaft.Raft, *hashicorpRaft.InmemTransport) {
	t.Helper()
	conf := hashicorpRaft.DefaultConfig()
	conf.LocalID = hashicorpRaft.ServerID(self)
	conf.HeartbeatTimeout = 50 * time.Millisecond
	conf.ElectionTimeout = 50 * time.Millisecond
	conf.LeaderLeaseTimeout = 25 * time.Millisecond
	conf.CommitTimeout = 5 * time.Millisecond
	addr, transport := hashicorpRaft.NewInmemTransport(hashicorpRaft.ServerAddress(self))
	r, err := hashicorpRaft.NewRaft(conf,
		&StateMachine{topo: topology.NewTopology("fsm", nil, 128*1024*1024, 0, false)},
		hashicorpRaft.NewInmemStore(), hashicorpRaft.NewInmemStore(),
		hashicorpRaft.NewInmemSnapshotStore(), transport)
	if err != nil {
		t.Fatalf("new raft: %v", err)
	}
	t.Cleanup(func() { r.Shutdown() })

	if err := r.BootstrapCluster(hashicorpRaft.Configuration{
		Servers: []hashicorpRaft.Server{{Suffrage: hashicorpRaft.Voter, ID: conf.LocalID, Address: addr}},
	}).Error(); err != nil {
		t.Fatalf("bootstrap cluster: %v", err)
	}
	for deadline := time.Now().Add(10 * time.Second); ; {
		if r.State() == hashicorpRaft.Leader {
			return r, transport
		}
		if time.Now().After(deadline) {
			t.Fatal("in-memory raft did not elect a leader")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
