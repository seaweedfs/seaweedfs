package weed_server

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/topology"
)

func TestSingleMasterNeverAdmitsRaftPeers(t *testing.T) {
	ms := &MasterServer{option: &MasterOption{SingleMaster: true, Master: "localhost:9333"}}

	// A foreign master announcing itself must not touch the admission path:
	// ms.Topo is deliberately nil so any admission attempt panics the test.
	ms.AdmitRaftPeer("10.0.0.1:9333")
	if _, admitted := ms.raftPeerAdmissions.Load("10.0.0.1:9333"); admitted {
		t.Fatal("single-master mode admitted a foreign raft peer")
	}
}

func TestClusteredMasterStillConsultsRaftForAdmission(t *testing.T) {
	ms := &MasterServer{
		option: &MasterOption{Master: "localhost:9333"},
		Topo:   topology.NewTopology("topo", nil, 128*1024*1024, 0, false),
	}

	// No raft handle means not a leader: admission must exit cleanly, not skip
	// the topology check the way the single-master guard does.
	ms.AdmitRaftPeer("10.0.0.1:9333")
}
