package ec_balance

import (
	"context"
	"net"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding/ecbalancer"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// The EC balance policy itself is tested in the shared ecbalancer package; these
// tests cover the worker adapter: building the planner topology from the master
// topology (filters, capacity) and the Detection entry point.

func ecTopo(node1Collection string) *master_pb.TopologyInfo {
	node1 := &master_pb.DataNodeInfo{
		Id: "node1",
		DiskInfos: map[string]*master_pb.DiskInfo{
			"": {Type: "", MaxVolumeCount: 100, EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
				{Id: 100, Collection: node1Collection, DiskId: 0, EcIndexBits: 0x3FFF}, // 14 shards
			}},
		},
	}
	node2 := &master_pb.DataNodeInfo{
		Id:        "node2",
		DiskInfos: map[string]*master_pb.DiskInfo{"": {Type: "", MaxVolumeCount: 100}},
	}
	return &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{
				{Id: "rack1", DataNodeInfos: []*master_pb.DataNodeInfo{node1}},
				{Id: "rack2", DataNodeInfos: []*master_pb.DataNodeInfo{node2}},
			},
		}},
	}
}

func TestBuildBalancerTopology(t *testing.T) {
	config := NewDefaultConfig()
	topo, nodeCount, _ := buildBalancerTopology(ecTopo("col1"), config)
	if nodeCount != 2 {
		t.Fatalf("nodeCount = %d, want 2", nodeCount)
	}
	moves := ecbalancer.Plan(topo, ecbalancer.Options{ImbalanceThreshold: 0.01})
	if len(moves) == 0 {
		t.Error("expected cross-rack moves for an all-on-one-rack volume")
	}
}

// TestBuildBalancerTopologyGroupsByHost: two volume servers on host 10.0.0.1
// (different ports) plus three other hosts, a 10+4 volume concentrated on the
// 10.0.0.1 machine. Four machines is enough to spread within parity, so after
// planning the 10.0.0.1 machine must hold <=4 shards of the volume -- which only
// holds if its two ports are grouped into one machine (host wired into the build).
func TestBuildBalancerTopologyGroupsByHost(t *testing.T) {
	mkNode := func(id string, bits uint32) *master_pb.DataNodeInfo {
		di := &master_pb.DiskInfo{Type: "", MaxVolumeCount: 100}
		if bits != 0 {
			di.EcShardInfos = []*master_pb.VolumeEcShardInformationMessage{{Id: 100, Collection: "col1", DiskId: 0, EcIndexBits: bits}}
		}
		return &master_pb.DataNodeInfo{Id: id, DiskInfos: map[string]*master_pb.DiskInfo{"": di}}
	}
	topoInfo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{{Id: "rack1", DataNodeInfos: []*master_pb.DataNodeInfo{
				mkNode("10.0.0.1:8080", 0x007F), // shards 0-6 on host 10.0.0.1
				mkNode("10.0.0.1:8081", 0x3F80), // shards 7-13 on host 10.0.0.1
				mkNode("10.0.0.2:8080", 0),
				mkNode("10.0.0.3:8080", 0),
				mkNode("10.0.0.4:8080", 0),
			}}},
		}},
	}

	topo, _, _ := buildBalancerTopology(topoInfo, NewDefaultConfig())
	moves := ecbalancer.Plan(topo, ecbalancer.Options{ImbalanceThreshold: 0.01})

	host := func(nodeID string) string { h, _, _ := net.SplitHostPort(nodeID); return h }
	count := map[string]int{"10.0.0.1": 14}
	for _, m := range moves {
		if m.VolumeID != 100 {
			continue
		}
		count[host(m.SourceNode)]--
		if m.SourceNode != m.TargetNode { // non-dedup move
			count[host(m.TargetNode)]++
		}
	}
	if count["10.0.0.1"] > 4 {
		t.Errorf("machine 10.0.0.1 holds %d shards of the volume after balancing, want <=4 (host grouping not applied)", count["10.0.0.1"])
	}
}

func TestBuildBalancerTopologyCollectionFilter(t *testing.T) {
	config := NewDefaultConfig()
	config.CollectionFilter = "other" // does not match the volume's collection
	topo, nodeCount, _ := buildBalancerTopology(ecTopo("col1"), config)
	if nodeCount != 2 {
		t.Fatalf("nodeCount = %d, want 2", nodeCount)
	}
	if moves := ecbalancer.Plan(topo, ecbalancer.Options{ImbalanceThreshold: 0.01}); len(moves) != 0 {
		t.Errorf("filtered-out collection should produce no moves, got %d", len(moves))
	}
}

func TestDetectionDisabled(t *testing.T) {
	config := NewDefaultConfig()
	config.Enabled = false

	results, hasMore, err := Detection(context.Background(), nil, nil, config, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hasMore {
		t.Error("expected hasMore=false")
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}

func TestDetectionNilTopology(t *testing.T) {
	config := NewDefaultConfig()
	clusterInfo := &types.ClusterInfo{ActiveTopology: nil}

	if _, _, err := Detection(context.Background(), nil, clusterInfo, config, 0); err == nil {
		t.Fatal("expected error for nil topology")
	}
}

func TestMovePhasePriority(t *testing.T) {
	cases := map[string]types.TaskPriority{
		"dedup":       types.TaskPriorityHigh,
		"cross_rack":  types.TaskPriorityMedium,
		"within_rack": types.TaskPriorityLow,
		"global":      types.TaskPriorityLow,
	}
	for phase, want := range cases {
		if got := movePhasePriority(phase); got != want {
			t.Errorf("movePhasePriority(%q) = %v, want %v", phase, got, want)
		}
	}
}

// keep the erasure_coding import meaningful for future adapter tests
var _ = erasure_coding.DataShardsCount
