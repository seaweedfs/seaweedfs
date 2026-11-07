package ec_balance

import (
	"context"
	"net"
	"slices"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding/ecbalancer"
	"github.com/seaweedfs/seaweedfs/weed/util/wildcard"
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
	topo, nodeCount, _ := buildBalancerTopology(ecTopo("col1"), config, nil)
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

	topo, _, _ := buildBalancerTopology(topoInfo, NewDefaultConfig(), nil)
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
	allowed, err := wildcard.CompileCollectionMatcher(config.CollectionFilter)
	if err != nil {
		t.Fatalf("CompileCollectionMatcher: %v", err)
	}
	topo, nodeCount, _ := buildBalancerTopology(ecTopo("col1"), config, allowed)
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

// TestSortMovesByPhaseKeepsGlobalAheadOfWithinRack: a long within-rack backlog
// must not push the capped global moves past the detection cap.
func TestSortMovesByPhaseKeepsGlobalAheadOfWithinRack(t *testing.T) {
	moves := []ecbalancer.Move{
		{VolumeID: 1, Phase: "within_rack"},
		{VolumeID: 2, Phase: "dedup"},
		{VolumeID: 3, Phase: "within_rack"},
		{VolumeID: 4, Phase: "global"},
		{VolumeID: 5, Phase: "cross_rack"},
		{VolumeID: 6, Phase: "global"},
	}
	sortMovesByPhase(moves)
	var got []uint32
	for _, m := range moves {
		got = append(got, m.VolumeID)
	}
	if want := []uint32{2, 5, 4, 6, 1, 3}; !slices.Equal(got, want) {
		t.Errorf("order = %v, want %v", got, want)
	}
}

// TestSortMovesByPhaseKeepsAVolumesMovesInPlanOrder: a global move of a shard
// that a within-rack move placed must not run ahead of that move.
func TestSortMovesByPhaseKeepsAVolumesMovesInPlanOrder(t *testing.T) {
	moves := []ecbalancer.Move{
		{VolumeID: 1, ShardID: 3, SourceNode: "within 1", Phase: "within_rack"},
		{VolumeID: 2, SourceNode: "within 2", Phase: "within_rack"},
		{VolumeID: 1, ShardID: 3, SourceNode: "global 1", Phase: "global"},
		{VolumeID: 3, SourceNode: "global 3", Phase: "global"},
		{VolumeID: 2, Collection: "other", SourceNode: "global other 2", Phase: "global"},
	}
	sortMovesByPhase(moves)
	var got []string
	for _, m := range moves {
		got = append(got, m.SourceNode)
	}
	if want := []string{"global 3", "global other 2", "within 1", "within 2", "global 1"}; !slices.Equal(got, want) {
		t.Errorf("order = %q, want %q", got, want)
	}
}

// TestDetectionKeepsGlobalMovesUnderTheCap: the within-rack moves that come
// first in plan order must not push every global move past maxResults.
func TestDetectionKeepsGlobalMovesUnderTheCap(t *testing.T) {
	disk := func(shards ...*master_pb.VolumeEcShardInformationMessage) map[string]*master_pb.DiskInfo {
		return map[string]*master_pb.DiskInfo{"": {Type: "", MaxVolumeCount: 100, EcShardInfos: shards}}
	}
	// Volume 100 sits on one machine (within-rack moves); volumes 1-20 load one
	// server (global moves).
	var loaded []*master_pb.VolumeEcShardInformationMessage
	for vid := uint32(1); vid <= 20; vid++ {
		loaded = append(loaded, &master_pb.VolumeEcShardInformationMessage{Id: vid, Collection: "col1", EcIndexBits: 1})
	}
	topoInfo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{{Id: "rack1", DataNodeInfos: []*master_pb.DataNodeInfo{
				{Id: "10.0.0.1:8080", DiskInfos: disk(&master_pb.VolumeEcShardInformationMessage{Id: 100, Collection: "col1", EcIndexBits: 0x3FFF})},
				{Id: "10.0.0.2:8080", DiskInfos: disk(loaded...)},
				{Id: "10.0.0.3:8080", DiskInfos: disk()},
				{Id: "10.0.0.4:8080", DiskInfos: disk()},
			}}},
		}},
	}
	at := topology.NewActiveTopology(0)
	if err := at.UpdateTopology(topoInfo); err != nil {
		t.Fatalf("UpdateTopology: %v", err)
	}
	clusterInfo := &types.ClusterInfo{ActiveTopology: at}

	all, _, err := Detection(context.Background(), nil, clusterInfo, NewDefaultConfig(), 0)
	if err != nil {
		t.Fatalf("Detection: %v", err)
	}
	withinRack := 0
	for _, r := range all {
		if strings.Contains(r.Reason, " within_rack:") {
			withinRack++
		}
	}
	if withinRack < 2 {
		t.Fatalf("fixture plans %d within-rack moves, want at least 2", withinRack)
	}

	results, hasMore, err := Detection(context.Background(), nil, clusterInfo, NewDefaultConfig(), 1)
	if err != nil {
		t.Fatalf("Detection: %v", err)
	}
	if len(results) != 1 || !hasMore {
		t.Fatalf("got %d results (hasMore %v), want 1 and more", len(results), hasMore)
	}
	if !strings.Contains(results[0].Reason, " global:") {
		t.Errorf("kept %q, want a global move", results[0].Reason)
	}
}

// TestDetectionUsesConfiguredGlobalCap: the configured per-rack cap reaches the planner, and
// an unset one plans the default rather than the planner's unlimited 0.
func TestDetectionUsesConfiguredGlobalCap(t *testing.T) {
	disk := func(shards ...*master_pb.VolumeEcShardInformationMessage) map[string]*master_pb.DiskInfo {
		return map[string]*master_pb.DiskInfo{"": {Type: "", MaxVolumeCount: 100, EcShardInfos: shards}}
	}
	var loaded []*master_pb.VolumeEcShardInformationMessage
	for vid := uint32(1); vid <= 40; vid++ {
		loaded = append(loaded, &master_pb.VolumeEcShardInformationMessage{Id: vid, Collection: "col1", EcIndexBits: 1})
	}
	topoInfo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{{Id: "rack1", DataNodeInfos: []*master_pb.DataNodeInfo{
				{Id: "10.0.0.1:8080", DiskInfos: disk(loaded...)},
				{Id: "10.0.0.2:8080", DiskInfos: disk()},
				{Id: "10.0.0.3:8080", DiskInfos: disk()},
				{Id: "10.0.0.4:8080", DiskInfos: disk()},
			}}},
		}},
	}
	at := topology.NewActiveTopology(0)
	if err := at.UpdateTopology(topoInfo); err != nil {
		t.Fatalf("UpdateTopology: %v", err)
	}
	clusterInfo := &types.ClusterInfo{ActiveTopology: at}

	for _, tc := range []struct{ configured, maxResults, want int }{
		{0, 0, DefaultGlobalMaxMovesPerRack},
		{25, 0, 25},
		{25, 20, 20},
	} {
		config := NewDefaultConfig()
		config.GlobalMaxMovesPerRack = tc.configured
		results, _, err := Detection(context.Background(), nil, clusterInfo, config, tc.maxResults)
		if err != nil {
			t.Fatalf("Detection: %v", err)
		}
		global := 0
		for _, r := range results {
			if strings.Contains(r.Reason, " global:") {
				global++
			}
		}
		if global != tc.want {
			t.Errorf("cap %d with %d results planned %d global moves, want %d", tc.configured, tc.maxResults, global, tc.want)
		}
	}
}

// TestGlobalMaxMovesPerRackBoundedByMaxResults: a rack plans no more global moves than the
// detection can keep.
func TestGlobalMaxMovesPerRackBoundedByMaxResults(t *testing.T) {
	config := &Config{GlobalMaxMovesPerRack: 5000}
	if got := globalMaxMovesPerRack(config, 500); got != 500 {
		t.Errorf("cap with 500 results = %d, want 500", got)
	}
	if got := globalMaxMovesPerRack(config, 0); got != 5000 {
		t.Errorf("cap with unlimited results = %d, want 5000", got)
	}
}
