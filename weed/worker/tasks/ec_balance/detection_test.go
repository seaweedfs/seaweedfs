package ec_balance

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

func TestShardBitCount(t *testing.T) {
	tests := []struct {
		bits     uint32
		expected int
	}{
		{0, 0},
		{1, 1},
		{0b111, 3},
		{0x3FFF, 14}, // all 14 shards
		{0b10101010, 4},
	}
	for _, tt := range tests {
		got := shardBitCount(tt.bits)
		if got != tt.expected {
			t.Errorf("shardBitCount(%b) = %d, want %d", tt.bits, got, tt.expected)
		}
	}
}

func TestCeilDivide(t *testing.T) {
	tests := []struct {
		a, b     int
		expected int
	}{
		{14, 3, 5},
		{14, 7, 2},
		{10, 3, 4},
		{0, 5, 0},
		{5, 0, 0},
	}
	for _, tt := range tests {
		got := ceilDivide(tt.a, tt.b)
		if got != tt.expected {
			t.Errorf("ceilDivide(%d, %d) = %d, want %d", tt.a, tt.b, got, tt.expected)
		}
	}
}

func TestDetectDuplicateShards(t *testing.T) {
	nodes := map[string]*ecNodeInfo{
		"node1": {
			nodeID: "node1", address: "node1:8080", rack: "dc1:rack1", freeSlots: 5,
			ecShards: map[uint32]*ecVolumeInfo{
				100: {collection: "col1", shardBits: 0b11}, // shard 0, 1
			},
		},
		"node2": {
			nodeID: "node2", address: "node2:8080", rack: "dc1:rack2", freeSlots: 10,
			ecShards: map[uint32]*ecVolumeInfo{
				100: {collection: "col1", shardBits: 0b01}, // shard 0 (duplicate)
			},
		},
	}

	moves := detectDuplicateShards(100, "col1", nodes, "")

	if len(moves) != 1 {
		t.Fatalf("expected 1 dedup move, got %d", len(moves))
	}

	move := moves[0]
	if move.phase != "dedup" {
		t.Errorf("expected phase 'dedup', got %q", move.phase)
	}
	if move.shardID != 0 {
		t.Errorf("expected shard 0 to be deduplicated, got %d", move.shardID)
	}
	// node1 has fewer free slots, so the duplicate on node1 should be removed (keeper is node2)
	if move.source.nodeID != "node1" {
		t.Errorf("expected source node1 (fewer free slots), got %s", move.source.nodeID)
	}
	// Dedup moves set target=source so isDedupPhase recognizes unmount+delete only
	if move.target.nodeID != "node1" {
		t.Errorf("expected target node1 (same as source for dedup), got %s", move.target.nodeID)
	}
}

func TestDetectCrossRackImbalance(t *testing.T) {
	// 14 shards all on rack1, 2 racks available — large imbalance
	nodes := map[string]*ecNodeInfo{
		"node1": {
			nodeID: "node1", address: "node1:8080", rack: "dc1:rack1", freeSlots: 0,
			ecShards: map[uint32]*ecVolumeInfo{
				100: {collection: "col1", shardBits: 0x3FFF}, // all 14 shards
			},
		},
		"node2": {
			nodeID: "node2", address: "node2:8080", rack: "dc1:rack2", freeSlots: 20,
			ecShards: map[uint32]*ecVolumeInfo{},
		},
	}
	racks := map[string]*ecRackInfo{
		"dc1:rack1": {
			nodes:     map[string]*ecNodeInfo{"node1": nodes["node1"]},
			freeSlots: 0,
		},
		"dc1:rack2": {
			nodes:     map[string]*ecNodeInfo{"node2": nodes["node2"]},
			freeSlots: 20,
		},
	}

	// Use very low threshold so this triggers
	moves := detectCrossRackImbalance(100, "col1", nodes, racks, "", 0.01)

	// With 14 shards across 2 racks, max per rack = 7
	// rack1 has 14 -> excess = 7, should move 7 to rack2
	if len(moves) != 7 {
		t.Fatalf("expected 7 cross-rack moves, got %d", len(moves))
	}
	for _, move := range moves {
		if move.phase != "cross_rack" {
			t.Errorf("expected phase 'cross_rack', got %q", move.phase)
		}
		if move.source.rack != "dc1:rack1" {
			t.Errorf("expected source dc1:rack1, got %s", move.source.rack)
		}
		if move.target.rack != "dc1:rack2" {
			t.Errorf("expected target dc1:rack2, got %s", move.target.rack)
		}
	}
}

func TestDetectCrossRackImbalanceBelowThreshold(t *testing.T) {
	// Slight imbalance: rack1 has 8, rack2 has 6 — imbalance = 2/7 ≈ 0.29
	nodes := map[string]*ecNodeInfo{
		"node1": {
			nodeID: "node1", address: "node1:8080", rack: "dc1:rack1", freeSlots: 10,
			ecShards: map[uint32]*ecVolumeInfo{
				100: {collection: "col1", shardBits: 0xFF}, // 8 shards
			},
		},
		"node2": {
			nodeID: "node2", address: "node2:8080", rack: "dc1:rack2", freeSlots: 10,
			ecShards: map[uint32]*ecVolumeInfo{
				100: {collection: "col1", shardBits: 0x3F00}, // 6 shards
			},
		},
	}
	racks := map[string]*ecRackInfo{
		"dc1:rack1": {
			nodes:     map[string]*ecNodeInfo{"node1": nodes["node1"]},
			freeSlots: 10,
		},
		"dc1:rack2": {
			nodes:     map[string]*ecNodeInfo{"node2": nodes["node2"]},
			freeSlots: 10,
		},
	}

	// High threshold should skip this
	moves := detectCrossRackImbalance(100, "col1", nodes, racks, "", 0.5)
	if len(moves) != 0 {
		t.Fatalf("expected 0 moves below threshold, got %d", len(moves))
	}
}

func TestDetectWithinRackImbalance(t *testing.T) {
	// rack1 has 2 nodes: node1 has 10 shards, node2 has 0 shards
	nodes := map[string]*ecNodeInfo{
		"node1": {
			nodeID: "node1", address: "node1:8080", rack: "dc1:rack1", freeSlots: 5,
			ecShards: map[uint32]*ecVolumeInfo{
				100: {collection: "col1", shardBits: 0b1111111111}, // shards 0-9
			},
		},
		"node2": {
			nodeID: "node2", address: "node2:8080", rack: "dc1:rack1", freeSlots: 20,
			ecShards: map[uint32]*ecVolumeInfo{},
		},
	}
	racks := map[string]*ecRackInfo{
		"dc1:rack1": {
			nodes:     map[string]*ecNodeInfo{"node1": nodes["node1"], "node2": nodes["node2"]},
			freeSlots: 25,
		},
	}

	moves := detectWithinRackImbalance(100, "col1", nodes, racks, "", 0.01)

	// 10 shards on 2 nodes, max per node = 5
	// node1 has 10 -> excess = 5, should move 5 to node2
	if len(moves) != 5 {
		t.Fatalf("expected 5 within-rack moves, got %d", len(moves))
	}
	for _, move := range moves {
		if move.phase != "within_rack" {
			t.Errorf("expected phase 'within_rack', got %q", move.phase)
		}
		if move.source.nodeID != "node1" {
			t.Errorf("expected source node1, got %s", move.source.nodeID)
		}
		if move.target.nodeID != "node2" {
			t.Errorf("expected target node2, got %s", move.target.nodeID)
		}
	}
}

func TestDetectGlobalImbalance(t *testing.T) {
	// node1 has 20 total shards, node2 has 2 total shards (same rack)
	nodes := map[string]*ecNodeInfo{
		"node1": {
			nodeID: "node1", address: "node1:8080", rack: "dc1:rack1", freeSlots: 5,
			ecShards: map[uint32]*ecVolumeInfo{
				100: {collection: "col1", shardBits: 0x3FFF},   // 14 shards
				200: {collection: "col1", shardBits: 0b111111}, // 6 shards
			},
		},
		"node2": {
			nodeID: "node2", address: "node2:8080", rack: "dc1:rack1", freeSlots: 30,
			ecShards: map[uint32]*ecVolumeInfo{
				300: {collection: "col1", shardBits: 0b11}, // 2 shards
			},
		},
	}
	racks := map[string]*ecRackInfo{
		"dc1:rack1": {
			nodes:     map[string]*ecNodeInfo{"node1": nodes["node1"], "node2": nodes["node2"]},
			freeSlots: 35,
		},
	}

	config := NewDefaultConfig()
	config.ImbalanceThreshold = 0.01 // low threshold to ensure moves happen
	moves := detectGlobalImbalance(nodes, racks, config, nil)

	// Total = 22 shards, avg = 11. node1 has 20, node2 has 2.
	// Should move shards until balanced (max 10 iterations)
	if len(moves) == 0 {
		t.Fatal("expected global balance moves, got 0")
	}
	for _, move := range moves {
		if move.phase != "global" {
			t.Errorf("expected phase 'global', got %q", move.phase)
		}
		if move.source.nodeID != "node1" {
			t.Errorf("expected moves from node1, got %s", move.source.nodeID)
		}
		if move.target.nodeID != "node2" {
			t.Errorf("expected moves to node2, got %s", move.target.nodeID)
		}
	}
}

// TestDetectGlobalImbalance_HeterogeneousCapacity is a regression test for
// the Phase 4 rebalancer on heterogeneous racks. node1 holds more shards in
// absolute terms but has much higher capacity, so it is actually the LESS
// utilized node; node2 holds fewer shards but is nearly full. The greedy
// algorithm must pick the most-utilized node as the source and move shards
// in the direction that reduces fractional fullness, NOT in the direction
// that would equalize raw counts (which here would overfill node2).
//
// Scenario:
//
//	node1: 10 shards, freeSlots=90   → capacity 100, util 10%
//	node2:  3 shards, freeSlots=2    → capacity  5, util 60%
//
// Correct behavior: move shards FROM node2 TO node1 (draining the
// most-utilized node), until no further improvement is possible. Also
// verifies that moves are de-duplicated — the inner loop must update
// shardBits between iterations so each proposed move refers to a distinct
// physical shard.
func TestDetectGlobalImbalance_HeterogeneousCapacity(t *testing.T) {
	nodes := map[string]*ecNodeInfo{
		"node1": {
			nodeID: "node1", address: "node1:8080", rack: "dc1:rack1", freeSlots: 90,
			ecShards: map[uint32]*ecVolumeInfo{
				100: {collection: "col1", shardBits: 0x3FF}, // 10 shards
			},
		},
		"node2": {
			nodeID: "node2", address: "node2:8080", rack: "dc1:rack1", freeSlots: 2,
			ecShards: map[uint32]*ecVolumeInfo{
				200: {collection: "col1", shardBits: 0b111}, // 3 shards
			},
		},
	}
	racks := map[string]*ecRackInfo{
		"dc1:rack1": {
			nodes:     map[string]*ecNodeInfo{"node1": nodes["node1"], "node2": nodes["node2"]},
			freeSlots: 92,
		},
	}

	config := NewDefaultConfig()
	config.ImbalanceThreshold = 0.01
	moves := detectGlobalImbalance(nodes, racks, config, nil)

	if len(moves) == 0 {
		t.Fatal("expected moves from high-util node2 to low-util node1, got 0")
	}

	// Every move must drain the higher-util node (node2) and target the
	// lower-util node (node1). A raw-count-based greedy algorithm would
	// pick the opposite direction — that is the bug this test guards.
	for _, move := range moves {
		if move.source.nodeID != "node2" {
			t.Errorf("expected source node2 (util 0.60), got %s", move.source.nodeID)
		}
		if move.target.nodeID != "node1" {
			t.Errorf("expected target node1 (util 0.10), got %s", move.target.nodeID)
		}
	}

	// Verify no duplicate (volumeID, shardID) pairs — the inner loop must
	// update shardBits between iterations so each move refers to a distinct
	// physical shard.
	seen := make(map[[2]int]bool, len(moves))
	for _, move := range moves {
		key := [2]int{int(move.volumeID), move.shardID}
		if seen[key] {
			t.Errorf("duplicate move for volume %d shard %d", move.volumeID, move.shardID)
		}
		seen[key] = true
	}
}

func TestDetectGlobalImbalanceSkipsFullNodes(t *testing.T) {
	// node2 has 0 free slots — should not be chosen as destination
	nodes := map[string]*ecNodeInfo{
		"node1": {
			nodeID: "node1", address: "node1:8080", rack: "dc1:rack1", freeSlots: 10,
			ecShards: map[uint32]*ecVolumeInfo{
				100: {collection: "col1", shardBits: 0x3FFF}, // 14 shards
			},
		},
		"node2": {
			nodeID: "node2", address: "node2:8080", rack: "dc1:rack1", freeSlots: 0,
			ecShards: map[uint32]*ecVolumeInfo{
				200: {collection: "col1", shardBits: 0b11}, // 2 shards
			},
		},
	}
	racks := map[string]*ecRackInfo{
		"dc1:rack1": {
			nodes:     map[string]*ecNodeInfo{"node1": nodes["node1"], "node2": nodes["node2"]},
			freeSlots: 10,
		},
	}

	config := NewDefaultConfig()
	config.ImbalanceThreshold = 0.01
	moves := detectGlobalImbalance(nodes, racks, config, nil)

	// node2 has no free slots so no moves should be proposed
	if len(moves) != 0 {
		t.Fatalf("expected 0 moves (node2 full), got %d", len(moves))
	}
}

func TestBuildECTopology(t *testing.T) {
	topoInfo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				Id: "dc1",
				RackInfos: []*master_pb.RackInfo{
					{
						Id: "rack1",
						DataNodeInfos: []*master_pb.DataNodeInfo{
							{
								Id: "server1:8080",
								DiskInfos: map[string]*master_pb.DiskInfo{
									"": {
										MaxVolumeCount: 100,
										VolumeCount:    50,
										EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
											{
												Id:          1,
												Collection:  "test",
												EcIndexBits: 0x3FFF, // all 14 shards
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	config := NewDefaultConfig()
	nodes, racks := buildECTopology(topoInfo, config)

	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}
	if len(racks) != 1 {
		t.Fatalf("expected 1 rack, got %d", len(racks))
	}

	node := nodes["server1:8080"]
	if node == nil {
		t.Fatal("expected node server1:8080")
	}
	if node.dc != "dc1" {
		t.Errorf("expected dc=dc1, got %s", node.dc)
	}
	// Rack key should be dc:rack composite
	if node.rack != "dc1:rack1" {
		t.Errorf("expected rack=dc1:rack1, got %s", node.rack)
	}

	ecInfo, ok := node.ecShards[1]
	if !ok {
		t.Fatal("expected EC shard info for volume 1")
	}
	if ecInfo.collection != "test" {
		t.Errorf("expected collection=test, got %s", ecInfo.collection)
	}
	if shardBitCount(ecInfo.shardBits) != 14 {
		t.Errorf("expected 14 shards, got %d", shardBitCount(ecInfo.shardBits))
	}
}

func TestBuildECTopologyCrossDCRackNames(t *testing.T) {
	// Two DCs with identically-named racks should produce distinct rack keys
	topoInfo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				Id: "dc1",
				RackInfos: []*master_pb.RackInfo{{
					Id: "rack1",
					DataNodeInfos: []*master_pb.DataNodeInfo{{
						Id: "node-dc1:8080",
						DiskInfos: map[string]*master_pb.DiskInfo{
							"": {MaxVolumeCount: 10, VolumeCount: 0},
						},
					}},
				}},
			},
			{
				Id: "dc2",
				RackInfos: []*master_pb.RackInfo{{
					Id: "rack1",
					DataNodeInfos: []*master_pb.DataNodeInfo{{
						Id: "node-dc2:8080",
						DiskInfos: map[string]*master_pb.DiskInfo{
							"": {MaxVolumeCount: 10, VolumeCount: 0},
						},
					}},
				}},
			},
		},
	}

	config := NewDefaultConfig()
	_, racks := buildECTopology(topoInfo, config)

	if len(racks) != 2 {
		t.Fatalf("expected 2 distinct racks, got %d", len(racks))
	}
	if _, ok := racks["dc1:rack1"]; !ok {
		t.Error("expected dc1:rack1 rack key")
	}
	if _, ok := racks["dc2:rack1"]; !ok {
		t.Error("expected dc2:rack1 rack key")
	}
}

func TestCollectECCollections(t *testing.T) {
	nodes := map[string]*ecNodeInfo{
		"node1": {
			ecShards: map[uint32]*ecVolumeInfo{
				100: {collection: "col1"},
				200: {collection: "col2"},
			},
		},
		"node2": {
			ecShards: map[uint32]*ecVolumeInfo{
				100: {collection: "col1"},
				300: {collection: "col2"},
			},
		},
	}

	config := NewDefaultConfig()
	collections := collectECCollections(nodes, config)

	if len(collections) != 2 {
		t.Fatalf("expected 2 collections, got %d", len(collections))
	}
	if len(collections["col1"]) != 1 {
		t.Errorf("expected 1 volume in col1, got %d", len(collections["col1"]))
	}
	if len(collections["col2"]) != 2 {
		t.Errorf("expected 2 volumes in col2, got %d", len(collections["col2"]))
	}
}

func TestCollectECCollectionsWithFilter(t *testing.T) {
	nodes := map[string]*ecNodeInfo{
		"node1": {
			ecShards: map[uint32]*ecVolumeInfo{
				100: {collection: "col1"},
				200: {collection: "col2"},
			},
		},
	}

	config := NewDefaultConfig()
	config.CollectionFilter = "col1"
	collections := collectECCollections(nodes, config)

	if len(collections) != 1 {
		t.Fatalf("expected 1 collection, got %d", len(collections))
	}
	if _, ok := collections["col1"]; !ok {
		t.Error("expected col1 to be present")
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

	_, _, err := Detection(context.Background(), nil, clusterInfo, config, 0)
	if err == nil {
		t.Fatal("expected error for nil topology")
	}
}

func TestMovePhasePriority(t *testing.T) {
	if movePhasePriority("dedup") != types.TaskPriorityHigh {
		t.Error("dedup should be high priority")
	}
	if movePhasePriority("cross_rack") != types.TaskPriorityMedium {
		t.Error("cross_rack should be medium priority")
	}
	if movePhasePriority("within_rack") != types.TaskPriorityLow {
		t.Error("within_rack should be low priority")
	}
	if movePhasePriority("global") != types.TaskPriorityLow {
		t.Error("global should be low priority")
	}
}

func TestExceedsImbalanceThreshold(t *testing.T) {
	// 14 vs 0 across 2 groups: imbalance = 14/7 = 2.0 > any reasonable threshold
	counts := map[string]int{"a": 14, "b": 0}
	if !exceedsImbalanceThreshold(counts, 14, 2, 0.2) {
		t.Error("expected imbalance to exceed 0.2 threshold")
	}

	// Only one group has shards but numGroups=2: min is 0 from absent group
	counts2 := map[string]int{"a": 14}
	if !exceedsImbalanceThreshold(counts2, 14, 2, 0.2) {
		t.Error("expected imbalance with absent group to exceed 0.2 threshold")
	}

	// 7 vs 7: perfectly balanced
	counts3 := map[string]int{"a": 7, "b": 7}
	if exceedsImbalanceThreshold(counts3, 14, 2, 0.01) {
		t.Error("expected balanced distribution to not exceed threshold")
	}
}

// helper to avoid unused import
var _ = erasure_coding.DataShardsCount
