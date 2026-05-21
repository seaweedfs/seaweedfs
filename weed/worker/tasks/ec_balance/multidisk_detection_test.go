package ec_balance

import (
	"context"
	"sort"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// In-process (no real cluster) tests of the detection/planning path against a
// topology shaped the way the master reports a multi-disk cluster: several
// same-type physical disks on a node collapse into a single DiskInfo, with each
// shard's real DiskId surviving only in the per-shard records (issue 9593). They
// run the master-wire-format -> ActiveTopology -> Detection path, then simulate
// executing the planned moves with the volume server's actual semantics
// (VolumeEcShardsDelete is node-wide) to assert no EC shard is ever lost. The
// real-cluster end-to-end equivalent lives in test/erasure_coding.

// nodeSpec describes one volume server: its rack and the volume's shards per
// physical disk (diskID -> shard ids).
type nodeSpec struct {
	id    string
	rack  string
	disks map[uint32][]int
}

const integDisksPerNode = 6

// buildMasterTopology renders nodeSpecs into a *master_pb.TopologyInfo exactly as
// the master would: one DiskInfo per node keyed by disk type (""), carrying one
// EcShardInfo per (volume, physical disk) plus regular VolumeInfos so every
// physical disk is discoverable even when it holds no EC shards.
func buildMasterTopology(collection string, vid uint32, maxVolPerDisk int, specs []nodeSpec) *master_pb.TopologyInfo {
	rackByID := map[string]*master_pb.RackInfo{}
	var rackOrder []string

	for _, spec := range specs {
		var ecShards []*master_pb.VolumeEcShardInformationMessage
		for diskID, shards := range spec.disks {
			var bits erasure_coding.ShardBits
			for _, s := range shards {
				bits = bits.Set(erasure_coding.ShardId(s))
			}
			ecShards = append(ecShards, &master_pb.VolumeEcShardInformationMessage{
				Id:          vid,
				Collection:  collection,
				DiskId:      diskID,
				EcIndexBits: uint32(bits),
			})
		}
		// Expose all physical disks (incl. EC-empty ones) via regular volumes.
		var volInfos []*master_pb.VolumeInformationMessage
		for d := uint32(0); d < integDisksPerNode; d++ {
			volInfos = append(volInfos, &master_pb.VolumeInformationMessage{Id: 90000 + d, DiskId: d})
		}
		dn := &master_pb.DataNodeInfo{
			Id: spec.id,
			DiskInfos: map[string]*master_pb.DiskInfo{
				"": {
					Type:           "",
					MaxVolumeCount: int64(maxVolPerDisk * integDisksPerNode),
					VolumeCount:    int64(integDisksPerNode),
					EcShardInfos:   ecShards,
					VolumeInfos:    volInfos,
				},
			},
		}
		r, ok := rackByID[spec.rack]
		if !ok {
			r = &master_pb.RackInfo{Id: spec.rack}
			rackByID[spec.rack] = r
			rackOrder = append(rackOrder, spec.rack)
		}
		r.DataNodeInfos = append(r.DataNodeInfos, dn)
	}

	dc := &master_pb.DataCenterInfo{Id: "dc1"}
	for _, rk := range rackOrder {
		dc.RackInfos = append(dc.RackInfos, rackByID[rk])
	}
	return &master_pb.TopologyInfo{Id: "integ", DataCenterInfos: []*master_pb.DataCenterInfo{dc}}
}

// runDetection wires the topology through a real ActiveTopology and runs Detection.
func runDetection(t *testing.T, topoInfo *master_pb.TopologyInfo, cfg *Config) []*types.TaskDetectionResult {
	t.Helper()
	at := topology.NewActiveTopology(0)
	if err := at.UpdateTopology(topoInfo); err != nil {
		t.Fatalf("UpdateTopology: %v", err)
	}
	results, _, err := Detection(context.Background(), nil, &types.ClusterInfo{ActiveTopology: at}, cfg, 0)
	if err != nil {
		t.Fatalf("Detection: %v", err)
	}
	return results
}

// shardModel is nodeID -> diskID -> set of shard ids for one volume.
type shardModel map[string]map[uint32]map[int]bool

func modelFromSpecs(specs []nodeSpec) shardModel {
	m := make(shardModel)
	for _, spec := range specs {
		m[spec.id] = make(map[uint32]map[int]bool)
		for diskID, shards := range spec.disks {
			set := make(map[int]bool)
			for _, s := range shards {
				set[s] = true
			}
			m[spec.id][diskID] = set
		}
	}
	return m
}

func (m shardModel) distinctShards() []int {
	seen := map[int]bool{}
	for _, disks := range m {
		for _, set := range disks {
			for s := range set {
				seen[s] = true
			}
		}
	}
	out := make([]int, 0, len(seen))
	for s := range seen {
		out = append(out, s)
	}
	sort.Ints(out)
	return out
}

// applyMovesRealistically replays planned moves with the volume server's actual
// behavior: a move copies the shard to the destination disk, then deletes it on
// the source node — and EC shard delete is node-wide (removes the shard from
// every disk on the source node). A dedup move (same node+disk) is delete-only.
func (m shardModel) apply(results []*types.TaskDetectionResult) {
	nodeWideDelete := func(node string, shard int) {
		for diskID := range m[node] {
			delete(m[node][diskID], shard)
		}
	}
	for _, r := range results {
		p := r.TypedParams
		if p == nil || len(p.Sources) == 0 || len(p.Targets) == 0 {
			continue
		}
		src, dst := p.Sources[0], p.Targets[0]
		if len(src.ShardIds) == 0 {
			continue
		}
		shard := int(src.ShardIds[0])
		dedup := src.Node == dst.Node && src.DiskId == dst.DiskId
		if !dedup {
			if m[dst.Node] == nil {
				m[dst.Node] = make(map[uint32]map[int]bool)
			}
			if m[dst.Node][dst.DiskId] == nil {
				m[dst.Node][dst.DiskId] = make(map[int]bool)
			}
			m[dst.Node][dst.DiskId][shard] = true
		}
		nodeWideDelete(src.Node, shard)
	}
}

// TestMultiDiskBalanceNeverLosesShards is the core regression for
// issue 9593: a freshly-encoded volume on a 3-node, 6-disk-per-node cluster,
// balanced and then concentrated, must never lose a shard when the planned moves
// are executed with real node-wide-delete semantics.
func TestMultiDiskBalanceNeverLosesShards(t *testing.T) {
	cfg := NewDefaultConfig()
	cfg.ImbalanceThreshold = 0.0 // balance to even; surface any move the planner makes

	cases := []struct {
		name  string
		specs []nodeSpec
	}{
		{
			// Healthy post-encode spread: 14 shards across 3 nodes (5/5/4), each
			// node spreading its shards over distinct physical disks.
			name: "balanced across disks",
			specs: []nodeSpec{
				{id: "n1", rack: "rack1", disks: map[uint32][]int{0: {0}, 1: {1}, 2: {2}, 3: {3}, 4: {4}}},
				{id: "n2", rack: "rack1", disks: map[uint32][]int{0: {5}, 1: {6}, 2: {7}, 3: {8}, 4: {9}}},
				{id: "n3", rack: "rack1", disks: map[uint32][]int{0: {10}, 1: {11}, 2: {12}, 3: {13}}},
			},
		},
		{
			// Worst case from the report: all 14 shards landed on one node's disks;
			// the balancer must redistribute without losing any.
			name: "concentrated on one node",
			specs: []nodeSpec{
				{id: "n1", rack: "rack1", disks: map[uint32][]int{0: {0, 1, 2}, 1: {3, 4, 5}, 2: {6, 7}, 3: {8, 9}, 4: {10, 11}, 5: {12, 13}}},
				{id: "n2", rack: "rack2", disks: map[uint32][]int{}},
				{id: "n3", rack: "rack3", disks: map[uint32][]int{}},
			},
		},
		{
			// Multi-rack healthy spread.
			name: "spread across racks",
			specs: []nodeSpec{
				{id: "n1", rack: "rack1", disks: map[uint32][]int{0: {0, 1}, 1: {2, 3}, 2: {4}}},
				{id: "n2", rack: "rack2", disks: map[uint32][]int{0: {5, 6}, 1: {7, 8}, 2: {9}}},
				{id: "n3", rack: "rack3", disks: map[uint32][]int{0: {10, 11}, 1: {12, 13}}},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			topoInfo := buildMasterTopology("col1", 28, 50, tc.specs)
			results := runDetection(t, topoInfo, cfg)

			model := modelFromSpecs(tc.specs)
			before := model.distinctShards()

			for _, r := range results {
				// No move may be a same-node cross-disk move (node-wide delete
				// would erase the shard after a skipped copy).
				p := r.TypedParams
				s, d := p.Sources[0], p.Targets[0]
				if s.Node == d.Node && s.DiskId != d.DiskId {
					t.Errorf("unsafe same-node cross-disk move: vol %d shard %v on %s disk %d->%d", p.VolumeId, s.ShardIds, s.Node, s.DiskId, d.DiskId)
				}
				// Source disk must actually hold the shard being moved.
				shard := int(s.ShardIds[0])
				if !model[s.Node][s.DiskId][shard] && s.Node != d.Node {
					t.Errorf("move sources vol %d shard %d from %s disk %d, which does not hold it", p.VolumeId, shard, s.Node, s.DiskId)
				}
			}

			model.apply(results)
			after := model.distinctShards()

			if len(after) != len(before) {
				t.Errorf("[%s] shard loss: had %v (%d), now %v (%d) after %d moves",
					tc.name, before, len(before), after, len(after), len(results))
			}
		})
	}
}

// TestConcentratedVolumeSpreadsAcrossNodesAndDisks asserts the
// remediation actually happens: a one-node-concentrated volume is redistributed
// to the other racks, landing on multiple distinct destination disks.
func TestConcentratedVolumeSpreadsAcrossNodesAndDisks(t *testing.T) {
	cfg := NewDefaultConfig()
	cfg.ImbalanceThreshold = 0.0

	specs := []nodeSpec{
		{id: "n1", rack: "rack1", disks: map[uint32][]int{0: {0, 1, 2}, 1: {3, 4, 5}, 2: {6, 7}, 3: {8, 9}, 4: {10, 11}, 5: {12, 13}}},
		{id: "n2", rack: "rack2", disks: map[uint32][]int{}},
		{id: "n3", rack: "rack3", disks: map[uint32][]int{}},
	}
	results := runDetection(t, buildMasterTopology("col1", 28, 50, specs), cfg)
	if len(results) == 0 {
		t.Fatal("expected redistribution moves for a one-node-concentrated volume")
	}

	destNodes := map[string]bool{}
	destDisksByNode := map[string]map[uint32]bool{}
	for _, r := range results {
		d := r.TypedParams.Targets[0]
		if d.Node == "n1" {
			continue
		}
		destNodes[d.Node] = true
		if destDisksByNode[d.Node] == nil {
			destDisksByNode[d.Node] = map[uint32]bool{}
		}
		destDisksByNode[d.Node][d.DiskId] = true
	}
	if len(destNodes) < 2 {
		t.Errorf("shards spread to only %d destination nodes, want both other racks", len(destNodes))
	}
	for node, disks := range destDisksByNode {
		if len(disks) < 2 {
			t.Errorf("destination %s received shards on only %d disk(s); expected spread across disks: %v", node, len(disks), disks)
		}
	}
}

// TestBuildBalancerTopologyNormalizesHddDiskType guards the disk-type filter:
// the master reports default-HDD disks under the empty-string key, so a config of
// "hdd" must match them (not filter everything out), while "ssd" must exclude them.
func TestBuildBalancerTopologyNormalizesHddDiskType(t *testing.T) {
	specs := []nodeSpec{
		{id: "n1", rack: "r1", disks: map[uint32][]int{0: {0, 1}}},
		{id: "n2", rack: "r1", disks: map[uint32][]int{0: {2}}},
	}
	topoInfo := buildMasterTopology("c", 100, 50, specs)

	if _, n := buildBalancerTopology(topoInfo, &Config{DiskType: "hdd"}); n != 2 {
		t.Errorf("disk_type=hdd matched %d nodes on an all-HDD cluster, want 2 (hdd must map to the empty HDD key)", n)
	}
	if _, n := buildBalancerTopology(topoInfo, &Config{DiskType: ""}); n != 2 {
		t.Errorf("disk_type=empty matched %d nodes, want 2 (all)", n)
	}
	if _, n := buildBalancerTopology(topoInfo, &Config{DiskType: "ssd"}); n != 0 {
		t.Errorf("disk_type=ssd matched %d nodes on an all-HDD cluster, want 0", n)
	}
}

// TestResolveReplicaPlacementFallsBackToMasterDefault verifies the worker mirrors
// the shell: explicit config wins, otherwise the master's default replication is
// the fallback, and an empty or zero-replication value means no constraint.
func TestResolveReplicaPlacementFallsBackToMasterDefault(t *testing.T) {
	cases := []struct {
		name        string
		configRP    string
		defaultRP   string
		wantApplied bool
	}{
		{"explicit config used", "010", "", true},
		{"explicit config wins over default", "010", "100", true},
		{"falls back to master default", "", "010", true},
		{"zero master default = no constraint", "", "000", false},
		{"empty everywhere = no constraint", "", "", false},
		{"invalid value ignored", "", "nonsense", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rp := resolveReplicaPlacement(&Config{ReplicaPlacement: tc.configRP},
				&types.ClusterInfo{DefaultReplicaPlacement: tc.defaultRP})
			if (rp != nil) != tc.wantApplied {
				t.Errorf("config=%q default=%q: applied=%v, want %v", tc.configRP, tc.defaultRP, rp != nil, tc.wantApplied)
			}
		})
	}
}
