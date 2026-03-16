package balance

import (
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

const (
	fullnessThreshold = 1.01
	stateActive       = "ACTIVE"
	stateFull         = "FULL"
)

// filterByState filters metrics by volume state for testing.
func filterByState(metrics []*types.VolumeHealthMetrics, state string) []*types.VolumeHealthMetrics {
	if state != stateActive && state != stateFull {
		return metrics
	}
	var out []*types.VolumeHealthMetrics
	for _, m := range metrics {
		if m == nil {
			continue
		}
		if state == stateActive && m.FullnessRatio < fullnessThreshold {
			out = append(out, m)
		}
		if state == stateFull && m.FullnessRatio >= fullnessThreshold {
			out = append(out, m)
		}
	}
	return out
}

// Integration tests that exercise multiple features together:
// DC/rack/node filters, volume state filtering, replica placement validation,
// and collection scoping all interacting within a single detection run.

// makeVolumesWithOptions generates metrics with additional options.
type volumeOption func(m *types.VolumeHealthMetrics)

func withFullness(ratio float64) volumeOption {
	return func(m *types.VolumeHealthMetrics) { m.FullnessRatio = ratio }
}

func withReplicas(rp int) volumeOption {
	return func(m *types.VolumeHealthMetrics) { m.ExpectedReplicas = rp }
}

func makeVolumesWith(server, diskType, dc, rack, collection string, volumeIDBase uint32, n int, opts ...volumeOption) []*types.VolumeHealthMetrics {
	vols := makeVolumes(server, diskType, dc, rack, collection, volumeIDBase, n)
	for _, v := range vols {
		for _, opt := range opts {
			opt(v)
		}
	}
	return vols
}

// buildReplicaMap builds a VolumeReplicaMap from metrics (each metric is one replica location).
func buildReplicaMap(metrics []*types.VolumeHealthMetrics) map[uint32][]types.ReplicaLocation {
	m := make(map[uint32][]types.ReplicaLocation)
	for _, metric := range metrics {
		m[metric.VolumeID] = append(m[metric.VolumeID], types.ReplicaLocation{
			DataCenter: metric.DataCenter,
			Rack:       metric.Rack,
			NodeID:     metric.Server,
		})
	}
	return m
}

// TestIntegration_DCFilterWithVolumeState tests that DC filtering and volume
// state filtering compose correctly: only ACTIVE volumes in the specified DC
// participate in balancing.
func TestIntegration_DCFilterWithVolumeState(t *testing.T) {
	servers := []serverSpec{
		{id: "node-a", diskType: "hdd", diskID: 1, dc: "dc1", rack: "rack1"},
		{id: "node-b", diskType: "hdd", diskID: 2, dc: "dc1", rack: "rack1"},
		{id: "node-c", diskType: "hdd", diskID: 3, dc: "dc2", rack: "rack1"},
	}

	var allMetrics []*types.VolumeHealthMetrics
	// dc1: node-a has 40 active volumes, node-b has 10 active volumes
	allMetrics = append(allMetrics, makeVolumesWith("node-a", "hdd", "dc1", "rack1", "c1", 1, 40, withFullness(0.5))...)
	allMetrics = append(allMetrics, makeVolumesWith("node-b", "hdd", "dc1", "rack1", "c1", 100, 10, withFullness(0.5))...)
	// dc1: node-a also has 20 FULL volumes that should be excluded
	allMetrics = append(allMetrics, makeVolumesWith("node-a", "hdd", "dc1", "rack1", "c1", 200, 20, withFullness(1.5))...)
	// dc2: node-c has 50 active volumes (should be excluded by DC filter)
	allMetrics = append(allMetrics, makeVolumesWith("node-c", "hdd", "dc2", "rack1", "c1", 300, 50, withFullness(0.5))...)

	// Apply volume state filter (ACTIVE only) first
	activeMetrics := filterByState(allMetrics, stateActive)
	// Then apply DC filter
	dcMetrics := make([]*types.VolumeHealthMetrics, 0)
	for _, m := range activeMetrics {
		if m.DataCenter == "dc1" {
			dcMetrics = append(dcMetrics, m)
		}
	}

	at := buildTopology(servers, allMetrics)
	clusterInfo := &types.ClusterInfo{ActiveTopology: at}

	conf := defaultConf()
	conf.DataCenterFilter = "dc1"

	tasks, _, err := Detection(dcMetrics, clusterInfo, conf, 100)
	if err != nil {
		t.Fatalf("Detection failed: %v", err)
	}
	if len(tasks) == 0 {
		t.Fatal("Expected balance tasks for 40/10 active-only imbalance in dc1, got 0")
	}

	for _, task := range tasks {
		if task.Server == "node-c" {
			t.Error("node-c (dc2) should not be a source")
		}
		if task.TypedParams != nil {
			for _, tgt := range task.TypedParams.Targets {
				if strings.Contains(tgt.Node, "node-c") {
					t.Error("node-c (dc2) should not be a target")
				}
			}
		}
	}

	// Verify convergence uses only the 50 active dc1 volumes (40+10),
	// not the 20 full volumes
	effective := computeEffectiveCounts(servers[:2], dcMetrics, tasks)
	total := 0
	maxC, minC := 0, len(dcMetrics)
	for _, c := range effective {
		total += c
		if c > maxC {
			maxC = c
		}
		if c < minC {
			minC = c
		}
	}
	if total != 50 {
		t.Errorf("Expected 50 total active volumes in dc1, got %d", total)
	}
	t.Logf("DC+state filter: %d tasks, effective=%v", len(tasks), effective)
}

// TestIntegration_NodeFilterWithCollections tests that node filtering works
// correctly when volumes span multiple collections.
func TestIntegration_NodeFilterWithCollections(t *testing.T) {
	servers := []serverSpec{
		{id: "node-a", diskType: "hdd", diskID: 1, dc: "dc1", rack: "rack1"},
		{id: "node-b", diskType: "hdd", diskID: 2, dc: "dc1", rack: "rack1"},
		{id: "node-c", diskType: "hdd", diskID: 3, dc: "dc1", rack: "rack1"},
	}

	var allMetrics []*types.VolumeHealthMetrics
	// node-a: 30 "photos" + 20 "videos" = 50 total
	allMetrics = append(allMetrics, makeVolumes("node-a", "hdd", "dc1", "rack1", "photos", 1, 30)...)
	allMetrics = append(allMetrics, makeVolumes("node-a", "hdd", "dc1", "rack1", "videos", 100, 20)...)
	// node-b: 5 "photos" + 5 "videos" = 10 total
	allMetrics = append(allMetrics, makeVolumes("node-b", "hdd", "dc1", "rack1", "photos", 200, 5)...)
	allMetrics = append(allMetrics, makeVolumes("node-b", "hdd", "dc1", "rack1", "videos", 300, 5)...)
	// node-c: 40 volumes (should be excluded by node filter)
	allMetrics = append(allMetrics, makeVolumes("node-c", "hdd", "dc1", "rack1", "photos", 400, 40)...)

	// Apply node filter
	filteredMetrics := make([]*types.VolumeHealthMetrics, 0)
	for _, m := range allMetrics {
		if m.Server == "node-a" || m.Server == "node-b" {
			filteredMetrics = append(filteredMetrics, m)
		}
	}

	at := buildTopology(servers, allMetrics)
	clusterInfo := &types.ClusterInfo{ActiveTopology: at}

	conf := defaultConf()
	conf.NodeFilter = "node-a,node-b"

	tasks, _, err := Detection(filteredMetrics, clusterInfo, conf, 100)
	if err != nil {
		t.Fatalf("Detection failed: %v", err)
	}
	if len(tasks) == 0 {
		t.Fatal("Expected tasks for 50/10 imbalance within node-a,node-b")
	}

	// All moves should be between node-a and node-b only
	for _, task := range tasks {
		if task.Server != "node-a" && task.Server != "node-b" {
			t.Errorf("Source %s should be node-a or node-b", task.Server)
		}
		if task.TypedParams != nil {
			for _, tgt := range task.TypedParams.Targets {
				if !strings.Contains(tgt.Node, "node-a") && !strings.Contains(tgt.Node, "node-b") {
					t.Errorf("Target %s should be node-a or node-b", tgt.Node)
				}
			}
		}
	}

	assertNoDuplicateVolumes(t, tasks)
	t.Logf("Node filter with mixed collections: %d tasks", len(tasks))
}

// TestIntegration_ReplicaPlacementWithDCFilter tests that replica placement
// validation prevents moves that would violate replication policy even when
// DC filtering restricts the available servers.
func TestIntegration_ReplicaPlacementWithDCFilter(t *testing.T) {
	// Setup: 2 DCs, volumes with rp=100 (1 replica in different DC)
	// DC filter restricts to dc1 only.
	// Replicas: each volume has one copy in dc1 and one in dc2.
	// The balancer should NOT move volumes within dc1 to a node that would
	// violate the cross-DC placement requirement.
	servers := []serverSpec{
		{id: "node-a", diskType: "hdd", diskID: 1, dc: "dc1", rack: "rack1"},
		{id: "node-b", diskType: "hdd", diskID: 2, dc: "dc1", rack: "rack1"},
		{id: "node-c", diskType: "hdd", diskID: 3, dc: "dc2", rack: "rack1"},
	}

	// node-a: 30 volumes with rp=100, node-b: 5 volumes with rp=100
	// Each volume also has a replica on node-c (dc2)
	var allMetrics []*types.VolumeHealthMetrics
	allMetrics = append(allMetrics, makeVolumesWith("node-a", "hdd", "dc1", "rack1", "c1", 1, 30, withReplicas(100))...)
	allMetrics = append(allMetrics, makeVolumesWith("node-b", "hdd", "dc1", "rack1", "c1", 100, 5, withReplicas(100))...)
	// dc2 replicas (not part of filtered metrics, but in replica map)
	dc2Replicas := makeVolumesWith("node-c", "hdd", "dc2", "rack1", "c1", 1, 30, withReplicas(100))

	// Build replica map: volumes 1-30 have replicas on node-a AND node-c
	replicaMap := buildReplicaMap(allMetrics)
	for _, r := range dc2Replicas {
		replicaMap[r.VolumeID] = append(replicaMap[r.VolumeID], types.ReplicaLocation{
			DataCenter: r.DataCenter,
			Rack:       r.Rack,
			NodeID:     r.Server,
		})
	}

	// Filter to dc1 only
	dc1Metrics := make([]*types.VolumeHealthMetrics, 0)
	for _, m := range allMetrics {
		if m.DataCenter == "dc1" {
			dc1Metrics = append(dc1Metrics, m)
		}
	}

	at := buildTopology(servers, allMetrics)
	clusterInfo := &types.ClusterInfo{
		ActiveTopology:   at,
		VolumeReplicaMap: replicaMap,
	}

	conf := defaultConf()
	conf.DataCenterFilter = "dc1"

	tasks, _, err := Detection(dc1Metrics, clusterInfo, conf, 100)
	if err != nil {
		t.Fatalf("Detection failed: %v", err)
	}

	// With rp=100, moving a volume from node-a to node-b is valid because
	// the cross-DC replica on node-c is preserved. The balancer should
	// produce moves that keep the dc2 replica intact.
	if len(tasks) == 0 {
		t.Fatal("Expected tasks: 30/5 imbalance with valid cross-DC replicas")
	}

	for _, task := range tasks {
		// All sources should be from dc1
		if task.Server != "node-a" && task.Server != "node-b" {
			t.Errorf("Source %s should be in dc1", task.Server)
		}
		// All targets should be in dc1
		if task.TypedParams != nil {
			for _, tgt := range task.TypedParams.Targets {
				if strings.Contains(tgt.Node, "node-c") {
					t.Errorf("Target should not be node-c (dc2) with dc1 filter")
				}
			}
		}
	}

	assertNoDuplicateVolumes(t, tasks)
	t.Logf("Replica placement + DC filter: %d tasks", len(tasks))
}

// TestIntegration_RackFilterWithReplicaPlacement tests that rack filtering
// and replica placement validation (rp=010) work together. With rp=010,
// replicas must be on different racks. When rack filtering restricts to one
// rack, the balancer should still produce valid moves within that rack for
// volumes whose replicas satisfy the cross-rack requirement via nodes outside
// the filter.
func TestIntegration_RackFilterWithReplicaPlacement(t *testing.T) {
	servers := []serverSpec{
		{id: "node-a", diskType: "hdd", diskID: 1, dc: "dc1", rack: "rack1"},
		{id: "node-b", diskType: "hdd", diskID: 2, dc: "dc1", rack: "rack1"},
		{id: "node-c", diskType: "hdd", diskID: 3, dc: "dc1", rack: "rack2"},
	}

	// rack1: node-a has 30 volumes, node-b has 5
	// Each volume also has a replica on node-c (rack2), satisfying rp=010
	var allMetrics []*types.VolumeHealthMetrics
	allMetrics = append(allMetrics, makeVolumesWith("node-a", "hdd", "dc1", "rack1", "c1", 1, 30, withReplicas(10))...)
	allMetrics = append(allMetrics, makeVolumesWith("node-b", "hdd", "dc1", "rack1", "c1", 100, 5, withReplicas(10))...)
	rack2Replicas := makeVolumesWith("node-c", "hdd", "dc1", "rack2", "c1", 1, 30, withReplicas(10))

	replicaMap := buildReplicaMap(allMetrics)
	for _, r := range rack2Replicas {
		replicaMap[r.VolumeID] = append(replicaMap[r.VolumeID], types.ReplicaLocation{
			DataCenter: r.DataCenter,
			Rack:       r.Rack,
			NodeID:     r.Server,
		})
	}

	// Filter to rack1 only
	rack1Metrics := make([]*types.VolumeHealthMetrics, 0)
	for _, m := range allMetrics {
		if m.Rack == "rack1" {
			rack1Metrics = append(rack1Metrics, m)
		}
	}

	at := buildTopology(servers, allMetrics)
	clusterInfo := &types.ClusterInfo{
		ActiveTopology:   at,
		VolumeReplicaMap: replicaMap,
	}

	conf := defaultConf()
	conf.RackFilter = "rack1"

	tasks, _, err := Detection(rack1Metrics, clusterInfo, conf, 100)
	if err != nil {
		t.Fatalf("Detection failed: %v", err)
	}

	// Moving within rack1 (node-a → node-b) is valid because the cross-rack
	// replica on node-c (rack2) is preserved.
	if len(tasks) == 0 {
		t.Fatal("Expected tasks for 30/5 imbalance within rack1")
	}

	for _, task := range tasks {
		if task.Server == "node-c" {
			t.Error("node-c (rack2) should not be a source with rack1 filter")
		}
		if task.TypedParams != nil {
			for _, tgt := range task.TypedParams.Targets {
				if strings.Contains(tgt.Node, "node-c") {
					t.Error("node-c (rack2) should not be a target with rack1 filter")
				}
			}
		}
	}

	assertNoDuplicateVolumes(t, tasks)
	t.Logf("Rack filter + replica placement: %d tasks", len(tasks))
}

// TestIntegration_AllFactors exercises all filtering dimensions simultaneously:
// DC filter, rack filter, volume state filter, replica placement validation,
// and mixed collections.
func TestIntegration_AllFactors(t *testing.T) {
	servers := []serverSpec{
		{id: "node-a", diskType: "hdd", diskID: 1, dc: "dc1", rack: "rack1"},
		{id: "node-b", diskType: "hdd", diskID: 2, dc: "dc1", rack: "rack1"},
		{id: "node-c", diskType: "hdd", diskID: 3, dc: "dc1", rack: "rack2"}, // excluded by rack filter
		{id: "node-d", diskType: "hdd", diskID: 4, dc: "dc2", rack: "rack1"}, // excluded by DC filter
	}

	var allMetrics []*types.VolumeHealthMetrics

	// node-a: 25 active "photos" + 10 full "photos" (full excluded by state filter)
	allMetrics = append(allMetrics, makeVolumesWith("node-a", "hdd", "dc1", "rack1", "photos", 1, 25, withFullness(0.5), withReplicas(100))...)
	allMetrics = append(allMetrics, makeVolumesWith("node-a", "hdd", "dc1", "rack1", "photos", 200, 10, withFullness(1.5), withReplicas(100))...)

	// node-b: 5 active "photos"
	allMetrics = append(allMetrics, makeVolumesWith("node-b", "hdd", "dc1", "rack1", "photos", 100, 5, withFullness(0.5), withReplicas(100))...)

	// node-c: 20 volumes in dc1/rack2 (excluded by rack filter)
	allMetrics = append(allMetrics, makeVolumesWith("node-c", "hdd", "dc1", "rack2", "photos", 300, 20, withFullness(0.5))...)

	// node-d: 30 volumes in dc2 (excluded by DC filter, but provides cross-DC replicas)
	dc2Replicas := makeVolumesWith("node-d", "hdd", "dc2", "rack1", "photos", 1, 25, withFullness(0.5), withReplicas(100))

	// Build replica map: volumes 1-25 have cross-DC replicas on node-d
	replicaMap := buildReplicaMap(allMetrics)
	for _, r := range dc2Replicas {
		replicaMap[r.VolumeID] = append(replicaMap[r.VolumeID], types.ReplicaLocation{
			DataCenter: r.DataCenter,
			Rack:       r.Rack,
			NodeID:     r.Server,
		})
	}

	// Apply all filters: ACTIVE state, dc1, rack1
	filtered := filterByState(allMetrics, stateActive)
	var finalMetrics []*types.VolumeHealthMetrics
	for _, m := range filtered {
		if m.DataCenter == "dc1" && m.Rack == "rack1" {
			finalMetrics = append(finalMetrics, m)
		}
	}

	at := buildTopology(servers, allMetrics)
	clusterInfo := &types.ClusterInfo{
		ActiveTopology:   at,
		VolumeReplicaMap: replicaMap,
	}

	conf := defaultConf()
	conf.DataCenterFilter = "dc1"
	conf.RackFilter = "rack1"

	tasks, _, err := Detection(finalMetrics, clusterInfo, conf, 100)
	if err != nil {
		t.Fatalf("Detection failed: %v", err)
	}
	if len(tasks) == 0 {
		t.Fatal("Expected tasks for 25/5 active imbalance in dc1/rack1")
	}

	// Verify all moves stay within dc1/rack1 scope
	for i, task := range tasks {
		if task.Server != "node-a" && task.Server != "node-b" {
			t.Errorf("Task %d: source %s should be node-a or node-b", i, task.Server)
		}
		if task.TypedParams != nil {
			for _, tgt := range task.TypedParams.Targets {
				node := tgt.Node
				if !strings.Contains(node, "node-a") && !strings.Contains(node, "node-b") {
					t.Errorf("Task %d: target %s should be node-a or node-b", i, node)
				}
			}
		}
	}

	assertNoDuplicateVolumes(t, tasks)

	// Verify convergence
	effective := computeEffectiveCounts(servers[:2], finalMetrics, tasks)
	total := 0
	maxC, minC := 0, len(finalMetrics)
	for _, c := range effective {
		total += c
		if c > maxC {
			maxC = c
		}
		if c < minC {
			minC = c
		}
	}

	// Should have balanced the 30 active dc1/rack1 volumes (25+5)
	if total != 30 {
		t.Errorf("Expected 30 total filtered volumes, got %d", total)
	}
	avg := float64(total) / float64(len(effective))
	imbalance := float64(maxC-minC) / avg
	if imbalance > conf.ImbalanceThreshold {
		t.Errorf("Still imbalanced: effective=%v, imbalance=%.1f%% (threshold=%.1f%%)",
			effective, imbalance*100, conf.ImbalanceThreshold*100)
	}

	t.Logf("All factors combined: %d tasks, effective=%v", len(tasks), effective)
}

// TestIntegration_FullVolumesOnlyBalancing verifies that with FULL state filter,
// only full volumes participate in balancing.
func TestIntegration_FullVolumesOnlyBalancing(t *testing.T) {
	servers := []serverSpec{
		{id: "node-a", diskType: "hdd", diskID: 1, dc: "dc1", rack: "rack1"},
		{id: "node-b", diskType: "hdd", diskID: 2, dc: "dc1", rack: "rack1"},
	}

	var allMetrics []*types.VolumeHealthMetrics
	// node-a: 10 active + 30 full
	allMetrics = append(allMetrics, makeVolumesWith("node-a", "hdd", "dc1", "rack1", "c1", 1, 10, withFullness(0.5))...)
	allMetrics = append(allMetrics, makeVolumesWith("node-a", "hdd", "dc1", "rack1", "c1", 100, 30, withFullness(1.5))...)
	// node-b: 10 active + 5 full
	allMetrics = append(allMetrics, makeVolumesWith("node-b", "hdd", "dc1", "rack1", "c1", 200, 10, withFullness(0.5))...)
	allMetrics = append(allMetrics, makeVolumesWith("node-b", "hdd", "dc1", "rack1", "c1", 300, 5, withFullness(1.5))...)

	// Filter to FULL only
	fullMetrics := filterByState(allMetrics, stateFull)

	at := buildTopology(servers, allMetrics)
	clusterInfo := &types.ClusterInfo{ActiveTopology: at}

	tasks, _, err := Detection(fullMetrics, clusterInfo, defaultConf(), 100)
	if err != nil {
		t.Fatalf("Detection failed: %v", err)
	}
	if len(tasks) == 0 {
		t.Fatal("Expected tasks for 30/5 full volume imbalance")
	}

	// Verify only full volumes are moved (IDs 100-129 from node-a, 300-304 from node-b)
	for _, task := range tasks {
		vid := task.VolumeID
		// Active volumes are IDs 1-10 (node-a) and 200-209 (node-b)
		if (vid >= 1 && vid <= 10) || (vid >= 200 && vid <= 209) {
			t.Errorf("Task moved active volume %d, should only move full volumes", vid)
		}
	}

	assertNoDuplicateVolumes(t, tasks)
	t.Logf("Full-only balancing: %d tasks", len(tasks))
}
