package balance

import (
	"fmt"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// serverSpec describes a server for the topology builder.
type serverSpec struct {
	id         string // e.g. "node-1"
	diskType   string // e.g. "ssd", "hdd"
	diskID     uint32
	dc         string
	rack       string
	maxVolumes int64
}

// buildTopology constructs an ActiveTopology from server specs and volume metrics.
func buildTopology(servers []serverSpec, metrics []*types.VolumeHealthMetrics) *topology.ActiveTopology {
	at := topology.NewActiveTopology(0)

	volumesByServer := make(map[string][]*master_pb.VolumeInformationMessage)
	for _, m := range metrics {
		volumesByServer[m.Server] = append(volumesByServer[m.Server], &master_pb.VolumeInformationMessage{
			Id:         m.VolumeID,
			Size:       m.Size,
			Collection: m.Collection,
			Version:    1,
		})
	}

	// Group servers by dc → rack for topology construction
	type rackKey struct{ dc, rack string }
	rackNodes := make(map[rackKey][]*master_pb.DataNodeInfo)

	for _, s := range servers {
		maxVol := s.maxVolumes
		if maxVol == 0 {
			maxVol = 1000
		}
		node := &master_pb.DataNodeInfo{
			Id:      s.id,
			Address: s.id + ":8080",
			DiskInfos: map[string]*master_pb.DiskInfo{
				s.diskType: {
					Type:           s.diskType,
					DiskId:         s.diskID,
					VolumeInfos:    volumesByServer[s.id],
					VolumeCount:    int64(len(volumesByServer[s.id])),
					MaxVolumeCount: maxVol,
				},
			},
		}
		key := rackKey{s.dc, s.rack}
		rackNodes[key] = append(rackNodes[key], node)
	}

	// Build DC → Rack tree
	dcRacks := make(map[string][]*master_pb.RackInfo)
	for key, nodes := range rackNodes {
		dcRacks[key.dc] = append(dcRacks[key.dc], &master_pb.RackInfo{
			Id:            key.rack,
			DataNodeInfos: nodes,
		})
	}

	var dcInfos []*master_pb.DataCenterInfo
	for dcID, racks := range dcRacks {
		dcInfos = append(dcInfos, &master_pb.DataCenterInfo{
			Id:        dcID,
			RackInfos: racks,
		})
	}

	at.UpdateTopology(&master_pb.TopologyInfo{DataCenterInfos: dcInfos})
	return at
}

// makeVolumes generates n VolumeHealthMetrics for a server starting at volumeIDBase.
func makeVolumes(server, diskType, dc, rack, collection string, volumeIDBase uint32, n int) []*types.VolumeHealthMetrics {
	out := make([]*types.VolumeHealthMetrics, n)
	for i := range out {
		out[i] = &types.VolumeHealthMetrics{
			VolumeID:      volumeIDBase + uint32(i),
			Server:        server,
			ServerAddress: server + ":8080",
			DiskType:      diskType,
			Collection:    collection,
			Size:          1024,
			DataCenter:    dc,
			Rack:          rack,
		}
	}
	return out
}

func defaultConf() *Config {
	return &Config{
		BaseConfig: base.BaseConfig{
			Enabled:             true,
			ScanIntervalSeconds: 30,
			MaxConcurrent:       1,
		},
		MinServerCount:     2,
		ImbalanceThreshold: 0.2,
	}
}

// assertNoDuplicateVolumes verifies every task moves a distinct volume.
func assertNoDuplicateVolumes(t *testing.T, tasks []*types.TaskDetectionResult) {
	t.Helper()
	seen := make(map[uint32]bool)
	for i, task := range tasks {
		if seen[task.VolumeID] {
			t.Errorf("duplicate volume %d in task %d", task.VolumeID, i)
		}
		seen[task.VolumeID] = true
	}
}

// computeEffectiveCounts returns per-server volume counts after applying all planned moves.
// servers seeds the map so that empty destination servers (no volumes in metrics) are tracked.
func computeEffectiveCounts(servers []serverSpec, metrics []*types.VolumeHealthMetrics, tasks []*types.TaskDetectionResult) map[string]int {
	// Build address → server ID mapping from the topology spec
	addrToServer := make(map[string]string, len(servers))
	counts := make(map[string]int, len(servers))
	for _, s := range servers {
		counts[s.id] = 0
		addrToServer[s.id+":8080"] = s.id
		addrToServer[s.id] = s.id
	}
	for _, m := range metrics {
		counts[m.Server]++
	}
	for _, task := range tasks {
		counts[task.Server]-- // source loses one
		if task.TypedParams != nil && len(task.TypedParams.Targets) > 0 {
			addr := task.TypedParams.Targets[0].Node
			if serverID, ok := addrToServer[addr]; ok {
				counts[serverID]++
			}
		}
	}
	return counts
}

func createMockTopology(volumes ...*types.VolumeHealthMetrics) *topology.ActiveTopology {
	at := topology.NewActiveTopology(0)

	// Group volumes by server for easier topology construction
	volumesByServer := make(map[string][]*master_pb.VolumeInformationMessage)
	for _, v := range volumes {
		if _, ok := volumesByServer[v.Server]; !ok {
			volumesByServer[v.Server] = []*master_pb.VolumeInformationMessage{}
		}
		volumesByServer[v.Server] = append(volumesByServer[v.Server], &master_pb.VolumeInformationMessage{
			Id:               v.VolumeID,
			Size:             v.Size,
			Collection:       v.Collection,
			ReplicaPlacement: 0,
			Ttl:              0,
			Version:          1,
		})
	}

	topoInfo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				Id: "dc1",
				RackInfos: []*master_pb.RackInfo{
					{
						Id: "rack1",
						DataNodeInfos: []*master_pb.DataNodeInfo{
							// SSD Nodes
							{
								Id:      "ssd-server-1",
								Address: "ssd-server-1:8080",
								DiskInfos: map[string]*master_pb.DiskInfo{
									"ssd": {
										Type:           "ssd",
										DiskId:         1,
										VolumeInfos:    volumesByServer["ssd-server-1"],
										MaxVolumeCount: 1000,
									},
								},
							},
							{
								Id:      "ssd-server-2",
								Address: "ssd-server-2:8080",
								DiskInfos: map[string]*master_pb.DiskInfo{
									"ssd": {
										Type:           "ssd",
										DiskId:         2,
										VolumeInfos:    volumesByServer["ssd-server-2"],
										MaxVolumeCount: 1000,
									},
								},
							},
							// HDD Nodes
							{
								Id:      "hdd-server-1",
								Address: "hdd-server-1:8080",
								DiskInfos: map[string]*master_pb.DiskInfo{
									"hdd": {
										Type:           "hdd",
										DiskId:         3, // Changed index to avoid conflict
										VolumeInfos:    volumesByServer["hdd-server-1"],
										MaxVolumeCount: 1000,
									},
								},
							},
							{
								Id:      "hdd-server-2",
								Address: "hdd-server-2:8080",
								DiskInfos: map[string]*master_pb.DiskInfo{
									"hdd": {
										Type:           "hdd",
										DiskId:         4,
										VolumeInfos:    volumesByServer["hdd-server-2"],
										MaxVolumeCount: 1000,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	at.UpdateTopology(topoInfo)
	return at
}

func TestDetection_MixedDiskTypes(t *testing.T) {
	// Setup metrics
	// 2 SSD servers with 10 volumes each (Balanced)
	// 2 HDD servers with 100 volumes each (Balanced)

	metrics := []*types.VolumeHealthMetrics{}

	// SSD Servers
	for i := 0; i < 10; i++ {
		metrics = append(metrics, &types.VolumeHealthMetrics{
			VolumeID:      uint32(i + 1),
			Server:        "ssd-server-1",
			ServerAddress: "ssd-server-1:8080",
			DiskType:      "ssd",
			Collection:    "c1",
			Size:          1024,
			DataCenter:    "dc1",
			Rack:          "rack1",
		})
	}
	for i := 0; i < 10; i++ {
		metrics = append(metrics, &types.VolumeHealthMetrics{
			VolumeID:      uint32(20 + i + 1),
			Server:        "ssd-server-2",
			ServerAddress: "ssd-server-2:8080",
			DiskType:      "ssd",
			Collection:    "c1",
			Size:          1024,
			DataCenter:    "dc1",
			Rack:          "rack1",
		})
	}

	// HDD Servers
	for i := 0; i < 100; i++ {
		metrics = append(metrics, &types.VolumeHealthMetrics{
			VolumeID:      uint32(100 + i + 1),
			Server:        "hdd-server-1",
			ServerAddress: "hdd-server-1:8080",
			DiskType:      "hdd",
			Collection:    "c1",
			Size:          1024,
			DataCenter:    "dc1",
			Rack:          "rack1",
		})
	}
	for i := 0; i < 100; i++ {
		metrics = append(metrics, &types.VolumeHealthMetrics{
			VolumeID:      uint32(200 + i + 1),
			Server:        "hdd-server-2",
			ServerAddress: "hdd-server-2:8080",
			DiskType:      "hdd",
			Collection:    "c1",
			Size:          1024,
			DataCenter:    "dc1",
			Rack:          "rack1",
		})
	}

	conf := &Config{
		BaseConfig: base.BaseConfig{
			Enabled:             true,
			ScanIntervalSeconds: 30,
			MaxConcurrent:       1,
		},
		MinServerCount:     2,
		ImbalanceThreshold: 0.2, // 20%
	}

	at := createMockTopology(metrics...)
	clusterInfo := &types.ClusterInfo{
		ActiveTopology: at,
	}

	tasks, _, err := Detection(metrics, clusterInfo, conf, 100)
	if err != nil {
		t.Fatalf("Detection failed: %v", err)
	}

	if len(tasks) != 0 {
		t.Errorf("Expected 0 tasks for balanced mixed types, got %d", len(tasks))
		for _, task := range tasks {
			t.Logf("Computed Task: %+v", task.Reason)
		}
	}
}

func TestDetection_ImbalancedDiskType(t *testing.T) {
	// Setup metrics
	// 2 SSD servers: One with 100, One with 10. Imbalance!
	metrics := []*types.VolumeHealthMetrics{}

	// Server 1 (Overloaded SSD)
	for i := 0; i < 100; i++ {
		metrics = append(metrics, &types.VolumeHealthMetrics{
			VolumeID:      uint32(i + 1),
			Server:        "ssd-server-1",
			ServerAddress: "ssd-server-1:8080",
			DiskType:      "ssd",
			Collection:    "c1",
			Size:          1024,
			DataCenter:    "dc1",
			Rack:          "rack1",
		})
	}
	// Server 2 (Underloaded SSD)
	for i := 0; i < 10; i++ {
		metrics = append(metrics, &types.VolumeHealthMetrics{
			VolumeID:      uint32(100 + i + 1),
			Server:        "ssd-server-2",
			ServerAddress: "ssd-server-2:8080",
			DiskType:      "ssd",
			Collection:    "c1",
			Size:          1024,
			DataCenter:    "dc1",
			Rack:          "rack1",
		})
	}

	conf := &Config{
		BaseConfig: base.BaseConfig{
			Enabled:             true,
			ScanIntervalSeconds: 30,
			MaxConcurrent:       1,
		},
		MinServerCount:     2,
		ImbalanceThreshold: 0.2,
	}

	at := createMockTopology(metrics...)
	clusterInfo := &types.ClusterInfo{
		ActiveTopology: at,
	}

	tasks, _, err := Detection(metrics, clusterInfo, conf, 100)
	if err != nil {
		t.Fatalf("Detection failed: %v", err)
	}

	if len(tasks) == 0 {
		t.Error("Expected tasks for imbalanced SSD cluster, got 0")
	}

	// With 100 volumes on server-1 and 10 on server-2, avg=55, detection should
	// propose multiple moves until imbalance drops below 20% threshold.
	// All tasks should move volumes from ssd-server-1 to ssd-server-2.
	if len(tasks) < 2 {
		t.Errorf("Expected multiple balance tasks, got %d", len(tasks))
	}

	for i, task := range tasks {
		if task.VolumeID == 0 {
			t.Errorf("Task %d has invalid VolumeID", i)
		}
		if task.TypedParams.Sources[0].Node != "ssd-server-1:8080" {
			t.Errorf("Task %d: expected source ssd-server-1:8080, got %s", i, task.TypedParams.Sources[0].Node)
		}
		if task.TypedParams.Targets[0].Node != "ssd-server-2:8080" {
			t.Errorf("Task %d: expected target ssd-server-2:8080, got %s", i, task.TypedParams.Targets[0].Node)
		}
	}
}

func TestDetection_RespectsMaxResults(t *testing.T) {
	// Setup: 2 SSD servers with big imbalance (100 vs 10)
	metrics := []*types.VolumeHealthMetrics{}

	for i := 0; i < 100; i++ {
		metrics = append(metrics, &types.VolumeHealthMetrics{
			VolumeID:      uint32(i + 1),
			Server:        "ssd-server-1",
			ServerAddress: "ssd-server-1:8080",
			DiskType:      "ssd",
			Collection:    "c1",
			Size:          1024,
			DataCenter:    "dc1",
			Rack:          "rack1",
		})
	}
	for i := 0; i < 10; i++ {
		metrics = append(metrics, &types.VolumeHealthMetrics{
			VolumeID:      uint32(100 + i + 1),
			Server:        "ssd-server-2",
			ServerAddress: "ssd-server-2:8080",
			DiskType:      "ssd",
			Collection:    "c1",
			Size:          1024,
			DataCenter:    "dc1",
			Rack:          "rack1",
		})
	}

	conf := &Config{
		BaseConfig: base.BaseConfig{
			Enabled:             true,
			ScanIntervalSeconds: 30,
			MaxConcurrent:       1,
		},
		MinServerCount:     2,
		ImbalanceThreshold: 0.2,
	}

	at := createMockTopology(metrics...)
	clusterInfo := &types.ClusterInfo{
		ActiveTopology: at,
	}

	// Request only 3 results — there are enough volumes to produce more,
	// so truncated should be true.
	tasks, truncated, err := Detection(metrics, clusterInfo, conf, 3)
	if err != nil {
		t.Fatalf("Detection failed: %v", err)
	}

	if len(tasks) != 3 {
		t.Errorf("Expected exactly 3 tasks (maxResults=3), got %d", len(tasks))
	}
	if !truncated {
		t.Errorf("Expected truncated=true when maxResults caps results")
	}

	// Verify truncated=false when detection finishes naturally (no cap)
	at2 := createMockTopology(metrics...)
	clusterInfo2 := &types.ClusterInfo{ActiveTopology: at2}
	tasks2, truncated2, err := Detection(metrics, clusterInfo2, conf, 500)
	if err != nil {
		t.Fatalf("Detection failed: %v", err)
	}
	if truncated2 {
		t.Errorf("Expected truncated=false when detection finishes naturally, got true (len=%d)", len(tasks2))
	}
}

// --- Complicated scenario tests ---

// TestDetection_ThreeServers_ConvergesToBalance verifies that with 3 servers
// (60/30/10 volumes) the algorithm moves volumes from the heaviest server first,
// then re-evaluates, potentially shifting from the second-heaviest too.
func TestDetection_ThreeServers_ConvergesToBalance(t *testing.T) {
	servers := []serverSpec{
		{id: "node-a", diskType: "hdd", diskID: 1, dc: "dc1", rack: "rack1"},
		{id: "node-b", diskType: "hdd", diskID: 2, dc: "dc1", rack: "rack1"},
		{id: "node-c", diskType: "hdd", diskID: 3, dc: "dc1", rack: "rack1"},
	}

	var metrics []*types.VolumeHealthMetrics
	metrics = append(metrics, makeVolumes("node-a", "hdd", "dc1", "rack1", "c1", 1, 60)...)
	metrics = append(metrics, makeVolumes("node-b", "hdd", "dc1", "rack1", "c1", 100, 30)...)
	metrics = append(metrics, makeVolumes("node-c", "hdd", "dc1", "rack1", "c1", 200, 10)...)

	at := buildTopology(servers, metrics)
	clusterInfo := &types.ClusterInfo{ActiveTopology: at}

	tasks, _, err := Detection(metrics, clusterInfo, defaultConf(), 100)
	if err != nil {
		t.Fatalf("Detection failed: %v", err)
	}

	if len(tasks) < 2 {
		t.Fatalf("Expected multiple tasks for 60/30/10 imbalance, got %d", len(tasks))
	}

	assertNoDuplicateVolumes(t, tasks)

	// Verify convergence: effective counts should be within 20% imbalance.
	effective := computeEffectiveCounts(servers, metrics, tasks)
	total := 0
	maxC, minC := 0, len(metrics)
	for _, c := range effective {
		total += c
		if c > maxC {
			maxC = c
		}
		if c < minC {
			minC = c
		}
	}
	avg := float64(total) / float64(len(effective))
	imbalance := float64(maxC-minC) / avg
	if imbalance > 0.2 {
		t.Errorf("After %d moves, cluster still imbalanced: effective=%v, imbalance=%.1f%%",
			len(tasks), effective, imbalance*100)
	}

	// All sources should be from the overloaded nodes, never node-c
	for i, task := range tasks {
		src := task.TypedParams.Sources[0].Node
		if src == "node-c:8080" {
			t.Errorf("Task %d: should not move FROM the underloaded server node-c", i)
		}
	}
}

// TestDetection_SkipsPreExistingPendingTasks verifies that volumes with
// already-registered pending tasks in ActiveTopology are skipped.
func TestDetection_SkipsPreExistingPendingTasks(t *testing.T) {
	servers := []serverSpec{
		{id: "node-a", diskType: "hdd", diskID: 1, dc: "dc1", rack: "rack1"},
		{id: "node-b", diskType: "hdd", diskID: 2, dc: "dc1", rack: "rack1"},
	}

	// node-a has 20, node-b has 5
	var metrics []*types.VolumeHealthMetrics
	metrics = append(metrics, makeVolumes("node-a", "hdd", "dc1", "rack1", "c1", 1, 20)...)
	metrics = append(metrics, makeVolumes("node-b", "hdd", "dc1", "rack1", "c1", 100, 5)...)

	at := buildTopology(servers, metrics)

	// Pre-register pending tasks for the first 15 volumes on node-a.
	// This simulates a previous detection run that already planned moves.
	for i := 0; i < 15; i++ {
		volID := uint32(1 + i)
		err := at.AddPendingTask(topology.TaskSpec{
			TaskID:     fmt.Sprintf("existing-%d", volID),
			TaskType:   topology.TaskTypeBalance,
			VolumeID:   volID,
			VolumeSize: 1024,
			Sources:    []topology.TaskSourceSpec{{ServerID: "node-a", DiskID: 1}},
			Destinations: []topology.TaskDestinationSpec{{ServerID: "node-b", DiskID: 2}},
		})
		if err != nil {
			t.Fatalf("AddPendingTask failed: %v", err)
		}
	}

	clusterInfo := &types.ClusterInfo{ActiveTopology: at}
	tasks, _, err := Detection(metrics, clusterInfo, defaultConf(), 100)
	if err != nil {
		t.Fatalf("Detection failed: %v", err)
	}

	// None of the results should reference a volume with an existing task (IDs 1-15).
	for i, task := range tasks {
		if task.VolumeID >= 1 && task.VolumeID <= 15 {
			t.Errorf("Task %d: volume %d already has a pending task, should have been skipped",
				i, task.VolumeID)
		}
	}

	// With 15 pending A→B moves, effective counts are A=5, B=20.
	// Detection sees B as overloaded and may plan moves from B (5 volumes).
	// Should produce a reasonable number of tasks without over-scheduling.
	if len(tasks) > 5 {
		t.Errorf("Expected at most 5 new tasks, got %d", len(tasks))
	}
	if len(tasks) == 0 {
		t.Errorf("Expected at least 1 new task since projected imbalance still exists")
	}

	assertNoDuplicateVolumes(t, tasks)
}

// TestDetection_NoDuplicateVolumesAcrossIterations verifies that the loop
// never selects the same volume twice, even under high maxResults.
func TestDetection_NoDuplicateVolumesAcrossIterations(t *testing.T) {
	servers := []serverSpec{
		{id: "node-a", diskType: "ssd", diskID: 1, dc: "dc1", rack: "rack1"},
		{id: "node-b", diskType: "ssd", diskID: 2, dc: "dc1", rack: "rack1"},
	}

	var metrics []*types.VolumeHealthMetrics
	metrics = append(metrics, makeVolumes("node-a", "ssd", "dc1", "rack1", "c1", 1, 50)...)
	metrics = append(metrics, makeVolumes("node-b", "ssd", "dc1", "rack1", "c1", 100, 10)...)

	at := buildTopology(servers, metrics)
	clusterInfo := &types.ClusterInfo{ActiveTopology: at}

	tasks, _, err := Detection(metrics, clusterInfo, defaultConf(), 200)
	if err != nil {
		t.Fatalf("Detection failed: %v", err)
	}

	if len(tasks) <= 1 {
		t.Fatalf("Expected multiple tasks to verify no-duplicate invariant across iterations, got %d", len(tasks))
	}

	assertNoDuplicateVolumes(t, tasks)
}

// TestDetection_ThreeServers_MaxServerShifts verifies that after enough moves
// from the top server, the algorithm detects a new max server and moves from it.
func TestDetection_ThreeServers_MaxServerShifts(t *testing.T) {
	servers := []serverSpec{
		{id: "node-a", diskType: "hdd", diskID: 1, dc: "dc1", rack: "rack1"},
		{id: "node-b", diskType: "hdd", diskID: 2, dc: "dc1", rack: "rack1"},
		{id: "node-c", diskType: "hdd", diskID: 3, dc: "dc1", rack: "rack1"},
	}

	// node-a: 40, node-b: 38, node-c: 10. avg ≈ 29.3
	// Initial imbalance = (40-10)/29.3 ≈ 1.02 → move from node-a.
	// After a few moves from node-a, node-b becomes the new max and should be
	// picked as the source.
	var metrics []*types.VolumeHealthMetrics
	metrics = append(metrics, makeVolumes("node-a", "hdd", "dc1", "rack1", "c1", 1, 40)...)
	metrics = append(metrics, makeVolumes("node-b", "hdd", "dc1", "rack1", "c1", 100, 38)...)
	metrics = append(metrics, makeVolumes("node-c", "hdd", "dc1", "rack1", "c1", 200, 10)...)

	at := buildTopology(servers, metrics)
	clusterInfo := &types.ClusterInfo{ActiveTopology: at}

	tasks, _, err := Detection(metrics, clusterInfo, defaultConf(), 100)
	if err != nil {
		t.Fatalf("Detection failed: %v", err)
	}

	if len(tasks) < 3 {
		t.Fatalf("Expected several tasks for 40/38/10 imbalance, got %d", len(tasks))
	}

	// Collect source servers
	sourceServers := make(map[string]int)
	for _, task := range tasks {
		sourceServers[task.Server]++
	}

	// Both node-a and node-b should appear as sources (max server shifts)
	if sourceServers["node-a"] == 0 {
		t.Error("Expected node-a to be a source for some moves")
	}
	if sourceServers["node-b"] == 0 {
		t.Error("Expected node-b to be a source after node-a is drained enough")
	}
	if sourceServers["node-c"] > 0 {
		t.Error("node-c (underloaded) should never be a source")
	}

	assertNoDuplicateVolumes(t, tasks)
}

// TestDetection_FourServers_DestinationSpreading verifies that with 4 servers
// (1 heavy, 3 light) the algorithm spreads moves across multiple destinations.
func TestDetection_FourServers_DestinationSpreading(t *testing.T) {
	servers := []serverSpec{
		{id: "node-a", diskType: "ssd", diskID: 1, dc: "dc1", rack: "rack1"},
		{id: "node-b", diskType: "ssd", diskID: 2, dc: "dc1", rack: "rack2"},
		{id: "node-c", diskType: "ssd", diskID: 3, dc: "dc1", rack: "rack3"},
		{id: "node-d", diskType: "ssd", diskID: 4, dc: "dc1", rack: "rack4"},
	}

	// node-a: 80, b/c/d: 5 each. avg=23.75
	var metrics []*types.VolumeHealthMetrics
	metrics = append(metrics, makeVolumes("node-a", "ssd", "dc1", "rack1", "c1", 1, 80)...)
	metrics = append(metrics, makeVolumes("node-b", "ssd", "dc1", "rack2", "c1", 100, 5)...)
	metrics = append(metrics, makeVolumes("node-c", "ssd", "dc1", "rack3", "c1", 200, 5)...)
	metrics = append(metrics, makeVolumes("node-d", "ssd", "dc1", "rack4", "c1", 300, 5)...)

	at := buildTopology(servers, metrics)
	clusterInfo := &types.ClusterInfo{ActiveTopology: at}

	tasks, _, err := Detection(metrics, clusterInfo, defaultConf(), 100)
	if err != nil {
		t.Fatalf("Detection failed: %v", err)
	}

	if len(tasks) < 5 {
		t.Fatalf("Expected many tasks, got %d", len(tasks))
	}

	// Count destination servers
	destServers := make(map[string]int)
	for _, task := range tasks {
		if task.TypedParams != nil && len(task.TypedParams.Targets) > 0 {
			destServers[task.TypedParams.Targets[0].Node]++
		}
	}

	// With 3 eligible destinations (b, c, d) and pending-task-aware scoring,
	// moves should go to more than just one destination.
	if len(destServers) < 2 {
		t.Errorf("Expected moves to spread across destinations, but only got: %v", destServers)
	}

	assertNoDuplicateVolumes(t, tasks)
}

// TestDetection_ConvergenceVerification verifies that after all planned moves,
// the effective volume distribution is within the configured threshold.
func TestDetection_ConvergenceVerification(t *testing.T) {
	tests := []struct {
		name       string
		counts     []int // volumes per server
		threshold  float64
	}{
		{"2-server-big-gap", []int{100, 10}, 0.2},
		{"3-server-staircase", []int{90, 50, 10}, 0.2},
		{"4-server-one-hot", []int{200, 20, 20, 20}, 0.2},
		{"3-server-tight-threshold", []int{30, 20, 10}, 0.1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var servers []serverSpec
			var metrics []*types.VolumeHealthMetrics
			volBase := uint32(1)

			for i, count := range tt.counts {
				id := fmt.Sprintf("node-%d", i)
				servers = append(servers, serverSpec{
					id: id, diskType: "hdd", diskID: uint32(i + 1),
					dc: "dc1", rack: "rack1",
				})
				metrics = append(metrics, makeVolumes(id, "hdd", "dc1", "rack1", "c1", volBase, count)...)
				volBase += uint32(count)
			}

			at := buildTopology(servers, metrics)
			clusterInfo := &types.ClusterInfo{ActiveTopology: at}

			conf := defaultConf()
			conf.ImbalanceThreshold = tt.threshold

			tasks, _, err := Detection(metrics, clusterInfo, conf, 500)
			if err != nil {
				t.Fatalf("Detection failed: %v", err)
			}

			if len(tasks) == 0 {
				t.Fatal("Expected balance tasks, got 0")
			}

			assertNoDuplicateVolumes(t, tasks)

			// Verify convergence
			effective := computeEffectiveCounts(servers, metrics, tasks)
			total := 0
			maxC, minC := 0, len(metrics)
			for _, c := range effective {
				total += c
				if c > maxC {
					maxC = c
				}
				if c < minC {
					minC = c
				}
			}
			avg := float64(total) / float64(len(effective))
			imbalance := float64(maxC-minC) / avg
			if imbalance > tt.threshold {
				t.Errorf("After %d moves, still imbalanced: effective=%v, imbalance=%.1f%% (threshold=%.1f%%)",
					len(tasks), effective, imbalance*100, tt.threshold*100)
			}
			t.Logf("%s: %d moves, effective=%v, imbalance=%.1f%%",
				tt.name, len(tasks), effective, imbalance*100)
		})
	}
}

// TestDetection_ExhaustedServerFallsThrough verifies that when the most
// overloaded server has all its volumes blocked by pre-existing tasks,
// the algorithm falls through to the next overloaded server instead of stopping.
func TestDetection_ExhaustedServerFallsThrough(t *testing.T) {
	servers := []serverSpec{
		{id: "node-a", diskType: "hdd", diskID: 1, dc: "dc1", rack: "rack1"},
		{id: "node-b", diskType: "hdd", diskID: 2, dc: "dc1", rack: "rack1"},
		{id: "node-c", diskType: "hdd", diskID: 3, dc: "dc1", rack: "rack1"},
	}

	// node-a: 50 volumes, node-b: 40 volumes, node-c: 10 volumes
	// avg = 33.3, imbalance = (50-10)/33.3 = 1.2 > 0.2
	var metrics []*types.VolumeHealthMetrics
	metrics = append(metrics, makeVolumes("node-a", "hdd", "dc1", "rack1", "c1", 1, 50)...)
	metrics = append(metrics, makeVolumes("node-b", "hdd", "dc1", "rack1", "c1", 100, 40)...)
	metrics = append(metrics, makeVolumes("node-c", "hdd", "dc1", "rack1", "c1", 200, 10)...)

	at := buildTopology(servers, metrics)

	// Block ALL of node-a's volumes with pre-existing tasks
	for i := 0; i < 50; i++ {
		volID := uint32(1 + i)
		err := at.AddPendingTask(topology.TaskSpec{
			TaskID:       fmt.Sprintf("existing-%d", volID),
			TaskType:     topology.TaskTypeBalance,
			VolumeID:     volID,
			VolumeSize:   1024,
			Sources:      []topology.TaskSourceSpec{{ServerID: "node-a", DiskID: 1}},
			Destinations: []topology.TaskDestinationSpec{{ServerID: "node-c", DiskID: 3}},
		})
		if err != nil {
			t.Fatalf("AddPendingTask failed: %v", err)
		}
	}

	clusterInfo := &types.ClusterInfo{ActiveTopology: at}
	tasks, _, err := Detection(metrics, clusterInfo, defaultConf(), 100)
	if err != nil {
		t.Fatalf("Detection failed: %v", err)
	}

	// node-a is exhausted, but node-b (40 vols) vs node-c (10 vols) is still
	// imbalanced. The algorithm should fall through and move from node-b.
	if len(tasks) == 0 {
		t.Fatal("Expected tasks from node-b after node-a was exhausted, got 0")
	}

	for i, task := range tasks {
		if task.Server == "node-a" {
			t.Errorf("Task %d: should not move FROM node-a (all volumes blocked)", i)
		}
	}

	// Verify node-b is the source
	hasNodeBSource := false
	for _, task := range tasks {
		if task.Server == "node-b" {
			hasNodeBSource = true
			break
		}
	}
	if !hasNodeBSource {
		t.Error("Expected node-b to be a source after node-a was exhausted")
	}

	assertNoDuplicateVolumes(t, tasks)
	t.Logf("Created %d tasks from node-b after node-a exhausted", len(tasks))
}

// TestDetection_HeterogeneousCapacity verifies that the balancer uses
// utilization ratio (volumes/maxVolumes) rather than raw volume counts.
// A server with more volumes but proportionally lower utilization should
// NOT be picked as the source over a server with fewer volumes but higher
// utilization.
func TestDetection_HeterogeneousCapacity(t *testing.T) {
	// Simulate a cluster like:
	//   server-1: 600 volumes, max 700  → utilization 85.7%
	//   server-2: 690 volumes, max 700  → utilization 98.6%  ← most utilized
	//   server-3: 695 volumes, max 700  → utilization 99.3%  ← most utilized
	//   server-4: 900 volumes, max 1260 → utilization 71.4%  ← least utilized
	//
	// The old algorithm (raw counts) would pick server-4 as source (900 > 695).
	// The correct behavior is to pick server-3 (or server-2) as source since
	// they have the highest utilization ratio.
	servers := []serverSpec{
		{id: "server-1", diskType: "hdd", dc: "dc1", rack: "rack1", maxVolumes: 700},
		{id: "server-2", diskType: "hdd", dc: "dc1", rack: "rack1", maxVolumes: 700},
		{id: "server-3", diskType: "hdd", dc: "dc1", rack: "rack1", maxVolumes: 700},
		{id: "server-4", diskType: "hdd", dc: "dc1", rack: "rack1", maxVolumes: 1260},
	}

	volCounts := map[string]int{
		"server-1": 600,
		"server-2": 690,
		"server-3": 695,
		"server-4": 900,
	}

	var metrics []*types.VolumeHealthMetrics
	vid := uint32(1)
	for _, server := range []string{"server-1", "server-2", "server-3", "server-4"} {
		count := volCounts[server]
		metrics = append(metrics, makeVolumes(server, "hdd", "dc1", "rack1", "", vid, count)...)
		vid += uint32(count)
	}

	at := buildTopology(servers, metrics)
	clusterInfo := &types.ClusterInfo{ActiveTopology: at}
	cfg := &Config{
		BaseConfig:         base.BaseConfig{Enabled: true},
		ImbalanceThreshold: 0.20,
		MinServerCount:     2,
	}

	tasks, _, err := Detection(metrics, clusterInfo, cfg, 5)
	if err != nil {
		t.Fatalf("Detection failed: %v", err)
	}
	if len(tasks) == 0 {
		t.Fatal("Expected balance tasks but got none")
	}

	// The source of the first task should be the most utilized server
	// (server-3 at 99.3% or server-2 at 98.6%), NOT server-4.
	firstSource := tasks[0].Server
	if firstSource == "server-4" {
		t.Errorf("Balancer incorrectly picked server-4 (lowest utilization 71.4%%) as source; should pick server-3 (99.3%%) or server-2 (98.6%%)")
	}
	if firstSource != "server-3" && firstSource != "server-2" {
		t.Errorf("Expected server-3 or server-2 as first source, got %s", firstSource)
	}
	t.Logf("First balance task: move from %s (correct: highest utilization)", firstSource)
}

// TestDetection_ZeroVolumeServerIncludedInBalance verifies that a server
// with zero volumes (seeded from topology with a matching disk type) is
// correctly included in the balance calculation and receives moves to
// equalize the distribution.
func TestDetection_ZeroVolumeServerIncludedInBalance(t *testing.T) {
	// 4 servers total, but only 3 have volumes.
	// node-d has a disk of the same type but zero volumes, so it appears in the
	// topology and is seeded into serverVolumeCounts with count=0.

	servers := []serverSpec{
		{id: "node-a", diskType: "hdd", diskID: 1, dc: "dc1", rack: "rack1", maxVolumes: 20},
		{id: "node-b", diskType: "hdd", diskID: 2, dc: "dc1", rack: "rack1", maxVolumes: 20},
		{id: "node-c", diskType: "hdd", diskID: 3, dc: "dc1", rack: "rack1", maxVolumes: 20},
		{id: "node-d", diskType: "hdd", diskID: 4, dc: "dc1", rack: "rack1", maxVolumes: 20},
	}

	// node-a: 8 volumes, node-b: 2, node-c: 1, node-d: 0
	var metrics []*types.VolumeHealthMetrics
	metrics = append(metrics, makeVolumes("node-a", "hdd", "dc1", "rack1", "", 1, 8)...)
	metrics = append(metrics, makeVolumes("node-b", "hdd", "dc1", "rack1", "", 20, 2)...)
	metrics = append(metrics, makeVolumes("node-c", "hdd", "dc1", "rack1", "", 30, 1)...)
	// node-d has 0 volumes — no metrics

	at := buildTopology(servers, metrics)
	clusterInfo := &types.ClusterInfo{ActiveTopology: at}

	tasks, _, err := Detection(metrics, clusterInfo, defaultConf(), 100)
	if err != nil {
		t.Fatalf("Detection failed: %v", err)
	}

	if len(tasks) == 0 {
		t.Fatal("Expected balance tasks for 8/2/1/0 distribution, got 0")
	}

	assertNoDuplicateVolumes(t, tasks)

	// With 11 volumes across 4 servers, the best achievable is 3/3/3/2
	// (imbalance=36.4%), which exceeds the 20% threshold. The algorithm should
	// stop when max-min<=1 rather than oscillating endlessly.
	effective := computeEffectiveCounts(servers, metrics, tasks)
	total := 0
	maxC, minC := 0, len(metrics)
	for _, c := range effective {
		total += c
		if c > maxC {
			maxC = c
		}
		if c < minC {
			minC = c
		}
	}

	// The diff between max and min should be at most 1 (as balanced as possible)
	if maxC-minC > 1 {
		t.Errorf("After %d moves, distribution not optimally balanced: effective=%v, max-min=%d (want ≤1)",
			len(tasks), effective, maxC-minC)
	}

	// Count destinations — moves should spread, not pile onto one server
	destCounts := make(map[string]int)
	for _, task := range tasks {
		if task.TypedParams != nil && len(task.TypedParams.Targets) > 0 {
			destCounts[task.TypedParams.Targets[0].Node]++
		}
	}

	// Moves should go to at least 2 different destinations
	if len(destCounts) < 2 {
		t.Errorf("Expected moves to spread across destinations, but got: %v", destCounts)
	}

	// Should need only ~5 moves for 8/2/1/0 → 3/3/3/2, not 8+ (oscillation)
	if len(tasks) > 8 {
		t.Errorf("Too many moves (%d) — likely oscillating; expected ≤8 for this distribution", len(tasks))
	}

	avg := float64(total) / float64(len(effective))
	imbalance := float64(maxC-minC) / avg
	t.Logf("Distribution 8/2/1/0 → %v after %d moves (imbalance=%.1f%%)",
		effective, len(tasks), imbalance*100)
}

func TestDetection_DataCenterFilter(t *testing.T) {
	servers := []serverSpec{
		{id: "node-a", diskType: "hdd", diskID: 1, dc: "dc1", rack: "rack1"},
		{id: "node-b", diskType: "hdd", diskID: 2, dc: "dc1", rack: "rack1"},
		{id: "node-c", diskType: "hdd", diskID: 3, dc: "dc2", rack: "rack1"},
	}

	var metrics []*types.VolumeHealthMetrics
	metrics = append(metrics, makeVolumes("node-a", "hdd", "dc1", "rack1", "c1", 1, 50)...)
	metrics = append(metrics, makeVolumes("node-b", "hdd", "dc1", "rack1", "c1", 100, 10)...)
	// node-c is in dc2, should be excluded by filter
	metrics = append(metrics, makeVolumes("node-c", "hdd", "dc2", "rack1", "c1", 200, 30)...)

	// Only include metrics from dc1
	dc1Metrics := make([]*types.VolumeHealthMetrics, 0)
	for _, m := range metrics {
		if m.DataCenter == "dc1" {
			dc1Metrics = append(dc1Metrics, m)
		}
	}

	at := buildTopology(servers, metrics)
	clusterInfo := &types.ClusterInfo{ActiveTopology: at}

	conf := defaultConf()
	conf.DataCenterFilter = "dc1"

	tasks, _, err := Detection(dc1Metrics, clusterInfo, conf, 100)
	if err != nil {
		t.Fatalf("Detection failed: %v", err)
	}

	// With DC filter, only node-a and node-b are considered in topology seeding.
	// node-c should never appear as source or destination.
	for _, task := range tasks {
		if task.Server == "node-c" {
			t.Errorf("node-c (dc2) should not be a source with dc1 filter")
		}
		if task.TypedParams != nil && len(task.TypedParams.Targets) > 0 {
			target := task.TypedParams.Targets[0].Node
			if strings.Contains(target, "node-c") {
				t.Errorf("node-c (dc2) should not be a target with dc1 filter")
			}
		}
	}

	if len(tasks) > 0 {
		t.Logf("Created %d tasks within dc1 scope", len(tasks))
	}
}

func TestDetection_NodeFilter(t *testing.T) {
	servers := []serverSpec{
		{id: "node-a", diskType: "hdd", diskID: 1, dc: "dc1", rack: "rack1"},
		{id: "node-b", diskType: "hdd", diskID: 2, dc: "dc1", rack: "rack1"},
		{id: "node-c", diskType: "hdd", diskID: 3, dc: "dc1", rack: "rack1"},
	}

	var metrics []*types.VolumeHealthMetrics
	metrics = append(metrics, makeVolumes("node-a", "hdd", "dc1", "rack1", "c1", 1, 50)...)
	metrics = append(metrics, makeVolumes("node-b", "hdd", "dc1", "rack1", "c1", 100, 10)...)
	metrics = append(metrics, makeVolumes("node-c", "hdd", "dc1", "rack1", "c1", 200, 5)...)

	// Only include metrics from node-a and node-b
	filteredMetrics := make([]*types.VolumeHealthMetrics, 0)
	for _, m := range metrics {
		if m.Server == "node-a" || m.Server == "node-b" {
			filteredMetrics = append(filteredMetrics, m)
		}
	}

	at := buildTopology(servers, metrics)
	clusterInfo := &types.ClusterInfo{ActiveTopology: at}

	conf := defaultConf()
	conf.NodeFilter = "node-a,node-b"

	tasks, _, err := Detection(filteredMetrics, clusterInfo, conf, 100)
	if err != nil {
		t.Fatalf("Detection failed: %v", err)
	}

	for _, task := range tasks {
		if task.Server == "node-c" {
			t.Errorf("node-c should not be a source with node filter")
		}
	}

	if len(tasks) > 0 {
		t.Logf("Created %d tasks within node-a,node-b scope", len(tasks))
	}
}
