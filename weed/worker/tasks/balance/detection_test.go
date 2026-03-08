package balance

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

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

	tasks, err := Detection(metrics, clusterInfo, conf, 100)
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

	tasks, err := Detection(metrics, clusterInfo, conf, 100)
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

	// Request only 3 results
	tasks, err := Detection(metrics, clusterInfo, conf, 3)
	if err != nil {
		t.Fatalf("Detection failed: %v", err)
	}

	if len(tasks) != 3 {
		t.Errorf("Expected exactly 3 tasks (maxResults=3), got %d", len(tasks))
	}
}
