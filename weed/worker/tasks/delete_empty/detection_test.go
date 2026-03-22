package delete_empty

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

func defaultDetectionConfig() *Config {
	return &Config{
		BaseConfig: base.BaseConfig{
			Enabled:             true,
			ScanIntervalSeconds: 3600,
			MaxConcurrent:       1,
		},
		DeleteEmptyEnabled: true,
		QuietForSeconds:    3600, // 1 hour for tests
	}
}

// emptyVolume returns a VolumeHealthMetrics for an empty volume quiet for > 1 hour.
func emptyVolume(id uint32, server string) *types.VolumeHealthMetrics {
	return &types.VolumeHealthMetrics{
		VolumeID:      id,
		Server:        server,
		ServerAddress: server + ":8080",
		DiskType:      "hdd",
		Collection:    "",
		Size:          0,
		LastModified:  time.Now().Add(-2 * time.Hour),
	}
}

// buildTopologyWithVolumes creates a minimal ActiveTopology for the given metrics.
func buildTopologyWithVolumes(metrics []*types.VolumeHealthMetrics) *topology.ActiveTopology {
	at := topology.NewActiveTopology(0)

	volumesByServer := make(map[string][]*master_pb.VolumeInformationMessage)
	for _, m := range metrics {
		volumesByServer[m.Server] = append(volumesByServer[m.Server], &master_pb.VolumeInformationMessage{
			Id:   m.VolumeID,
			Size: m.Size,
		})
	}

	var nodes []*master_pb.DataNodeInfo
	seen := make(map[string]bool)
	for _, m := range metrics {
		if seen[m.Server] {
			continue
		}
		seen[m.Server] = true
		nodes = append(nodes, &master_pb.DataNodeInfo{
			Id:      m.Server,
			Address: m.Server + ":8080",
			DiskInfos: map[string]*master_pb.DiskInfo{
				"hdd": {
					Type:           "hdd",
					DiskId:         1,
					VolumeInfos:    volumesByServer[m.Server],
					MaxVolumeCount: 1000,
				},
			},
		})
	}

	at.UpdateTopology(&master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				Id: "dc1",
				RackInfos: []*master_pb.RackInfo{
					{Id: "rack1", DataNodeInfos: nodes},
				},
			},
		},
	})
	return at
}

func clusterInfo(metrics []*types.VolumeHealthMetrics) *types.ClusterInfo {
	return &types.ClusterInfo{
		ActiveTopology: buildTopologyWithVolumes(metrics),
	}
}

// --- Tests ---

func TestDetection_DetectsEmptyVolume(t *testing.T) {
	vol := emptyVolume(42, "server-1")
	metrics := []*types.VolumeHealthMetrics{vol}

	results, err := Detection(metrics, clusterInfo(metrics), defaultDetectionConfig())
	if err != nil {
		t.Fatalf("Detection error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 detection result, got %d", len(results))
	}
	if results[0].VolumeID != 42 {
		t.Errorf("Expected VolumeID=42, got %d", results[0].VolumeID)
	}
	if results[0].TaskType != types.TaskTypeCompaction {
		t.Errorf("Expected TaskType=%s, got %s", types.TaskTypeDeleteEmpty, results[0].TaskType)
	}
	if results[0].Priority != types.TaskPriorityLow {
		t.Errorf("Expected PriorityLow, got %v", results[0].Priority)
	}
	if results[0].TypedParams == nil {
		t.Fatal("TypedParams must not be nil")
	}
	if len(results[0].TypedParams.Sources) == 0 {
		t.Error("TypedParams.Sources must not be empty")
	}
	if results[0].TypedParams.Sources[0].Node != "server-1:8080" {
		t.Errorf("Expected source node server-1:8080, got %s", results[0].TypedParams.Sources[0].Node)
	}
}

func TestDetection_SkipsNonEmptyVolume(t *testing.T) {
	vol := &types.VolumeHealthMetrics{
		VolumeID:     10,
		Server:       "server-1",
		Size:         1024 * 1024,
		LastModified: time.Now().Add(-2 * time.Hour),
	}
	metrics := []*types.VolumeHealthMetrics{vol}

	results, err := Detection(metrics, clusterInfo(metrics), defaultDetectionConfig())
	if err != nil {
		t.Fatalf("Detection error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Expected 0 results for non-empty volume, got %d", len(results))
	}
}

func TestDetection_SkipsSuperBlockSizePlusOne(t *testing.T) {
	vol := &types.VolumeHealthMetrics{
		VolumeID:     11,
		Server:       "server-1",
		Size:         superBlockSize + 1,
		LastModified: time.Now().Add(-2 * time.Hour),
	}
	metrics := []*types.VolumeHealthMetrics{vol}

	results, err := Detection(metrics, clusterInfo(metrics), defaultDetectionConfig())
	if err != nil {
		t.Fatalf("Detection error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Expected 0 results for size=%d, got %d", superBlockSize+1, len(results))
	}
}

func TestDetection_SkipsRecentlyModifiedVolume(t *testing.T) {
	vol := &types.VolumeHealthMetrics{
		VolumeID:     20,
		Server:       "server-1",
		Size:         0,
		LastModified: time.Now().Add(-10 * time.Minute), // too recent
	}
	metrics := []*types.VolumeHealthMetrics{vol}

	results, err := Detection(metrics, clusterInfo(metrics), defaultDetectionConfig())
	if err != nil {
		t.Fatalf("Detection error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Expected 0 results for recently modified volume, got %d", len(results))
	}
}

func TestDetection_SkipsZeroLastModified(t *testing.T) {
	vol := &types.VolumeHealthMetrics{
		VolumeID: 30,
		Server:   "server-1",
		Size:     0,
		// LastModified is zero value
	}
	metrics := []*types.VolumeHealthMetrics{vol}

	results, err := Detection(metrics, clusterInfo(metrics), defaultDetectionConfig())
	if err != nil {
		t.Fatalf("Detection error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Expected 0 results for zero LastModified, got %d", len(results))
	}
}

func TestDetection_DeleteEmptyDisabledReturnsNil(t *testing.T) {
	cfg := defaultDetectionConfig()
	cfg.DeleteEmptyEnabled = false

	vol := emptyVolume(55, "server-1")
	metrics := []*types.VolumeHealthMetrics{vol}

	results, err := Detection(metrics, clusterInfo(metrics), cfg)
	if err != nil {
		t.Fatalf("Detection error: %v", err)
	}
	if results != nil {
		t.Errorf("Expected nil results when DeleteEmptyEnabled=false, got %v", results)
	}
}

func TestDetection_DisabledConfigReturnsNil(t *testing.T) {
	cfg := defaultDetectionConfig()
	cfg.Enabled = false

	vol := emptyVolume(50, "server-1")
	metrics := []*types.VolumeHealthMetrics{vol}

	results, err := Detection(metrics, clusterInfo(metrics), cfg)
	if err != nil {
		t.Fatalf("Detection error: %v", err)
	}
	if results != nil {
		t.Errorf("Expected nil results for disabled config, got %v", results)
	}
}

func TestDetection_MultipeEmptyVolumes(t *testing.T) {
	metrics := []*types.VolumeHealthMetrics{
		emptyVolume(100, "server-1"),
		emptyVolume(101, "server-1"),
		emptyVolume(102, "server-2"),
		{
			VolumeID:     103,
			Server:       "server-2",
			Size:         512 * 1024, // not empty
			LastModified: time.Now().Add(-2 * time.Hour),
		},
	}

	results, err := Detection(metrics, clusterInfo(metrics), defaultDetectionConfig())
	if err != nil {
		t.Fatalf("Detection error: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("Expected 3 results (3 empty, 1 skipped), got %d", len(results))
	}

	// Verify no duplicates
	seen := make(map[uint32]bool)
	for _, r := range results {
		if seen[r.VolumeID] {
			t.Errorf("Duplicate VolumeID %d in results", r.VolumeID)
		}
		seen[r.VolumeID] = true
	}
}

func TestDetection_SkipsVolumeWithExistingTask(t *testing.T) {
	metrics := []*types.VolumeHealthMetrics{
		emptyVolume(200, "server-1"),
		emptyVolume(201, "server-1"),
	}

	at := buildTopologyWithVolumes(metrics)
	err := at.AddPendingTask(topology.TaskSpec{
		TaskID:       "existing-200",
		TaskType:     topology.TaskTypeBalance,
		VolumeID:     200,
		VolumeSize:   0,
		Sources:      []topology.TaskSourceSpec{{ServerID: "server-1", DiskID: 1}},
		Destinations: []topology.TaskDestinationSpec{{ServerID: "server-1", DiskID: 2}},
	})
	if err != nil {
		t.Fatalf("AddPendingTask failed: %v", err)
	}

	ci := &types.ClusterInfo{ActiveTopology: at}
	results, err := Detection(metrics, ci, defaultDetectionConfig())
	if err != nil {
		t.Fatalf("Detection error: %v", err)
	}

	// Volume 200 has pending task → skipped; 201 should still be detected
	if len(results) != 1 {
		t.Fatalf("Expected 1 result (vol 201 only), got %d", len(results))
	}
	if results[0].VolumeID != 201 {
		t.Errorf("Expected VolumeID=201, got %d", results[0].VolumeID)
	}
}

func TestDetection_NilClusterInfo(t *testing.T) {
	vol := emptyVolume(300, "server-1")
	metrics := []*types.VolumeHealthMetrics{vol}

	// Should not panic with nil clusterInfo
	results, err := Detection(metrics, nil, defaultDetectionConfig())
	if err != nil {
		t.Fatalf("Detection error: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 result with nil clusterInfo, got %d", len(results))
	}
}

func TestDetection_EmptyMetricsSlice(t *testing.T) {
	results, err := Detection(nil, nil, defaultDetectionConfig())
	if err != nil {
		t.Fatalf("Detection error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Expected 0 results for empty metrics, got %d", len(results))
	}
}

func TestDetection_QuietPeriodBoundary(t *testing.T) {
	cfg := defaultDetectionConfig()
	cfg.QuietForSeconds = 3600 // 1 hour

	// Just before the quiet period – should NOT be detected (Age < quietFor)
	tooRecent := &types.VolumeHealthMetrics{
		VolumeID:     400,
		Server:       "server-1",
		Size:         0,
		LastModified: time.Now().Add(-time.Hour + time.Second),
	}
	// Exactly at and past boundary – should be detected (Age >= quietFor)
	atBoundary := &types.VolumeHealthMetrics{
		VolumeID:     401,
		Server:       "server-1",
		Size:         0,
		LastModified: time.Now().Add(-time.Hour),
	}
	pastLimit := &types.VolumeHealthMetrics{
		VolumeID:     402,
		Server:       "server-1",
		Size:         0,
		LastModified: time.Now().Add(-time.Hour - time.Second),
	}

	metrics := []*types.VolumeHealthMetrics{tooRecent, atBoundary, pastLimit}
	results, err := Detection(metrics, clusterInfo(metrics), cfg)
	if err != nil {
		t.Fatalf("Detection error: %v", err)
	}

	seen := make(map[uint32]bool)
	for _, r := range results {
		seen[r.VolumeID] = true
	}

	if seen[400] {
		t.Errorf("Volume 400 (too recent) should not be detected")
	}
	if !seen[401] {
		t.Errorf("Volume 401 (exactly at boundary) should be detected")
	}
	if !seen[402] {
		t.Errorf("Volume 402 (past quiet period) should be detected")
	}
}

func TestDetection_TaskIDUnique(t *testing.T) {
	metrics := []*types.VolumeHealthMetrics{
		emptyVolume(500, "server-1"),
		emptyVolume(501, "server-1"),
	}

	results, err := Detection(metrics, clusterInfo(metrics), defaultDetectionConfig())
	if err != nil {
		t.Fatalf("Detection error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}

	if results[0].TaskID == results[1].TaskID {
		t.Errorf("Task IDs should be unique: both are %q", results[0].TaskID)
	}
}
