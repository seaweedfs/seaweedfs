package pluginworker

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func makeTestVolumeListResponse(volumes ...*master_pb.VolumeInformationMessage) *master_pb.VolumeListResponse {
	return &master_pb.VolumeListResponse{
		VolumeSizeLimitMb: 30000,
		TopologyInfo: &master_pb.TopologyInfo{
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
										"hdd": {
											VolumeInfos: volumes,
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
}

func TestBuildVolumeMetricsEmptyFilter(t *testing.T) {
	resp := makeTestVolumeListResponse(
		&master_pb.VolumeInformationMessage{Id: 1, Collection: "photos", Size: 100},
		&master_pb.VolumeInformationMessage{Id: 2, Collection: "videos", Size: 200},
	)
	metrics, _, err := buildVolumeMetrics(resp, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(metrics) != 2 {
		t.Fatalf("expected 2 metrics, got %d", len(metrics))
	}
}

func TestBuildVolumeMetricsAllCollections(t *testing.T) {
	resp := makeTestVolumeListResponse(
		&master_pb.VolumeInformationMessage{Id: 1, Collection: "photos", Size: 100},
		&master_pb.VolumeInformationMessage{Id: 2, Collection: "videos", Size: 200},
	)
	metrics, _, err := buildVolumeMetrics(resp, "ALL_COLLECTIONS")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(metrics) != 2 {
		t.Fatalf("expected 2 metrics, got %d", len(metrics))
	}
}

func TestBuildVolumeMetricsEachCollection(t *testing.T) {
	resp := makeTestVolumeListResponse(
		&master_pb.VolumeInformationMessage{Id: 1, Collection: "photos", Size: 100},
		&master_pb.VolumeInformationMessage{Id: 2, Collection: "videos", Size: 200},
	)
	// EACH_COLLECTION passes all volumes through; filtering happens in the handler
	metrics, _, err := buildVolumeMetrics(resp, "EACH_COLLECTION")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(metrics) != 2 {
		t.Fatalf("expected 2 metrics, got %d", len(metrics))
	}
}

func TestBuildVolumeMetricsRegexFilter(t *testing.T) {
	resp := makeTestVolumeListResponse(
		&master_pb.VolumeInformationMessage{Id: 1, Collection: "photos", Size: 100},
		&master_pb.VolumeInformationMessage{Id: 2, Collection: "videos", Size: 200},
		&master_pb.VolumeInformationMessage{Id: 3, Collection: "photos-backup", Size: 300},
	)
	metrics, _, err := buildVolumeMetrics(resp, "^photos$")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(metrics) != 1 {
		t.Fatalf("expected 1 metric, got %d", len(metrics))
	}
	if metrics[0].Collection != "photos" {
		t.Fatalf("expected collection 'photos', got %q", metrics[0].Collection)
	}
}

func TestBuildVolumeMetricsInvalidRegex(t *testing.T) {
	resp := makeTestVolumeListResponse(
		&master_pb.VolumeInformationMessage{Id: 1, Collection: "photos", Size: 100},
	)
	_, _, err := buildVolumeMetrics(resp, "[invalid")
	if err == nil {
		t.Fatal("expected error for invalid regex")
	}
}
