package shell

import (
	"io"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func TestCollectVolumeMarkTargetsByCollection(t *testing.T) {
	topo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				RackInfos: []*master_pb.RackInfo{
					{
						DataNodeInfos: []*master_pb.DataNodeInfo{
							{
								Id:       "node-a:8080",
								Address:  "10.0.0.1:8080",
								GrpcPort: 19080,
								DiskInfos: map[string]*master_pb.DiskInfo{
									"hdd": {
										VolumeInfos: []*master_pb.VolumeInformationMessage{
											{Id: 1, Collection: "photos"},
											{Id: 2, Collection: "docs"},
										},
									},
									"ssd": {
										VolumeInfos: []*master_pb.VolumeInformationMessage{
											{Id: 3, Collection: "photos"},
										},
									},
								},
							},
							{
								Id: "node-b:8080",
								DiskInfos: map[string]*master_pb.DiskInfo{
									"hdd": {
										VolumeInfos: []*master_pb.VolumeInformationMessage{
											{Id: 1, Collection: "photos"},
										},
										EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
											{Id: 4, Collection: "photos"},
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

	targets, err := collectVolumeMarkTargetsByCollection(topo, "photos")
	if err != nil {
		t.Fatalf("collectVolumeMarkTargetsByCollection: %v", err)
	}

	got := map[string]int{}
	for _, target := range targets {
		got[string(target.sourceVolumeServer)+"#"+target.volumeId.String()]++
	}
	want := map[string]int{
		string(pb.ServerAddress("10.0.0.1:8080.19080")) + "#" + needle.VolumeId(1).String(): 1,
		string(pb.ServerAddress("10.0.0.1:8080.19080")) + "#" + needle.VolumeId(3).String(): 1,
		string(pb.ServerAddress("node-b:8080")) + "#" + needle.VolumeId(1).String():         1,
	}
	if len(got) != len(want) {
		t.Fatalf("got %v targets, want %v", got, want)
	}
	for key, wantCount := range want {
		if got[key] != wantCount {
			t.Fatalf("target %s count %d, want %d; all targets: %v", key, got[key], wantCount, got)
		}
	}
}

func TestCollectVolumeMarkTargetsByCollectionRejectsEmptyCollection(t *testing.T) {
	_, err := collectVolumeMarkTargetsByCollection(&master_pb.TopologyInfo{}, "")
	if err == nil || !strings.Contains(err.Error(), "collection is required") {
		t.Fatalf("expected collection required error, got %v", err)
	}
}

func TestCollectVolumeMarkTargetsByCollectionReportsMissingCollection(t *testing.T) {
	_, err := collectVolumeMarkTargetsByCollection(&master_pb.TopologyInfo{}, "missing")
	if err == nil || !strings.Contains(err.Error(), "collection missing has no volumes") {
		t.Fatalf("expected missing collection error, got %v", err)
	}
}

func TestVolumeMarkRejectsCollectionWithNodeOrVolumeId(t *testing.T) {
	err := (&commandVolumeMark{}).Do([]string{
		"-collection", "photos",
		"-node", "127.0.0.1:8080",
		"-readonly",
	}, nil, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "cannot use -collection with -node or -volumeId") {
		t.Fatalf("expected collection conflict error, got %v", err)
	}

	err = (&commandVolumeMark{}).Do([]string{
		"-collection", "photos",
		"-volumeId", "1",
		"-readonly",
	}, nil, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "cannot use -collection with -node or -volumeId") {
		t.Fatalf("expected collection conflict error, got %v", err)
	}

	err = (&commandVolumeMark{}).Do([]string{
		"-collection", "photos",
		"-volumeId", "0",
		"-readonly",
	}, nil, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "cannot use -collection with -node or -volumeId") {
		t.Fatalf("expected collection conflict error, got %v", err)
	}
}

func TestVolumeMarkRejectsEmptyCollectionFlag(t *testing.T) {
	err := (&commandVolumeMark{}).Do([]string{
		"-collection", "",
		"-readonly",
	}, nil, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "collection is required") {
		t.Fatalf("expected collection required error, got %v", err)
	}
}
