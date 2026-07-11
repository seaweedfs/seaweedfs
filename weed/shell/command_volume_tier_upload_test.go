package shell

import (
	"io"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func tierUploadTopo(nodes map[string]*master_pb.VolumeInformationMessage) *master_pb.TopologyInfo {
	var dns []*master_pb.DataNodeInfo
	for _, id := range []string{"n1", "n2", "n3"} {
		vi, found := nodes[id]
		if !found {
			continue
		}
		dns = append(dns, &master_pb.DataNodeInfo{
			Id:        id,
			DiskInfos: map[string]*master_pb.DiskInfo{"": {VolumeInfos: []*master_pb.VolumeInformationMessage{vi}}},
		})
	}
	return &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			RackInfos: []*master_pb.RackInfo{{DataNodeInfos: dns}},
		}},
	}
}

// A replica that is already tiered must become the upload source, so a rerun
// after a partial failure reuses its remote object instead of uploading a
// second copy under a new key.
func TestCollectVolumeTierUploadLocationsPrefersTieredReplica(t *testing.T) {
	topo := tierUploadTopo(map[string]*master_pb.VolumeInformationMessage{
		"n1": {Id: 1, Collection: "c"},
		"n2": {Id: 1, Collection: "c", RemoteStorageName: "s3.default", RemoteStorageKey: "some-key"},
		"n3": {Id: 1, Collection: "c"},
	})
	locations := collectVolumeTierUploadLocations(topo, needle.VolumeId(1), "c", io.Discard)
	if len(locations) != 3 {
		t.Fatalf("want 3 locations, got %d", len(locations))
	}
	if locations[0].Url != "n2" {
		t.Fatalf("want tiered replica n2 first, got %s", locations[0].Url)
	}
}

func TestCollectVolumeTierUploadLocationsFiltersCollection(t *testing.T) {
	topo := tierUploadTopo(map[string]*master_pb.VolumeInformationMessage{
		"n1": {Id: 1, Collection: "c"},
		"n2": {Id: 1, Collection: "other"},
	})
	if locations := collectVolumeTierUploadLocations(topo, needle.VolumeId(1), "c", io.Discard); len(locations) != 1 || locations[0].Url != "n1" {
		t.Fatalf("want only n1, got %+v", locations)
	}
}
