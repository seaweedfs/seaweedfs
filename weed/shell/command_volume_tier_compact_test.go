package shell

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"github.com/stretchr/testify/assert"
)

func buildTestTopologyWithRemoteVolumes() *master_pb.TopologyInfo {
	return &master_pb.TopologyInfo{
		Id: "topo",
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				Id: "dc1",
				RackInfos: []*master_pb.RackInfo{
					{
						Id: "rack1",
						DataNodeInfos: []*master_pb.DataNodeInfo{
							{
								Id:       "server1:8080",
								GrpcPort: 18080,
								DiskInfos: map[string]*master_pb.DiskInfo{
									"": {
										VolumeInfos: []*master_pb.VolumeInformationMessage{
											{
												Id:                1,
												Collection:        "col1",
												RemoteStorageName: "s3.default",
												RemoteStorageKey:  "col1/1.dat",
												DeleteCount:       100,
												DeletedByteCount:  1000,
												Size:              5000,
											},
											{
												Id:                2,
												Collection:        "col1",
												RemoteStorageName: "",
												RemoteStorageKey:  "",
												Size:              3000,
											},
											{
												Id:                3,
												Collection:        "col2",
												RemoteStorageName: "s3.archive",
												RemoteStorageKey:  "col2/3.dat",
												DeleteCount:       50,
												DeletedByteCount:  500,
												Size:              2000,
											},
										},
									},
								},
							},
						},
					},
				},
			},
			{
				Id: "dc2",
				RackInfos: []*master_pb.RackInfo{
					{
						Id: "rack1",
						DataNodeInfos: []*master_pb.DataNodeInfo{
							{
								Id:       "server2:8080",
								GrpcPort: 18080,
								DiskInfos: map[string]*master_pb.DiskInfo{
									"": {
										VolumeInfos: []*master_pb.VolumeInformationMessage{
											{
												Id:                4,
												Collection:        "bucket1",
												RemoteStorageName: "s3.default",
												RemoteStorageKey:  "bucket1/4.dat",
												Size:              8000,
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
}

func TestFindRemoteVolumeInTopology(t *testing.T) {
	topo := buildTestTopologyWithRemoteVolumes()

	tests := []struct {
		name       string
		vid        needle.VolumeId
		collection string
		wantFound  bool
		wantDest   string
	}{
		{
			name:      "find existing remote volume",
			vid:       1,
			wantFound: true,
			wantDest:  "s3.default",
		},
		{
			name:      "find remote volume with different backend",
			vid:       3,
			wantFound: true,
			wantDest:  "s3.archive",
		},
		{
			name:      "find remote volume on another server",
			vid:       4,
			wantFound: true,
			wantDest:  "s3.default",
		},
		{
			name:      "local volume not found as remote",
			vid:       2,
			wantFound: false,
		},
		{
			name:      "non-existent volume not found",
			vid:       999,
			wantFound: false,
		},
		{
			name:       "filter by matching collection exact",
			vid:        1,
			collection: "^col1$",
			wantFound:  true,
			wantDest:   "s3.default",
		},
		{
			name:       "filter by matching collection regex",
			vid:        1,
			collection: "col.*",
			wantFound:  true,
			wantDest:   "s3.default",
		},
		{
			name:       "filter by non-matching collection",
			vid:        1,
			collection: "^col2$",
			wantFound:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rv, found, err := findRemoteVolumeInTopology(topo, tt.vid, tt.collection)
			assert.NoError(t, err)
			assert.Equal(t, tt.wantFound, found)
			if found {
				assert.Equal(t, tt.vid, rv.vid)
				assert.Equal(t, tt.wantDest, rv.remoteStorageName)
			}
		})
	}
}

func TestFindRemoteVolumeInTopologyInvalidPattern(t *testing.T) {
	topo := buildTestTopologyWithRemoteVolumes()

	_, _, err := findRemoteVolumeInTopology(topo, 1, "[invalid")
	assert.Error(t, err)
}

func TestCollectRemoteVolumesWithInfo(t *testing.T) {
	topo := buildTestTopologyWithRemoteVolumes()

	tests := []struct {
		name              string
		collectionPattern string
		wantCount         int
		wantVids          []needle.VolumeId
	}{
		{
			name:              "empty pattern matches empty collection only",
			collectionPattern: "",
			wantCount:         0,
		},
		{
			name:              "match all collections",
			collectionPattern: ".*",
			wantCount:         3, // volumes 1, 3, 4
			wantVids:          []needle.VolumeId{1, 3, 4},
		},
		{
			name:              "match specific collection",
			collectionPattern: "^col1$",
			wantCount:         1,
			wantVids:          []needle.VolumeId{1},
		},
		{
			name:              "match collection prefix",
			collectionPattern: "col.*",
			wantCount:         2, // volumes 1, 3
			wantVids:          []needle.VolumeId{1, 3},
		},
		{
			name:              "match bucket collection",
			collectionPattern: "bucket1",
			wantCount:         1,
			wantVids:          []needle.VolumeId{4},
		},
		{
			name:              "no match",
			collectionPattern: "^nonexistent$",
			wantCount:         0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := collectRemoteVolumesWithInfo(topo, tt.collectionPattern)
			assert.NoError(t, err)
			assert.Equal(t, tt.wantCount, len(result))

			if tt.wantVids != nil {
				gotVids := make(map[needle.VolumeId]bool)
				for _, rv := range result {
					gotVids[rv.vid] = true
				}
				for _, vid := range tt.wantVids {
					assert.True(t, gotVids[vid], "expected volume %d in results", vid)
				}
			}
		})
	}
}

func TestCollectRemoteVolumesWithInfoCaptures(t *testing.T) {
	topo := buildTestTopologyWithRemoteVolumes()

	result, err := collectRemoteVolumesWithInfo(topo, "^col1$")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))

	rv := result[0]
	assert.Equal(t, needle.VolumeId(1), rv.vid)
	assert.Equal(t, "col1", rv.collection)
	assert.Equal(t, "s3.default", rv.remoteStorageName)
	assert.Equal(t, "server1:8080", rv.serverUrl)
	assert.NotEmpty(t, rv.serverAddress)
}

func TestCollectRemoteVolumesWithInfoInvalidPattern(t *testing.T) {
	topo := buildTestTopologyWithRemoteVolumes()

	_, err := collectRemoteVolumesWithInfo(topo, "[invalid")
	assert.Error(t, err)
}

func buildTestTopologyWithReplicas() *master_pb.TopologyInfo {
	mkDn := func(id string, volumes ...*master_pb.VolumeInformationMessage) *master_pb.DataNodeInfo {
		return &master_pb.DataNodeInfo{
			Id:       id,
			GrpcPort: 18080,
			DiskInfos: map[string]*master_pb.DiskInfo{
				"": {VolumeInfos: volumes},
			},
		}
	}
	remote := func(vid uint32) *master_pb.VolumeInformationMessage {
		return &master_pb.VolumeInformationMessage{
			Id:                vid,
			Collection:        "col1",
			RemoteStorageName: "s3.default",
			RemoteStorageKey:  "col1/vid.dat",
		}
	}
	local := func(vid uint32) *master_pb.VolumeInformationMessage {
		return &master_pb.VolumeInformationMessage{Id: vid, Collection: "col1"}
	}
	return &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				RackInfos: []*master_pb.RackInfo{
					{
						DataNodeInfos: []*master_pb.DataNodeInfo{
							mkDn("n1:8080", remote(1)),
							mkDn("n2:8080", remote(1), local(2)),
							mkDn("n3:8080", remote(1)),
						},
					},
				},
			},
		},
	}
}

func TestCollectVolumeTierCompactLocations(t *testing.T) {
	topo := buildTestTopologyWithReplicas()

	tiered, local := collectVolumeTierCompactLocations(topo, needle.VolumeId(1), "col1")
	assert.Equal(t, 3, len(tiered))
	assert.Equal(t, 0, len(local))

	tiered, local = collectVolumeTierCompactLocations(topo, needle.VolumeId(2), "col1")
	assert.Equal(t, 0, len(tiered))
	assert.Equal(t, 1, len(local))

	// a different collection or volume id matches nothing
	tiered, local = collectVolumeTierCompactLocations(topo, needle.VolumeId(1), "other")
	assert.Equal(t, 0, len(tiered))
	assert.Equal(t, 0, len(local))
}

func TestPlanTierCompactResync(t *testing.T) {
	loc := func(url string) wdclient.Location {
		return wdclient.Location{Url: url, GrpcPort: 1}
	}
	urls := func(locs []wdclient.Location) (out []string) {
		for _, l := range locs {
			out = append(out, l.Url)
		}
		return
	}

	// single replica: nothing to resync, nothing to delete (its download
	// already removed the object)
	order, deleter := planTierCompactResync([]wdclient.Location{loc("a")}, nil)
	assert.Empty(t, order)
	assert.Empty(t, deleter)

	// two tiered replicas: the sibling is the last holder and goes last
	order, deleter = planTierCompactResync([]wdclient.Location{loc("a"), loc("b")}, nil)
	assert.Empty(t, order)
	assert.Equal(t, []string{"b"}, urls(deleter))

	// three tiered replicas plus one local copy: middle replicas resync in
	// order, the last tiered replica deletes and is resynced last
	order, deleter = planTierCompactResync(
		[]wdclient.Location{loc("a"), loc("b"), loc("c")},
		[]wdclient.Location{loc("d")})
	assert.Equal(t, []string{"b", "d"}, urls(order))
	assert.Equal(t, []string{"c"}, urls(deleter))
}
