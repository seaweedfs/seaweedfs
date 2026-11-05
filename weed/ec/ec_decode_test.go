package ec

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// shardsOn builds one holder's inventory.
func shardsOn(ids ...int) *erasure_coding.ShardsInfo {
	si := erasure_coding.NewShardsInfo()
	for _, id := range ids {
		si.Set(erasure_coding.NewShardInfo(erasure_coding.ShardId(id), 1024))
	}
	return si
}

// missingDataShard decides whether a decode may delete the shards of a volume
// that already exists elsewhere. Only the deletion phase of a decode removes a
// data shard, so the answer separates "an earlier decode was interrupted while
// cleaning up" from the two states that look the same from the outside: an
// encode interrupted before it deleted the original, and a decode killed while
// generating. Both of those leave every shard in place, and the volume beside
// them may be the untouched original or a half-written one -- neither is safe
// to trade for the shards.
func TestMissingDataShard(t *testing.T) {
	const dataShards = 10

	tests := []struct {
		name       string
		holders    map[pb.ServerAddress]*erasure_coding.ShardsInfo
		wantID     erasure_coding.ShardId
		wantMissed bool
	}{
		{
			name: "complete set on one holder decodes, so nothing is finished",
			holders: map[pb.ServerAddress]*erasure_coding.ShardsInfo{
				"server1:8080": shardsOn(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13),
			},
		},
		{
			name: "complete set spread across holders is still complete",
			holders: map[pb.ServerAddress]*erasure_coding.ShardsInfo{
				"server1:8080": shardsOn(0, 1, 2, 3, 4),
				"server2:8080": shardsOn(5, 6, 7, 8, 9),
				"server3:8080": shardsOn(10, 11, 12, 13),
			},
		},
		{
			name: "parity gone is not the deletion phase: the volume still decodes",
			holders: map[pb.ServerAddress]*erasure_coding.ShardsInfo{
				"server1:8080": shardsOn(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
			},
		},
		{
			name: "a data shard gone is a decode that was interrupted mid-cleanup",
			holders: map[pb.ServerAddress]*erasure_coding.ShardsInfo{
				"server1:8080": shardsOn(0, 1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13),
			},
			wantID:     6,
			wantMissed: true,
		},
		{
			name: "the first gap is reported, not the last",
			holders: map[pb.ServerAddress]*erasure_coding.ShardsInfo{
				"server1:8080": shardsOn(0, 3, 4, 5, 6, 7, 8, 9),
			},
			wantID:     1,
			wantMissed: true,
		},
		{
			name:       "no holders at all leaves every data shard missing",
			holders:    map[pb.ServerAddress]*erasure_coding.ShardsInfo{},
			wantID:     0,
			wantMissed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, missed := missingDataShard(tt.holders, dataShards)
			if missed != tt.wantMissed {
				t.Fatalf("missingDataShard = %v, want %v", missed, tt.wantMissed)
			}
			if missed && id != tt.wantID {
				t.Errorf("missing shard id = %d, want %d", id, tt.wantID)
			}
		})
	}
}

func TestFindEcVolumeCollection(t *testing.T) {
	topo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				Id: "dc1",
				RackInfos: []*master_pb.RackInfo{
					{
						Id: "rack1",
						DataNodeInfos: []*master_pb.DataNodeInfo{
							{
								Id: "node1",
								DiskInfos: map[string]*master_pb.DiskInfo{
									"hdd": {
										EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
											{
												Id:         10,
												Collection: "s3bucket",
											},
											{
												Id:         30,
												Collection: "",
											},
										},
										VolumeInfos: []*master_pb.VolumeInformationMessage{
											{
												Id:         20,
												Collection: "regular_col",
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

	tests := []struct {
		name      string
		topo      *master_pb.TopologyInfo
		vid       needle.VolumeId
		wantCol   string
		wantFound bool
	}{
		{
			name:      "nil topology",
			topo:      nil,
			vid:       10,
			wantCol:   "",
			wantFound: false,
		},
		{
			name:      "ec volume with named collection",
			topo:      topo,
			vid:       10,
			wantCol:   "s3bucket",
			wantFound: true,
		},
		{
			name:      "ec volume with empty collection",
			topo:      topo,
			vid:       30,
			wantCol:   "",
			wantFound: true,
		},
		{
			name:      "regular volume record is not an ec volume",
			topo:      topo,
			vid:       20,
			wantCol:   "",
			wantFound: false,
		},
		{
			name:      "not found",
			topo:      topo,
			vid:       999,
			wantCol:   "",
			wantFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col, found := FindEcVolumeCollection(tt.topo, tt.vid)
			if found != tt.wantFound {
				t.Fatalf("FindEcVolumeCollection() found = %v, want %v", found, tt.wantFound)
			}
			if col != tt.wantCol {
				t.Errorf("FindEcVolumeCollection() col = %q, want %q", col, tt.wantCol)
			}
		})
	}
}
