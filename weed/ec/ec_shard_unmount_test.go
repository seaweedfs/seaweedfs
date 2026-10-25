package ec

import (
	"bytes"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/stretchr/testify/assert"
)

func TestEcShardsFromString(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    []*ShardRef
		wantErr bool
	}{
		{
			name: "single id",
			in:   "2",
			want: []*ShardRef{{ShardID: 2}},
		},
		{
			name: "list of ids",
			in:   "0, 3 ,5",
			want: []*ShardRef{{ShardID: 0}, {ShardID: 3}, {ShardID: 5}},
		},
		{
			name: "qualified keeps host:port",
			in:   "3@10.200.18.88:9007",
			want: []*ShardRef{{ShardID: 3, NodeAddress: "10.200.18.88:9007"}},
		},
		{
			name: "mixed bare and qualified",
			in:   "2,3@10.200.18.88:9007",
			want: []*ShardRef{{ShardID: 2}, {ShardID: 3, NodeAddress: "10.200.18.88:9007"}},
		},
		{
			name:    "colon without node marker is not a shard ID",
			in:      "3:10.200.18.88:9007",
			wantErr: true,
		},
		{
			// shard IDs beyond the default 10+4 total are valid on custom-ratio volumes.
			name: "shard id within MaxShardCount",
			in:   "20",
			want: []*ShardRef{{ShardID: 20}},
		},
		{
			name:    "shard id at MaxShardCount is rejected",
			in:      "32",
			wantErr: true,
		},
		{
			name:    "negative shard id",
			in:      "-1",
			wantErr: true,
		},
		{
			// must not wrap through uint32 into a valid-looking shard ID (4294967301 -> 5)
			name:    "out-of-range shard id does not wrap",
			in:      "4294967301",
			wantErr: true,
		},
		{
			name:    "non-numeric shard id",
			in:      "abc",
			wantErr: true,
		},
		{
			name:    "empty element",
			in:      "1,,2",
			wantErr: true,
		},
	}

	// guard the assumption the bound test relies on
	assert.Equal(t, uint32(32), uint32(erasure_coding.MaxShardCount))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ShardRefsFromString(tt.in)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// the printed topology form must parse back, so entries can be copied verbatim.
func TestEcShardStringRoundTrips(t *testing.T) {
	for _, s := range []*ShardRef{
		{ShardID: 2},
		{ShardID: 3, NodeAddress: "10.200.18.88:9007"},
	} {
		got, err := ShardRefsFromString(s.String())
		assert.NoError(t, err)
		assert.Equal(t, []*ShardRef{s}, got)
	}
}

func unmountTestTopology(nodes map[string]map[string][]uint32) *master_pb.TopologyInfo {
	var dataNodes []*master_pb.DataNodeInfo
	for addr, disks := range nodes {
		diskInfos := map[string]*master_pb.DiskInfo{}
		for disk, shardIds := range disks {
			si := erasure_coding.NewShardsInfo()
			for _, sid := range shardIds {
				si.Set(erasure_coding.NewShardInfo(erasure_coding.ShardId(sid), 1))
			}
			diskInfos[disk] = &master_pb.DiskInfo{
				Type:           "hdd",
				MaxVolumeCount: 100,
				EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{{
					Id:          100,
					Collection:  "c1",
					EcIndexBits: si.Bitmap(),
					ShardSizes:  si.SizesInt64(),
					DiskType:    "hdd",
				}},
			}
		}
		dataNodes = append(dataNodes, &master_pb.DataNodeInfo{
			Id:        addr,
			Address:   addr,
			DiskInfos: diskInfos,
		})
	}
	return &master_pb.TopologyInfo{DataCenterInfos: []*master_pb.DataCenterInfo{{
		Id:        "dc1",
		RackInfos: []*master_pb.RackInfo{{Id: "r1", DataNodeInfos: dataNodes}},
	}}}
}

// A shard held on two disks of one node is a single unmount target: the
// unmount RPC addresses the node, not the disk.
func TestUnmountResolvesShardDuplicatedOnOneNode(t *testing.T) {
	topo := unmountTestTopology(map[string]map[string][]uint32{
		"vs3:8080": {"hdd:0": {12}, "hdd:1": {12}},
		"vs4:8080": {"hdd:0": {13}},
	})

	for _, shards := range [][]*ShardRef{
		{{ShardID: 12}},
		{{ShardID: 12, NodeAddress: "vs3:8080"}},
	} {
		var out bytes.Buffer
		err := UnmountShards(&Env{}, &out, ShardUnmountRequest{
			Topology: topo,
			VolumeID: 100,
			Shards:   shards,
		})
		assert.NoError(t, err, "shard %+v", shards)
		assert.Contains(t, out.String(), "Will unmount 1 shard(s)")
	}
}

func TestUnmountStillRejectsShardOnTwoNodes(t *testing.T) {
	topo := unmountTestTopology(map[string]map[string][]uint32{
		"vs3:8080": {"hdd:0": {12}},
		"vs4:8080": {"hdd:0": {12}},
	})

	var out bytes.Buffer
	err := UnmountShards(&Env{}, &out, ShardUnmountRequest{
		Topology: topo,
		VolumeID: 100,
		Shards:   []*ShardRef{{ShardID: 12}},
	})
	assert.ErrorContains(t, err, "ambiguous")

	out.Reset()
	err = UnmountShards(&Env{}, &out, ShardUnmountRequest{
		Topology: topo,
		VolumeID: 100,
		Shards:   []*ShardRef{{ShardID: 12, NodeAddress: "vs3:8080"}},
	})
	assert.NoError(t, err)
}
