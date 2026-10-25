package erasure_coding

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/stretchr/testify/require"
)

type ecShardSet struct {
	bits       uint32
	encodeTsNs int64
}

func ecBits(shardIds ...int) uint32 {
	var b uint32
	for _, id := range shardIds {
		b |= uint32(1) << uint(id)
	}
	return b
}

// buildEcTopology builds an ActiveTopology where each shard set lives on its own
// node, so countExistingEcShardsForVolume can be exercised across encode
// generations spread over different disks.
func buildEcTopology(t *testing.T, vid uint32, collection string, sets []ecShardSet) *topology.ActiveTopology {
	t.Helper()
	at := topology.NewActiveTopology(10)
	nodes := make([]*master_pb.DataNodeInfo, 0, len(sets))
	for i, set := range sets {
		nodes = append(nodes, &master_pb.DataNodeInfo{
			Id: fmt.Sprintf("10.0.0.%d:8080", i+1),
			DiskInfos: map[string]*master_pb.DiskInfo{
				"hdd": {
					DiskId:         0,
					MaxVolumeCount: 100,
					EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
						{Id: vid, Collection: collection, EcIndexBits: set.bits, EncodeTsNs: set.encodeTsNs},
					},
				},
			},
		})
	}
	topologyInfo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id:        "dc1",
			RackInfos: []*master_pb.RackInfo{{Id: "rack1", DataNodeInfos: nodes}},
		}},
	}
	require.NoError(t, at.UpdateTopology(topologyInfo))
	return at
}

// Two interrupted encode runs whose shard sets overlap in count must NOT be
// unioned into a false-complete set. countExistingEcShardsForVolume returns the
// largest single generation's shard count, not the cross-run union.
func TestCountExistingEcShards_DoesNotUnionAcrossGenerations(t *testing.T) {
	const vid = uint32(42)

	// run A holds shards 0-6 (7 shards, encode_ts 1); run B holds shards 7-13
	// (7 shards, encode_ts 2). The union would be all 14; the per-generation max
	// is 7, so the orphan-source delete trigger (>= 14) must not fire.
	at := buildEcTopology(t, vid, "", []ecShardSet{
		{bits: ecBits(0, 1, 2, 3, 4, 5, 6), encodeTsNs: 1},
		{bits: ecBits(7, 8, 9, 10, 11, 12, 13), encodeTsNs: 2},
	})
	require.Equal(t, 7, countExistingEcShardsForVolume(at, vid, ""))
}

// A single complete generation still counts as complete, so genuine stuck-source
// cleanup keeps working.
func TestCountExistingEcShards_SingleCompleteGeneration(t *testing.T) {
	const vid = uint32(43)
	at := buildEcTopology(t, vid, "", []ecShardSet{
		{bits: ecBits(0, 1, 2, 3, 4, 5, 6), encodeTsNs: 5},
		{bits: ecBits(7, 8, 9, 10, 11, 12, 13), encodeTsNs: 5},
	})
	require.Equal(t, 14, countExistingEcShardsForVolume(at, vid, ""))
}

// Shards from pre-upgrade servers report encode_ts_ns==0; a complete 0-generation
// set is still recognized as complete (its own bucket).
func TestCountExistingEcShards_PreUpgradeZeroGeneration(t *testing.T) {
	const vid = uint32(44)
	at := buildEcTopology(t, vid, "", []ecShardSet{
		{bits: ecBits(0, 1, 2, 3, 4, 5, 6), encodeTsNs: 0},
		{bits: ecBits(7, 8, 9, 10, 11, 12, 13), encodeTsNs: 0},
	})
	require.Equal(t, 14, countExistingEcShardsForVolume(at, vid, ""))
}
