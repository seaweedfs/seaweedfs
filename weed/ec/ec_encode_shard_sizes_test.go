package ec

import (
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// sizedShards builds one holder's inventory as {shard id: size}.
func sizedShards(idToSize map[int]int64) *erasure_coding.ShardsInfo {
	si := erasure_coding.NewShardsInfo()
	for id, size := range idToSize {
		si.Set(erasure_coding.NewShardInfo(erasure_coding.ShardId(id), erasure_coding.ShardSize(size)))
	}
	return si
}

// An encode deletes the volume the shards were made from, on the strength of
// having counted them. Every shard takes one piece of each block row, so they
// are written to the same length: one that disagrees was truncated or half
// copied, and counting cannot see it.
func TestRequireUniformShardSizes(t *testing.T) {
	tests := []struct {
		name        string
		byNode      map[pb.ServerAddress]*erasure_coding.ShardsInfo
		wantErr     bool
		errContains []string
	}{
		{
			name: "one holder, all shards the same length",
			byNode: map[pb.ServerAddress]*erasure_coding.ShardsInfo{
				"server1:8080": sizedShards(map[int]int64{0: 1048576, 1: 1048576, 2: 1048576}),
			},
		},
		{
			name: "spread across holders, still one length",
			byNode: map[pb.ServerAddress]*erasure_coding.ShardsInfo{
				"server1:8080": sizedShards(map[int]int64{0: 1048576, 1: 1048576}),
				"server2:8080": sizedShards(map[int]int64{2: 1048576, 3: 1048576}),
			},
		},
		{
			name: "a truncated shard is named with its holder",
			byNode: map[pb.ServerAddress]*erasure_coding.ShardsInfo{
				"server1:8080": sizedShards(map[int]int64{0: 1048576, 1: 1048576}),
				"server2:8080": sizedShards(map[int]int64{2: 4096}),
			},
			wantErr:     true,
			errContains: []string{"disagree on size", "server2:8080.2", "4096"},
		},
		{
			name: "two copies of one shard that disagree are still a disagreement",
			byNode: map[pb.ServerAddress]*erasure_coding.ShardsInfo{
				"server1:8080": sizedShards(map[int]int64{5: 1048576}),
				"server2:8080": sizedShards(map[int]int64{5: 1048570}),
			},
			wantErr:     true,
			errContains: []string{"disagree on size"},
		},

		// Sizes the cluster does not report must never block an encode: an
		// older volume server, or one that has not heartbeated its sizes yet,
		// reports zero, and refusing to delete on that would strand every
		// encode against such a cluster in the hybrid state this check exists
		// to avoid.
		{
			name: "unreported sizes are skipped, not read as a disagreement",
			byNode: map[pb.ServerAddress]*erasure_coding.ShardsInfo{
				"server1:8080": sizedShards(map[int]int64{0: 1048576, 1: 0}),
				"server2:8080": sizedShards(map[int]int64{2: 0}),
			},
		},
		{
			name: "nothing reported at all verifies vacuously",
			byNode: map[pb.ServerAddress]*erasure_coding.ShardsInfo{
				"server1:8080": sizedShards(map[int]int64{0: 0, 1: 0}),
			},
		},
		{
			name:   "no holders is not a size problem",
			byNode: map[pb.ServerAddress]*erasure_coding.ShardsInfo{},
		},
		{
			name: "a nil inventory is skipped rather than panicking",
			byNode: map[pb.ServerAddress]*erasure_coding.ShardsInfo{
				"server1:8080": nil,
				"server2:8080": sizedShards(map[int]int64{0: 1048576}),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := requireUniformShardSizes(needle.VolumeId(7), tt.byNode)
			if !tt.wantErr {
				if err != nil {
					t.Fatalf("want the encode to proceed, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("want the deletion held back, got no error")
			}
			for _, want := range tt.errContains {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not mention %q", err, want)
				}
			}
		})
	}
}

// ecShardReport builds one heartbeat entry: a generation stamp and the shards
// that generation put on this node, all of one size.
func ecShardReport(vid uint32, encodeTsNs int64, size int64, ids ...int) *master_pb.VolumeEcShardInformationMessage {
	si := erasure_coding.NewShardsInfo()
	for _, id := range ids {
		si.Set(erasure_coding.NewShardInfo(erasure_coding.ShardId(id), erasure_coding.ShardSize(size)))
	}
	return &master_pb.VolumeEcShardInformationMessage{
		Id:          vid,
		Collection:  "ectest",
		EcIndexBits: si.Bitmap(),
		ShardSizes:  si.SizesInt64(),
		EncodeTsNs:  encodeTsNs,
	}
}

func topologyWith(reports map[string][]*master_pb.VolumeEcShardInformationMessage) *master_pb.TopologyInfo {
	var nodes []*master_pb.DataNodeInfo
	for addr, shards := range reports {
		nodes = append(nodes, &master_pb.DataNodeInfo{
			Id: addr,
			DiskInfos: map[string]*master_pb.DiskInfo{
				"hdd": {EcShardInfos: shards},
			},
		})
	}
	return &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id:        "dc1",
			RackInfos: []*master_pb.RackInfo{{Id: "rack1", DataNodeInfos: nodes}},
		}},
	}
}

// A re-encode can change the ratio, so an older generation's shards are a
// different length by nature. Recoverability already judges only the newest
// generation; if the size check did not, an orphaned older encode -- one the
// pre-encode sweep could not reach, but the master still hears about -- would
// disagree on size at every retry and strand the volume beside its shards for
// good.
func TestShardSizeCheckIgnoresOlderEncodeGenerations(t *testing.T) {
	const vid = 9

	topo := topologyWith(map[string][]*master_pb.VolumeEcShardInformationMessage{
		// the current encode: one length, spread over two nodes
		"server1:8080": {ecShardReport(vid, 2000, 1048576, 0, 1, 2)},
		"server2:8080": {ecShardReport(vid, 2000, 1048576, 3, 4, 5)},
		// an orphan from an earlier encode, at a different length
		"server3:8080": {ecShardReport(vid, 1000, 524288, 0, 1)},
	})

	byNode := collectNewestGenerationShardsInfo(topo, needle.VolumeId(vid))
	if _, stale := byNode[pb.ServerAddress("server3:8080")]; stale {
		t.Error("the older generation's holder must not be collected")
	}
	if err := requireUniformShardSizes(needle.VolumeId(vid), byNode); err != nil {
		t.Fatalf("a healthy current generation must not be vetoed by an orphan: %v", err)
	}
}

// Fencing to the newest generation must not blind the check: a short shard
// inside that generation is exactly what it exists to catch.
func TestShardSizeCheckStillCatchesShortShardInNewestGeneration(t *testing.T) {
	const vid = 9

	topo := topologyWith(map[string][]*master_pb.VolumeEcShardInformationMessage{
		"server1:8080": {ecShardReport(vid, 2000, 1048576, 0, 1)},
		"server2:8080": {ecShardReport(vid, 2000, 4096, 2)},
		"server3:8080": {ecShardReport(vid, 1000, 524288, 0)},
	})

	err := requireUniformShardSizes(needle.VolumeId(vid), collectNewestGenerationShardsInfo(topo, needle.VolumeId(vid)))
	if err == nil {
		t.Fatal("a truncated shard in the newest generation must hold the deletion back")
	}
	if !strings.Contains(err.Error(), "server2:8080.2") {
		t.Errorf("error %q does not name the short shard", err)
	}
}

// Volumes encoded before generation stamping report zero, which is a single
// legacy generation rather than an orphan to drop.
func TestShardSizeCheckHandlesUnstampedGenerations(t *testing.T) {
	const vid = 9

	topo := topologyWith(map[string][]*master_pb.VolumeEcShardInformationMessage{
		"server1:8080": {ecShardReport(vid, 0, 1048576, 0, 1)},
		"server2:8080": {ecShardReport(vid, 0, 4096, 2)},
	})

	byNode := collectNewestGenerationShardsInfo(topo, needle.VolumeId(vid))
	if len(byNode) != 2 {
		t.Fatalf("unstamped shards form one generation, got %d holder(s)", len(byNode))
	}
	if err := requireUniformShardSizes(needle.VolumeId(vid), byNode); err == nil {
		t.Error("a truncated legacy shard must still hold the deletion back")
	}
}
