package ec

import (
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
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
