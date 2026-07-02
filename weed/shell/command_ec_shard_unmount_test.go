package shell

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/stretchr/testify/assert"
)

func TestEcShardsFromString(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    []*ecShard
		wantErr bool
	}{
		{
			name: "single id",
			in:   "2",
			want: []*ecShard{{ShardID: 2}},
		},
		{
			name: "list of ids",
			in:   "0, 3 ,5",
			want: []*ecShard{{ShardID: 0}, {ShardID: 3}, {ShardID: 5}},
		},
		{
			name: "qualified keeps host:port",
			in:   "3@10.200.18.88:9007",
			want: []*ecShard{{ShardID: 3, NodeAddress: "10.200.18.88:9007"}},
		},
		{
			name: "mixed bare and qualified",
			in:   "2,3@10.200.18.88:9007",
			want: []*ecShard{{ShardID: 2}, {ShardID: 3, NodeAddress: "10.200.18.88:9007"}},
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
			want: []*ecShard{{ShardID: 20}},
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
			got, err := ecShardsFromString(tt.in)
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
	for _, s := range []*ecShard{
		{ShardID: 2},
		{ShardID: 3, NodeAddress: "10.200.18.88:9007"},
	} {
		got, err := ecShardsFromString(s.String())
		assert.NoError(t, err)
		assert.Equal(t, []*ecShard{s}, got)
	}
}
