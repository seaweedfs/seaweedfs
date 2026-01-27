package erasure_coding_test

import (
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	erasure_coding "github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

func TestShardsInfoDeleteParityShards(t *testing.T) {
	si := erasure_coding.NewShardsInfo()
	for _, id := range erasure_coding.AllShardIds() {
		si.Set(erasure_coding.ShardInfo{Id: id, Size: 123})
	}
	si.DeleteParityShards()

	if got, want := si.String(), "0:123 B 1:123 B 2:123 B 3:123 B 4:123 B 5:123 B 6:123 B 7:123 B 8:123 B 9:123 B"; got != want {
		t.Errorf("expected %q, got %q", want, got)
	}

}

func TestShardsInfoAsSlice(t *testing.T) {
	si := erasure_coding.NewShardsInfo()
	si.Set(erasure_coding.ShardInfo{Id: 5, Size: 555})
	si.Set(erasure_coding.ShardInfo{Id: 2, Size: 222})
	si.Set(erasure_coding.ShardInfo{Id: 7, Size: 777})
	si.Set(erasure_coding.ShardInfo{Id: 1, Size: 111})

	want := []erasure_coding.ShardInfo{
		{Id: 1, Size: 111},
		{Id: 2, Size: 222},
		{Id: 5, Size: 555},
		{Id: 7, Size: 777},
	}
	if got := si.AsSlice(); !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
}

func TestShardsInfoSerialize(t *testing.T) {
	testCases := []struct {
		name      string
		shardIds  map[erasure_coding.ShardId]erasure_coding.ShardSize
		wantBits  uint32
		wantSizes []erasure_coding.ShardSize
	}{
		{
			name:      "no bits",
			shardIds:  nil,
			wantBits:  0b0,
			wantSizes: []erasure_coding.ShardSize{},
		},
		{
			name: "single shard, first",
			shardIds: map[erasure_coding.ShardId]erasure_coding.ShardSize{
				0: 2345,
			},
			wantBits:  0b1,
			wantSizes: []erasure_coding.ShardSize{2345},
		},
		{
			name: "single shard, 5th",
			shardIds: map[erasure_coding.ShardId]erasure_coding.ShardSize{
				4: 6789,
			},
			wantBits:  0b10000,
			wantSizes: []erasure_coding.ShardSize{6789},
		},
		{
			name: "multiple shards",
			shardIds: map[erasure_coding.ShardId]erasure_coding.ShardSize{
				8: 800,
				0: 5,
				3: 300,
				1: 100,
			},
			wantBits:  0b100001011,
			wantSizes: []erasure_coding.ShardSize{5, 100, 300, 800},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			si := erasure_coding.NewShardsInfo()
			for id, size := range tc.shardIds {
				si.Set(erasure_coding.ShardInfo{Id: id, Size: size})
			}

			if got, want := si.Bitmap(), tc.wantBits; got != want {
				t.Errorf("expected bits %v, got %v", want, got)
			}
			if got, want := si.Sizes(), tc.wantSizes; !reflect.DeepEqual(got, want) {
				t.Errorf("expected sizes %v, got %v", want, got)
			}
		})
	}
}

func TestShardsInfoFromVolumeEcShardInformationMessage(t *testing.T) {
	testCases := []struct {
		name    string
		ecvInfo *master_pb.VolumeEcShardInformationMessage
		want    string
	}{
		{
			name:    "no msg",
			ecvInfo: nil,
			want:    "",
		},
		{
			name:    "no shards",
			ecvInfo: &master_pb.VolumeEcShardInformationMessage{},
			want:    "",
		},
		{
			name: "single shard",
			ecvInfo: &master_pb.VolumeEcShardInformationMessage{
				EcIndexBits: 0b100,
				ShardSizes:  []int64{333},
			},
			want: "2:333 B",
		},
		{
			name: "multiple shards",
			ecvInfo: &master_pb.VolumeEcShardInformationMessage{
				EcIndexBits: 0b1101,
				ShardSizes:  []int64{111, 333, 444},
			},
			want: "0:111 B 2:333 B 3:444 B",
		},
		{
			name: "multiple shards with missing sizes",
			ecvInfo: &master_pb.VolumeEcShardInformationMessage{
				EcIndexBits: 0b110110,
				ShardSizes:  []int64{111, 333, 444},
			},
			want: "1:111 B 2:333 B 4:444 B 5:0 B",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			si := erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(tc.ecvInfo)
			if got, want := si.String(), tc.want; got != want {
				t.Errorf("expected %q, got %q", want, got)
			}
		})
	}
}

func TestShardsInfoCombine(t *testing.T) {
	a := erasure_coding.NewShardsInfo()
	a.Set(erasure_coding.ShardInfo{Id: 1, Size: 111})
	a.Set(erasure_coding.ShardInfo{Id: 2, Size: 222})
	a.Set(erasure_coding.ShardInfo{Id: 3, Size: 333})
	a.Set(erasure_coding.ShardInfo{Id: 4, Size: 444})
	a.Set(erasure_coding.ShardInfo{Id: 5, Size: 0})

	b := erasure_coding.NewShardsInfo()
	b.Set(erasure_coding.ShardInfo{Id: 1, Size: 555})
	b.Set(erasure_coding.ShardInfo{Id: 4, Size: 666})
	b.Set(erasure_coding.ShardInfo{Id: 5, Size: 777})
	b.Set(erasure_coding.ShardInfo{Id: 6, Size: 888})

	if got, want := a.Plus(b).String(), "1:555 B 2:222 B 3:333 B 4:666 B 5:777 B 6:888 B"; got != want {
		t.Errorf("expected %q for plus, got %q", want, got)
	}
	if got, want := a.Minus(b).String(), "2:222 B 3:333 B"; got != want {
		t.Errorf("expected %q for minus, got %q", want, got)
	}
}
