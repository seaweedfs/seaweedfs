package erasure_coding

import (
	"math"
	"sort"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
)

type EcVolume struct {
	Shards []*EcVolumeShard
}

func (ev *EcVolume) AddEcVolumeShard(ecVolumeShard *EcVolumeShard) bool {
	for _, s := range ev.Shards {
		if s.ShardId == ecVolumeShard.ShardId {
			return false
		}
	}
	ev.Shards = append(ev.Shards, ecVolumeShard)
	sort.Slice(ev, func(i, j int) bool {
		return ev.Shards[i].VolumeId < ev.Shards[j].VolumeId ||
			ev.Shards[i].VolumeId == ev.Shards[j].VolumeId && ev.Shards[i].ShardId < ev.Shards[j].ShardId
	})
	return true
}

func (ev *EcVolume) DeleteEcVolumeShard(shardId ShardId) bool {
	foundPosition := -1
	for i, s := range ev.Shards {
		if s.ShardId == shardId {
			foundPosition = i
		}
	}
	if foundPosition < 0 {
		return false
	}

	ev.Shards = append(ev.Shards[:foundPosition], ev.Shards[foundPosition+1:]...)
	return true
}

func (ev *EcVolume) FindEcVolumeShard(shardId ShardId) (ecVolumeShard *EcVolumeShard, found bool) {
	for _, s := range ev.Shards {
		if s.ShardId == shardId {
			return s, true
		}
	}
	return nil, false
}

func (ev *EcVolume) Close() {
	for _, s := range ev.Shards {
		s.Close()
	}
}

func (ev *EcVolume) ToVolumeEcShardInformationMessage() (messages []*master_pb.VolumeEcShardInformationMessage) {
	prevVolumeId := needle.VolumeId(math.MaxUint32)
	var m *master_pb.VolumeEcShardInformationMessage
	for _, s := range ev.Shards {
		if s.VolumeId != prevVolumeId {
			m = &master_pb.VolumeEcShardInformationMessage{
				Id:         uint32(s.VolumeId),
				Collection: s.Collection,
			}
			messages = append(messages, m)
		}
		prevVolumeId = s.VolumeId
		m.EcIndexBits = uint32(ShardBits(m.EcIndexBits).AddShardId(s.ShardId))
	}
	return
}

func (ev *EcVolume) LocateEcShardNeedle(n *needle.Needle) (offset types.Offset, size uint32, intervals []Interval, err error) {

	shard := ev.Shards[0]
	// find the needle from ecx file
	offset, size, err = shard.findNeedleFromEcx(n.Id)
	if err != nil {
		return types.Offset{}, 0, nil, err
	}

	// calculate the locations in the ec shards
	intervals = LocateData(ErasureCodingLargeBlockSize, ErasureCodingSmallBlockSize, shard.ecxFileSize, offset.ToAcutalOffset(), size)

	return
}
