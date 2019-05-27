package erasure_coding

import (
	"math"
	"sort"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

type EcVolumeShards []*EcVolumeShard

func (shards *EcVolumeShards) AddEcVolumeShard(ecVolumeShard *EcVolumeShard) bool {
	for _, s := range *shards {
		if s.ShardId == ecVolumeShard.ShardId {
			return false
		}
	}
	*shards = append(*shards, ecVolumeShard)
	sort.Slice(shards, func(i, j int) bool {
		return (*shards)[i].VolumeId < (*shards)[j].VolumeId ||
			(*shards)[i].VolumeId == (*shards)[j].VolumeId && (*shards)[i].ShardId < (*shards)[j].ShardId
	})
	return true
}

func (shards *EcVolumeShards) DeleteEcVolumeShard(ecVolumeShard *EcVolumeShard) bool {
	foundPosition := -1
	for i, s := range *shards {
		if s.ShardId == ecVolumeShard.ShardId {
			foundPosition = i
		}
	}
	if foundPosition < 0 {
		return false
	}

	*shards = append((*shards)[:foundPosition], (*shards)[foundPosition+1:]...)
	return true
}

func (shards *EcVolumeShards) FindEcVolumeShard(shardId ShardId) (ecVolumeShard *EcVolumeShard, found bool) {
	for _, s := range *shards {
		if s.ShardId == shardId {
			return s, true
		}
	}
	return nil, false
}

func (shards *EcVolumeShards) Close() {
	for _, s := range *shards {
		s.Close()
	}
}

func (shards *EcVolumeShards) ToVolumeEcShardInformationMessage() (messages []*master_pb.VolumeEcShardInformationMessage) {
	prevVolumeId := needle.VolumeId(math.MaxUint32)
	var m *master_pb.VolumeEcShardInformationMessage
	for _, s := range *shards {
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

func (shards *EcVolumeShards) ReadEcShardNeedle(n *needle.Needle) (int, error) {

	shard := (*shards)[0]
	// find the needle from ecx file
	offset, size, err := shard.findNeedleFromEcx(n.Id)
	if err != nil {
		return 0, err
	}

	// calculate the locations in the ec shards
	intervals := LocateData(ErasureCodingLargeBlockSize, ErasureCodingSmallBlockSize, shard.ecxFileSize, offset.ToAcutalOffset(), size)

	// TODO read the intervals

	return len(intervals), nil
}
