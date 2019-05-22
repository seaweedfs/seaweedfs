package erasure_coding

import (
	"fmt"
	"os"
	"path"
	"strconv"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

type EcVolumeShard struct {
	VolumeId   needle.VolumeId
	ShardId    uint8
	Collection string
	dir        string
	ecdFile    *os.File
	ecxFile    *os.File
}
type EcVolumeShards []*EcVolumeShard

func NewEcVolumeShard(dirname string, collection string, id needle.VolumeId, shardId int) (v *EcVolumeShard, e error) {

	v = &EcVolumeShard{dir: dirname, Collection: collection, VolumeId: id, ShardId: uint8(shardId)}

	baseFileName := v.FileName()
	if v.ecxFile, e = os.OpenFile(baseFileName+".ecx", os.O_RDONLY, 0644); e != nil {
		return nil, fmt.Errorf("cannot read ec volume index %s.ecx: %v", baseFileName, e)
	}
	if v.ecdFile, e = os.OpenFile(baseFileName+ToExt(shardId), os.O_RDONLY, 0644); e != nil {
		return nil, fmt.Errorf("cannot read ec volume shard %s.%s: %v", baseFileName, ToExt(shardId), e)
	}

	return
}

func (shards *EcVolumeShards) AddEcVolumeShard(ecVolumeShard *EcVolumeShard) bool {
	for _, s := range *shards {
		if s.ShardId == ecVolumeShard.ShardId {
			return false
		}
	}
	*shards = append(*shards, ecVolumeShard)
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

func (shards *EcVolumeShards) Close() {
	for _, s := range *shards {
		s.Close()
	}
}

func (shards *EcVolumeShards) ToVolumeInformationMessage() (messages []*master_pb.VolumeEcShardInformationMessage) {
	for _, s := range *shards {
		m := &master_pb.VolumeEcShardInformationMessage{
			Id:         uint32(s.VolumeId),
			Collection: s.Collection,
			EcIndex:    uint32(s.ShardId),
		}
		messages = append(messages, m)
	}
	return
}

func (v *EcVolumeShard) String() string {
	return fmt.Sprintf("ec shard %v:%v, dir:%s, Collection:%s", v.VolumeId, v.ShardId, v.dir, v.Collection)
}

func (v *EcVolumeShard) FileName() (fileName string) {
	return EcShardFileName(v.Collection, v.dir, int(v.VolumeId))
}

func EcShardFileName(collection string, dir string, id int) (fileName string) {
	idString := strconv.Itoa(id)
	if collection == "" {
		fileName = path.Join(dir, idString)
	} else {
		fileName = path.Join(dir, collection+"_"+idString)
	}
	return
}

func (v *EcVolumeShard) Close() {
	if v.ecdFile != nil {
		_ = v.ecdFile.Close()
		v.ecdFile = nil
	}
	if v.ecxFile != nil {
		_ = v.ecxFile.Close()
		v.ecxFile = nil
	}
}
