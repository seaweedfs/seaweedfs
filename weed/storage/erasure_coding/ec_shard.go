package erasure_coding

import (
	"fmt"
	"os"
	"path"
	"strconv"

	"github.com/chrislusf/seaweedfs/weed/storage/idx"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
)

type ShardId uint8

type EcVolumeShard struct {
	VolumeId    needle.VolumeId
	ShardId     ShardId
	Collection  string
	dir         string
	ecdFile     *os.File
	ecdFileSize int64
	ecxFile     *os.File
	ecxFileSize int64
}

func NewEcVolumeShard(dirname string, collection string, id needle.VolumeId, shardId ShardId) (v *EcVolumeShard, e error) {

	v = &EcVolumeShard{dir: dirname, Collection: collection, VolumeId: id, ShardId: shardId}

	baseFileName := v.FileName()

	// open ecx file
	if v.ecxFile, e = os.OpenFile(baseFileName+".ecx", os.O_RDONLY, 0644); e != nil {
		return nil, fmt.Errorf("cannot read ec volume index %s.ecx: %v", baseFileName, e)
	}
	ecxFi, statErr := v.ecxFile.Stat()
	if statErr != nil {
		return nil, fmt.Errorf("can not stat ec volume index %s.ecx: %v", baseFileName, statErr)
	}
	v.ecxFileSize = ecxFi.Size()

	// open ecd file
	if v.ecdFile, e = os.OpenFile(baseFileName+ToExt(int(shardId)), os.O_RDONLY, 0644); e != nil {
		return nil, fmt.Errorf("cannot read ec volume shard %s.%s: %v", baseFileName, ToExt(int(shardId)), e)
	}
	ecdFi, statErr := v.ecdFile.Stat()
	if statErr != nil {
		return nil, fmt.Errorf("can not stat ec volume shard %s.%s: %v", baseFileName, ToExt(int(shardId)), statErr)
	}
	v.ecdFileSize = ecdFi.Size()

	return
}

func (shard *EcVolumeShard) String() string {
	return fmt.Sprintf("ec shard %v:%v, dir:%s, Collection:%s", shard.VolumeId, shard.ShardId, shard.dir, shard.Collection)
}

func (shard *EcVolumeShard) FileName() (fileName string) {
	return EcShardFileName(shard.Collection, shard.dir, int(shard.VolumeId))
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

func (shard *EcVolumeShard) Close() {
	if shard.ecdFile != nil {
		_ = shard.ecdFile.Close()
		shard.ecdFile = nil
	}
	if shard.ecxFile != nil {
		_ = shard.ecxFile.Close()
		shard.ecxFile = nil
	}
}

func (shard *EcVolumeShard) findNeedleFromEcx(needleId types.NeedleId) (offset types.Offset, size uint32, err error) {
	var key types.NeedleId
	buf := make([]byte, types.NeedleMapEntrySize)
	l, h := int64(0), shard.ecxFileSize/types.NeedleMapEntrySize
	for l < h {
		m := (l + h) / 2
		if _, err := shard.ecxFile.ReadAt(buf, m*types.NeedleMapEntrySize); err != nil {
			return types.Offset{}, 0, err
		}
		key, offset, size = idx.IdxFileEntry(buf)
		if key == needleId {
			return
		}
		if key < needleId {
			l = m + 1
		} else {
			h = m
		}
	}

	err = fmt.Errorf("needle id %d not found", needleId)
	return
}
