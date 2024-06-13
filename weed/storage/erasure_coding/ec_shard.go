package erasure_coding

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
)

type ShardId uint8

type EcVolumeShard struct {
	VolumeId          needle.VolumeId
	ShardId           ShardId
	Collection        string
	dir               string
	ecdFile           *os.File
	ecdFileAccessLock sync.RWMutex
	ecdFileSize       int64
	DiskType          types.DiskType
}

func NewEcVolumeShard(diskType types.DiskType, dirname string, collection string, id needle.VolumeId, shardId ShardId) (v *EcVolumeShard, e error) {

	v = &EcVolumeShard{dir: dirname, Collection: collection, VolumeId: id, ShardId: shardId, DiskType: diskType}

	baseFileName := v.FileName()

	// open ecd file
	if v.ecdFile, e = os.OpenFile(baseFileName+ToExt(int(shardId)), os.O_RDONLY, 0644); e != nil {
		if e == os.ErrNotExist || strings.Contains(e.Error(), "no such file or directory") {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("cannot read ec volume shard %s%s: %v", baseFileName, ToExt(int(shardId)), e)
	}
	ecdFi, statErr := v.ecdFile.Stat()
	if statErr != nil {
		_ = v.ecdFile.Close()
		return nil, fmt.Errorf("can not stat ec volume shard %s%s: %v", baseFileName, ToExt(int(shardId)), statErr)
	}
	v.ecdFileSize = ecdFi.Size()

	stats.VolumeServerVolumeGauge.WithLabelValues(v.Collection, "ec_shards").Inc()

	return
}

func (shard *EcVolumeShard) Size() int64 {
	shard.ecdFileAccessLock.RLock()
	defer shard.ecdFileAccessLock.Unlock()

	if shard.ecdFile == nil {
		err := shard.tryOpenEcdFile()
		if err != nil {
			return 0
		}
	}
	return shard.ecdFileSize
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

func EcShardBaseFileName(collection string, id int) (baseFileName string) {
	baseFileName = strconv.Itoa(id)
	if collection != "" {
		baseFileName = collection + "_" + baseFileName
	}
	return
}

func (shard *EcVolumeShard) Close() {
	shard.ecdFileAccessLock.Lock()
	defer shard.ecdFileAccessLock.Unlock()

	if shard.ecdFile != nil {
		_ = shard.ecdFile.Close()
		shard.ecdFile = nil
	}
}

func (shard *EcVolumeShard) Destroy() {
	os.Remove(shard.FileName() + ToExt(int(shard.ShardId)))
	stats.VolumeServerVolumeGauge.WithLabelValues(shard.Collection, "ec_shards").Dec()
}

func (shard *EcVolumeShard) ReadAt(buf []byte, offset int64) (int, error) {
	shard.ecdFileAccessLock.RLock()
	defer shard.ecdFileAccessLock.RUnlock()

	if shard.ecdFile == nil {
		err := shard.tryOpenEcdFile()
		if err != nil {
			return 0, err
		}
	}

	return shard.ecdFile.ReadAt(buf, offset)
}

func (shard *EcVolumeShard) tryOpenEcdFile() (err error) {
	shard.ecdFileAccessLock.Lock()
	defer shard.ecdFileAccessLock.Unlock()

	if shard.ecdFile == nil {
		baseFileName := shard.FileName()
		if shard.ecdFile, err = os.OpenFile(baseFileName+ToExt(int(shard.ShardId)), os.O_RDONLY, 0644); err != nil {
			if err == os.ErrNotExist || strings.Contains(err.Error(), "no such file or directory") {
				return err
			}
			return fmt.Errorf("cannot read ec volume shard %s%s: %v", baseFileName, ToExt(int(shard.ShardId)), err)
		}
	}
	return
}
