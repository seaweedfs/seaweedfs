package erasure_coding

import (
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

type ShardId uint8

type EcVolumeShard struct {
	VolumeId    needle.VolumeId
	ShardId     ShardId
	Collection  string
	dir         string
	ecdFile     *os.File
	ecdFileSize int64
	DiskType    types.DiskType
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

	v.Mount()

	return
}

func (shard *EcVolumeShard) Mount() {
	stats.VolumeServerVolumeGauge.WithLabelValues(shard.Collection, "ec_shards").Inc()
}

func (shard *EcVolumeShard) Unmount() {
	stats.VolumeServerVolumeGauge.WithLabelValues(shard.Collection, "ec_shards").Dec()
}

func (shard *EcVolumeShard) Size() int64 {
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
	if shard.ecdFile != nil {
		_ = shard.ecdFile.Close()
		shard.ecdFile = nil
	}
}

func (shard *EcVolumeShard) Destroy() {
	shard.Unmount()
	os.Remove(shard.FileName() + ToExt(int(shard.ShardId)))
}

func (shard *EcVolumeShard) ReadAt(buf []byte, offset int64) (int, error) {

	n, err := shard.ecdFile.ReadAt(buf, offset)
	if err == io.EOF && n == len(buf) {
		err = nil
	}
	return n, err

}
