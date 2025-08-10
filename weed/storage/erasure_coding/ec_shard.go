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

func NewEcVolumeShard(diskType types.DiskType, dirname string, collection string, id needle.VolumeId, shardId ShardId, generation uint32) (v *EcVolumeShard, e error) {

	v = &EcVolumeShard{dir: dirname, Collection: collection, VolumeId: id, ShardId: shardId, DiskType: diskType}

	baseFileName := v.FileNameWithGeneration(generation)

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

func (shard *EcVolumeShard) FileNameWithGeneration(generation uint32) (fileName string) {
	return EcShardFileNameWithGeneration(shard.Collection, shard.dir, int(shard.VolumeId), generation)
}

func EcShardFileName(collection string, dir string, id int) (fileName string) {
	return EcShardFileNameWithGeneration(collection, dir, id, 0)
}

// EcShardFileNameWithGeneration generates filename for EC volume files with generation support
//
// Generation File Layout Design:
//
//   - Generation 0 (default): Uses existing filename format for backward compatibility
//     Example: "data_123" -> data_123.vif, data_123.ecx, data_123.ec00, etc.
//
//   - Generation > 0: Adds "_g{N}" suffix to base filename
//     Example: "data_123_g1" -> data_123_g1.vif, data_123_g1.ecx, data_123_g1.ec00, etc.
//
// This approach provides:
// - Backward compatibility: Existing volumes continue to work without changes
// - Atomic operations: All files for a generation share the same base name
// - Easy cleanup: Delete files matching pattern "volume_*_g{N}.*"
// - Performance: All files remain in the same directory for fast access
func EcShardFileNameWithGeneration(collection string, dir string, id int, generation uint32) (fileName string) {
	idString := strconv.Itoa(id)
	var baseFileName string

	if collection == "" {
		baseFileName = idString
	} else {
		baseFileName = collection + "_" + idString
	}

	// Add generation suffix for non-zero generations (backward compatibility)
	if generation > 0 {
		baseFileName = baseFileName + "_g" + strconv.FormatUint(uint64(generation), 10)
	}

	fileName = path.Join(dir, baseFileName)
	return
}

func EcShardBaseFileName(collection string, id int) (baseFileName string) {
	return EcShardBaseFileNameWithGeneration(collection, id, 0)
}

func EcShardBaseFileNameWithGeneration(collection string, id int, generation uint32) (baseFileName string) {
	baseFileName = strconv.Itoa(id)
	if collection != "" {
		baseFileName = collection + "_" + baseFileName
	}

	// Add generation suffix for non-zero generations (backward compatibility)
	if generation > 0 {
		baseFileName = baseFileName + "_g" + strconv.FormatUint(uint64(generation), 10)
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

// ParseGenerationFromFileName extracts generation from EC volume filename
// Returns 0 for files without generation suffix (backward compatibility)
func ParseGenerationFromFileName(fileName string) uint32 {
	// Remove extension first
	baseName := fileName
	if lastDot := strings.LastIndex(fileName, "."); lastDot >= 0 {
		baseName = fileName[:lastDot]
	}

	// Look for _g{N} pattern at the end
	if gIndex := strings.LastIndex(baseName, "_g"); gIndex >= 0 {
		generationStr := baseName[gIndex+2:]
		if generation, err := strconv.ParseUint(generationStr, 10, 32); err == nil {
			return uint32(generation)
		}
	}

	// No generation suffix found, return 0 for backward compatibility
	return 0
}
