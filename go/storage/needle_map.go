package storage

import (
	"fmt"
	"os"

	"github.com/chrislusf/seaweedfs/go/util"
)

type NeedleMapType int

const (
	NeedleMapInMemory NeedleMapType = iota
	NeedleMapLevelDb
	NeedleMapBoltDb
)

type NeedleMapper interface {
	Put(key uint64, offset uint32, size uint32) error
	Get(key uint64) (element *NeedleValue, ok bool)
	Delete(key uint64) error
	Close()
	Destroy() error
	ContentSize() uint64
	DeletedSize() uint64
	FileCount() int
	DeletedCount() int
	MaxFileKey() uint64
}

type mapMetric struct {
	DeletionCounter     int    `json:"DeletionCounter"`
	FileCounter         int    `json:"FileCounter"`
	DeletionByteCounter uint64 `json:"DeletionByteCounter"`
	FileByteCounter     uint64 `json:"FileByteCounter"`
	MaximumFileKey      uint64 `json:"MaxFileKey"`
}

func appendToIndexFile(indexFile *os.File,
	key uint64, offset uint32, size uint32) error {
	bytes := make([]byte, 16)
	util.Uint64toBytes(bytes[0:8], key)
	util.Uint32toBytes(bytes[8:12], offset)
	util.Uint32toBytes(bytes[12:16], size)
	if _, err := indexFile.Seek(0, 2); err != nil {
		return fmt.Errorf("cannot seek end of indexfile %s: %v",
			indexFile.Name(), err)
	}
	_, err := indexFile.Write(bytes)
	return err
}

func (mm *mapMetric) logDelete(deletedByteCount uint32) {
	mm.DeletionByteCounter = mm.DeletionByteCounter + uint64(deletedByteCount)
	mm.DeletionCounter++
}

func (mm *mapMetric) logPut(key uint64, oldSize uint32, newSize uint32) {
	if key > mm.MaximumFileKey {
		mm.MaximumFileKey = key
	}
	mm.FileCounter++
	mm.FileByteCounter = mm.FileByteCounter + uint64(newSize)
	if oldSize > 0 {
		mm.DeletionCounter++
		mm.DeletionByteCounter = mm.DeletionByteCounter + uint64(oldSize)
	}
}
