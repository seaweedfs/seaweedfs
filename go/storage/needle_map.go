package storage

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"

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
	IndexFileSize() uint64
	IndexFileContent() ([]byte, error)
	IndexFileName() string
}

type baseNeedleMapper struct {
	indexFile           *os.File
	indexFileAccessLock sync.Mutex

	mapMetric
}

func (nm baseNeedleMapper) IndexFileSize() uint64 {
	stat, err := nm.indexFile.Stat()
	if err == nil {
		return uint64(stat.Size())
	}
	return 0
}

func (nm baseNeedleMapper) IndexFileName() string {
	return nm.indexFile.Name()
}

func idxFileEntry(bytes []byte) (key uint64, offset uint32, size uint32) {
	key = util.BytesToUint64(bytes[:8])
	offset = util.BytesToUint32(bytes[8:12])
	size = util.BytesToUint32(bytes[12:16])
	return
}
func (nm baseNeedleMapper) appendToIndexFile(key uint64, offset uint32, size uint32) error {
	bytes := make([]byte, 16)
	util.Uint64toBytes(bytes[0:8], key)
	util.Uint32toBytes(bytes[8:12], offset)
	util.Uint32toBytes(bytes[12:16], size)

	nm.indexFileAccessLock.Lock()
	defer nm.indexFileAccessLock.Unlock()
	if _, err := nm.indexFile.Seek(0, 2); err != nil {
		return fmt.Errorf("cannot seek end of indexfile %s: %v",
			nm.indexFile.Name(), err)
	}
	_, err := nm.indexFile.Write(bytes)
	return err
}
func (nm baseNeedleMapper) IndexFileContent() ([]byte, error) {
	nm.indexFileAccessLock.Lock()
	defer nm.indexFileAccessLock.Unlock()
	return ioutil.ReadFile(nm.indexFile.Name())
}

type mapMetric struct {
	indexFile *os.File

	DeletionCounter     int    `json:"DeletionCounter"`
	FileCounter         int    `json:"FileCounter"`
	DeletionByteCounter uint64 `json:"DeletionByteCounter"`
	FileByteCounter     uint64 `json:"FileByteCounter"`
	MaximumFileKey      uint64 `json:"MaxFileKey"`
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

func (mm mapMetric) ContentSize() uint64 {
	return mm.FileByteCounter
}
func (mm mapMetric) DeletedSize() uint64 {
	return mm.DeletionByteCounter
}
func (mm mapMetric) FileCount() int {
	return mm.FileCounter
}
func (mm mapMetric) DeletedCount() int {
	return mm.DeletionCounter
}
func (mm mapMetric) MaxFileKey() uint64 {
	return mm.MaximumFileKey
}
