package storage

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/util"
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
	mutex               sync.RWMutex
	deletionCounter     int
	fileCounter         int
	deletionByteCounter uint64
	fileByteCounter     uint64
	maximumFileKey      uint64
}

func (nm *baseNeedleMapper) IndexFileSize() uint64 {
	nm.mutex.RLock()
	defer nm.mutex.RUnlock()
	stat, err := nm.indexFile.Stat()
	if err == nil {
		return uint64(stat.Size())
	}
	return 0
}

func (nm *baseNeedleMapper) IndexFileName() string {
	nm.mutex.RLock()
	defer nm.mutex.RUnlock()
	return nm.indexFile.Name()
}

func idxFileEntry(bytes []byte) (key uint64, offset uint32, size uint32) {
	key = util.BytesToUint64(bytes[:8])
	offset = util.BytesToUint32(bytes[8:12])
	size = util.BytesToUint32(bytes[12:16])
	return
}
func (nm *baseNeedleMapper) appendToIndexFile(key uint64, offset uint32, size uint32) error {
	bytes := make([]byte, 16)
	util.Uint64toBytes(bytes[0:8], key)
	util.Uint32toBytes(bytes[8:12], offset)
	util.Uint32toBytes(bytes[12:16], size)

	nm.mutex.Lock()
	defer nm.mutex.Unlock()
	if _, err := nm.indexFile.Seek(0, 2); err != nil {
		return fmt.Errorf("cannot seek end of indexfile %s: %v",
			nm.indexFile.Name(), err)
	}
	_, err := nm.indexFile.Write(bytes)
	return err
}
func (nm *baseNeedleMapper) IndexFileContent() ([]byte, error) {
	nm.mutex.RLock()
	defer nm.mutex.RUnlock()
	return ioutil.ReadFile(nm.indexFile.Name())
}

func (nm *baseNeedleMapper) logDelete(deletedByteCount uint32) {
	nm.mutex.Lock()
	defer nm.mutex.Unlock()
	nm.deletionByteCounter = nm.deletionByteCounter + uint64(deletedByteCount)
	nm.deletionCounter++
}

func (nm *baseNeedleMapper) logPut(key uint64, oldSize uint32, newSize uint32) {
	nm.mutex.Lock()
	defer nm.mutex.Unlock()
	if key > nm.maximumFileKey {
		nm.maximumFileKey = key
	}
	nm.fileCounter++
	nm.fileByteCounter = nm.fileByteCounter + uint64(newSize)
	if oldSize > 0 {
		nm.deletionCounter++
		nm.deletionByteCounter = nm.deletionByteCounter + uint64(oldSize)
	}
}

func (nm *baseNeedleMapper) ContentSize() uint64 {
	nm.mutex.RLock()
	defer nm.mutex.RUnlock()
	return nm.fileByteCounter
}
func (nm *baseNeedleMapper) DeletedSize() uint64 {
	nm.mutex.RLock()
	defer nm.mutex.RUnlock()
	return nm.deletionByteCounter
}
func (nm *baseNeedleMapper) FileCount() int {
	nm.mutex.RLock()
	defer nm.mutex.RUnlock()
	return nm.fileCounter
}
func (nm *baseNeedleMapper) DeletedCount() int {
	nm.mutex.RLock()
	defer nm.mutex.RUnlock()
	return nm.deletionCounter
}
func (nm *baseNeedleMapper) MaxFileKey() uint64 {
	nm.mutex.RLock()
	defer nm.mutex.RUnlock()
	return nm.maximumFileKey
}
