package storage

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/storage/idx"
	"github.com/chrislusf/seaweedfs/weed/storage/needle_map"
	. "github.com/chrislusf/seaweedfs/weed/storage/types"
)

type NeedleMapType int

const (
	NeedleMapInMemory      NeedleMapType = iota
	NeedleMapLevelDb                     // small memory footprint, 4MB total, 1 write buffer, 3 block buffer
	NeedleMapLevelDbMedium               // medium memory footprint, 8MB total, 3 write buffer, 5 block buffer
	NeedleMapLevelDbLarge                // large memory footprint, 12MB total, 4write buffer, 8 block buffer
)

type NeedleMapper interface {
	Put(key NeedleId, offset Offset, size Size) error
	Get(key NeedleId) (element *needle_map.NeedleValue, ok bool)
	Delete(key NeedleId, offset Offset) error
	Close()
	Destroy() error
	ContentSize() uint64
	DeletedSize() uint64
	FileCount() int
	DeletedCount() int
	MaxFileKey() NeedleId
	IndexFileSize() uint64
	Sync() error
	ReadIndexEntry(n int64) (key NeedleId, offset Offset, size Size, err error)
}

type baseNeedleMapper struct {
	mapMetric

	indexFile           *os.File
	indexFileAccessLock sync.Mutex
}

func (nm *baseNeedleMapper) IndexFileSize() uint64 {
	stat, err := nm.indexFile.Stat()
	if err == nil {
		return uint64(stat.Size())
	}
	return 0
}

func (nm *baseNeedleMapper) appendToIndexFile(key NeedleId, offset Offset, size Size) error {
	bytes := needle_map.ToBytes(key, offset, size)

	nm.indexFileAccessLock.Lock()
	defer nm.indexFileAccessLock.Unlock()
	if _, err := nm.indexFile.Seek(0, 2); err != nil {
		return fmt.Errorf("cannot seek end of indexfile %s: %v",
			nm.indexFile.Name(), err)
	}
	_, err := nm.indexFile.Write(bytes)
	return err
}

func (nm *baseNeedleMapper) Sync() error {
	return nm.indexFile.Sync()
}

func (nm *baseNeedleMapper) ReadIndexEntry(n int64) (key NeedleId, offset Offset, size Size, err error) {
	bytes := make([]byte, NeedleMapEntrySize)
	var readCount int
	if readCount, err = nm.indexFile.ReadAt(bytes, n*NeedleMapEntrySize); err != nil {
		if err == io.EOF {
			if readCount == NeedleMapEntrySize {
				err = nil
			}
		}
		if err != nil {
			return
		}
	}
	key, offset, size = idx.IdxFileEntry(bytes)
	return
}
