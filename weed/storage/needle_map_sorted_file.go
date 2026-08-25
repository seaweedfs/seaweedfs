package storage

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// SortedFileNeedleMap backs every read-only volume, which on a tiered cluster
// is nearly every volume. It deliberately keeps no *os.File of its own:
// .idx and .sdx are borrowed from pooledIndexFiles per operation, so a volume
// nobody is reading costs zero descriptors. See needle_map_file_pool.go.
type SortedFileNeedleMap struct {
	mapMetric
	baseFileName  string
	indexFileName string
	dbFileName    string
	dbFileSize    int64

	indexFileAccessLock sync.Mutex
	indexFileOffset     int64
	indexNeedsSync      bool
}

func NewSortedFileNeedleMap(indexBaseFileName string, indexFile *os.File, version needle.Version) (m *SortedFileNeedleMap, err error) {
	m = &SortedFileNeedleMap{
		baseFileName:  indexBaseFileName,
		indexFileName: indexFile.Name(),
		dbFileName:    indexBaseFileName + ".sdx",
	}
	if !isSortedFileFresh(m.dbFileName, indexFile) {
		glog.V(0).Infof("Start to Generate %s from %s", m.dbFileName, indexFile.Name())
		erasure_coding.WriteSortedFileFromIdx(indexBaseFileName, ".sdx")
		glog.V(0).Infof("Finished Generating %s from %s", m.dbFileName, indexFile.Name())
	}

	dbStat, err := os.Stat(m.dbFileName)
	if err != nil {
		return nil, fmt.Errorf("stat %s: %v", m.dbFileName, err)
	}
	m.dbFileSize = dbStat.Size()
	// Seed indexFileOffset so Delete() appends tombstones to the tail of
	// .idx instead of overwriting from offset 0 and clobbering existing
	// records with tombstones for unrelated keys.
	indexStat, statErr := indexFile.Stat()
	if statErr != nil {
		return nil, fmt.Errorf("stat %s: %v", indexFile.Name(), statErr)
	}
	m.indexFileOffset = indexStat.Size()
	glog.V(1).Infof("Loading %s...", indexFile.Name())
	mm, indexLoadError := newNeedleMapMetricFromIndexFile(indexFile, version)
	if indexLoadError != nil {
		return nil, indexLoadError
	}
	m.mapMetric = *mm
	// Everything past the load walk goes through the pool, so hand the
	// caller's descriptor back instead of holding it for the volume's life.
	indexFile.Close()
	return
}

func isSortedFileFresh(dbFileName string, indexFile *os.File) bool {
	// normally we always write to index file first
	dbFile, err := os.Open(dbFileName)
	if err != nil {
		return false
	}
	defer dbFile.Close()
	dbStat, dbStatErr := dbFile.Stat()
	indexStat, indexStatErr := indexFile.Stat()
	if dbStatErr != nil || indexStatErr != nil {
		glog.V(0).Infof("Can not stat file: %v and %v", dbStatErr, indexStatErr)
		return false
	}

	return dbStat.ModTime().After(indexStat.ModTime())
}

func (m *SortedFileNeedleMap) Get(key NeedleId) (element *needle_map.NeedleValue, ok bool) {
	f, err := pooledIndexFiles.borrow(m.dbFileName, false)
	if err != nil {
		glog.Warningf("open %s: %v", m.dbFileName, err)
		return &needle_map.NeedleValue{Key: key}, false
	}
	offset, size, err := erasure_coding.SearchNeedleFromSortedIndex(f.file, m.dbFileSize, key, nil)
	pooledIndexFiles.release(f)
	ok = err == nil
	return &needle_map.NeedleValue{Key: key, Offset: offset, Size: size}, ok

}

func (m *SortedFileNeedleMap) Put(key NeedleId, offset Offset, size Size) error {
	return fmt.Errorf("needle map %s.sdx is read only: %w", m.baseFileName, os.ErrInvalid)
}

func (m *SortedFileNeedleMap) Delete(key NeedleId, offset Offset) error {

	f, err := pooledIndexFiles.borrow(m.dbFileName, true)
	if err != nil {
		return err
	}
	defer pooledIndexFiles.release(f)

	_, size, err := erasure_coding.SearchNeedleFromSortedIndex(f.file, m.dbFileSize, key, nil)

	if err != nil {
		if err == erasure_coding.NotFoundError {
			return nil
		}
		return err
	}

	if size.IsDeleted() {
		return nil
	}

	// write to index file first
	if err := m.appendToIndexFile(key, offset, TombstoneFileSize); err != nil {
		return err
	}
	_, _, err = erasure_coding.SearchNeedleFromSortedIndex(f.file, m.dbFileSize, key, erasure_coding.MarkNeedleDeleted)

	return err
}

func (m *SortedFileNeedleMap) appendToIndexFile(key NeedleId, offset Offset, size Size) error {
	f, err := pooledIndexFiles.borrow(m.indexFileName, true)
	if err != nil {
		return err
	}
	defer pooledIndexFiles.release(f)

	bytes := needle_map.ToBytes(key, offset, size)

	m.indexFileAccessLock.Lock()
	defer m.indexFileAccessLock.Unlock()
	written, err := f.file.WriteAt(bytes, m.indexFileOffset)
	if err == nil {
		m.indexFileOffset += int64(written)
		m.indexNeedsSync = true
	}
	return err
}

// IndexFileSize answers from the offset the appends maintain rather than a
// stat: the heartbeat asks every volume for this on every beat, and a
// read-only volume's .idx only ever grows through appendToIndexFile.
func (m *SortedFileNeedleMap) IndexFileSize() uint64 {
	m.indexFileAccessLock.Lock()
	defer m.indexFileAccessLock.Unlock()
	return uint64(m.indexFileOffset)
}

// Sync flushes tombstones appended by Delete. A read-only volume that has never
// been deleted from — the overwhelming majority — opens nothing here, so
// shutting down a server holding hundreds of thousands of them costs no fsyncs.
func (m *SortedFileNeedleMap) Sync() error {
	m.indexFileAccessLock.Lock()
	defer m.indexFileAccessLock.Unlock()
	if !m.indexNeedsSync {
		return nil
	}
	f, err := pooledIndexFiles.borrow(m.indexFileName, true)
	if err != nil {
		return err
	}
	defer pooledIndexFiles.release(f)
	if err := f.file.Sync(); err != nil {
		return err
	}
	m.indexNeedsSync = false
	return nil
}

func (m *SortedFileNeedleMap) ReadIndexEntry(n int64) (key NeedleId, offset Offset, size Size, err error) {
	var f *pooledFile
	if f, err = pooledIndexFiles.borrow(m.indexFileName, false); err != nil {
		return
	}
	defer pooledIndexFiles.release(f)

	bytes := make([]byte, NeedleMapEntrySize)
	var readCount int
	if readCount, err = f.file.ReadAt(bytes, n*NeedleMapEntrySize); err != nil {
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

func (m *SortedFileNeedleMap) Close() {
	if m == nil {
		return
	}
	// Drop the pooled handles too: the caller may be about to rename or remove
	// these paths, and a descriptor left behind would keep answering reads from
	// the old inode.
	pooledIndexFiles.discard(m.indexFileName)
	pooledIndexFiles.discard(m.dbFileName)
}

func (m *SortedFileNeedleMap) Destroy() error {
	m.Close()
	os.Remove(m.indexFileName)
	return os.Remove(m.dbFileName)
}
