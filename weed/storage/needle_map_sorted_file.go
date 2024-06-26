package storage

import (
	"os"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

type SortedFileNeedleMap struct {
	baseNeedleMapper
	baseFileName string
	dbFile       *os.File
	dbFileSize   int64
}

func NewSortedFileNeedleMap(indexBaseFileName string, indexFile *os.File) (m *SortedFileNeedleMap, err error) {
	m = &SortedFileNeedleMap{baseFileName: indexBaseFileName}
	m.indexFile = indexFile
	fileName := indexBaseFileName + ".sdx"
	if !isSortedFileFresh(fileName, indexFile) {
		glog.V(0).Infof("Start to Generate %s from %s", fileName, indexFile.Name())
		erasure_coding.WriteSortedFileFromIdx(indexBaseFileName, ".sdx")
		glog.V(0).Infof("Finished Generating %s from %s", fileName, indexFile.Name())
	}
	glog.V(1).Infof("Opening %s...", fileName)

	if m.dbFile, err = os.OpenFile(indexBaseFileName+".sdx", os.O_RDWR, 0); err != nil {
		return
	}
	dbStat, _ := m.dbFile.Stat()
	m.dbFileSize = dbStat.Size()
	glog.V(1).Infof("Loading %s...", indexFile.Name())
	mm, indexLoadError := newNeedleMapMetricFromIndexFile(indexFile)
	if indexLoadError != nil {
		_ = m.dbFile.Close()
		return nil, indexLoadError
	}
	m.mapMetric = *mm
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
	offset, size, err := erasure_coding.SearchNeedleFromSortedIndex(m.dbFile, m.dbFileSize, key, nil)
	ok = err == nil
	return &needle_map.NeedleValue{Key: key, Offset: offset, Size: size}, ok

}

func (m *SortedFileNeedleMap) Put(key NeedleId, offset Offset, size Size) error {
	return os.ErrInvalid
}

func (m *SortedFileNeedleMap) Delete(key NeedleId, offset Offset) error {

	_, size, err := erasure_coding.SearchNeedleFromSortedIndex(m.dbFile, m.dbFileSize, key, nil)

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
	_, _, err = erasure_coding.SearchNeedleFromSortedIndex(m.dbFile, m.dbFileSize, key, erasure_coding.MarkNeedleDeleted)

	return err
}

func (m *SortedFileNeedleMap) Close() {
	if m == nil {
		return
	}
	if m.indexFile != nil {
		m.indexFile.Close()
	}
	if m.dbFile != nil {
		m.dbFile.Close()
	}
}

func (m *SortedFileNeedleMap) Destroy() error {
	m.Close()
	os.Remove(m.indexFile.Name())
	return os.Remove(m.baseFileName + ".sdx")
}
