package storage

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/syndtr/goleveldb/leveldb"
)

type LevelDbNeedleMap struct {
	dbFileName string
	db         *leveldb.DB
	baseNeedleMapper
}

func NewLevelDbNeedleMap(dbFileName string, indexFile *os.File) (m *LevelDbNeedleMap, err error) {
	m = &LevelDbNeedleMap{dbFileName: dbFileName}
	m.indexFile = indexFile
	if !isLevelDbFresh(dbFileName, indexFile) {
		glog.V(1).Infof("Start to Generate %s from %s", dbFileName, indexFile.Name())
		generateLevelDbFile(dbFileName, indexFile)
		glog.V(1).Infof("Finished Generating %s from %s", dbFileName, indexFile.Name())
	}
	glog.V(1).Infof("Opening %s...", dbFileName)
	if m.db, err = leveldb.OpenFile(dbFileName, nil); err != nil {
		return
	}
	glog.V(1).Infof("Loading %s...", indexFile.Name())
	nm, indexLoadError := LoadBtreeNeedleMap(indexFile)
	if indexLoadError != nil {
		return nil, indexLoadError
	}
	m.mapMetric = nm.mapMetric
	return
}

func isLevelDbFresh(dbFileName string, indexFile *os.File) bool {
	// normally we always write to index file first
	dbLogFile, err := os.Open(filepath.Join(dbFileName, "LOG"))
	if err != nil {
		return false
	}
	defer dbLogFile.Close()
	dbStat, dbStatErr := dbLogFile.Stat()
	indexStat, indexStatErr := indexFile.Stat()
	if dbStatErr != nil || indexStatErr != nil {
		glog.V(0).Infof("Can not stat file: %v and %v", dbStatErr, indexStatErr)
		return false
	}

	return dbStat.ModTime().After(indexStat.ModTime())
}

func generateLevelDbFile(dbFileName string, indexFile *os.File) error {
	db, err := leveldb.OpenFile(dbFileName, nil)
	if err != nil {
		return err
	}
	defer db.Close()
	return WalkIndexFile(indexFile, func(key uint64, offset, size uint32) error {
		if offset > 0 && size != TombstoneFileSize {
			levelDbWrite(db, key, offset, size)
		} else {
			levelDbDelete(db, key)
		}
		return nil
	})
}

func (m *LevelDbNeedleMap) Get(key uint64) (element *needle.NeedleValue, ok bool) {
	bytes := make([]byte, 8)
	util.Uint64toBytes(bytes, key)
	data, err := m.db.Get(bytes, nil)
	if err != nil || len(data) != 8 {
		return nil, false
	}
	offset := util.BytesToUint32(data[0:4])
	size := util.BytesToUint32(data[4:8])
	return &needle.NeedleValue{Key: needle.Key(key), Offset: offset, Size: size}, true
}

func (m *LevelDbNeedleMap) Put(key uint64, offset uint32, size uint32) error {
	var oldSize uint32
	if oldNeedle, ok := m.Get(key); ok {
		oldSize = oldNeedle.Size
	}
	m.logPut(key, oldSize, size)
	// write to index file first
	if err := m.appendToIndexFile(key, offset, size); err != nil {
		return fmt.Errorf("cannot write to indexfile %s: %v", m.indexFile.Name(), err)
	}
	return levelDbWrite(m.db, key, offset, size)
}

func levelDbWrite(db *leveldb.DB,
	key uint64, offset uint32, size uint32) error {
	bytes := make([]byte, 16)
	util.Uint64toBytes(bytes[0:8], key)
	util.Uint32toBytes(bytes[8:12], offset)
	util.Uint32toBytes(bytes[12:16], size)
	if err := db.Put(bytes[0:8], bytes[8:16], nil); err != nil {
		return fmt.Errorf("failed to write leveldb: %v", err)
	}
	return nil
}
func levelDbDelete(db *leveldb.DB, key uint64) error {
	bytes := make([]byte, 8)
	util.Uint64toBytes(bytes, key)
	return db.Delete(bytes, nil)
}

func (m *LevelDbNeedleMap) Delete(key uint64, offset uint32) error {
	if oldNeedle, ok := m.Get(key); ok {
		m.logDelete(oldNeedle.Size)
	}
	// write to index file first
	if err := m.appendToIndexFile(key, offset, TombstoneFileSize); err != nil {
		return err
	}
	return levelDbDelete(m.db, key)
}

func (m *LevelDbNeedleMap) Close() {
	m.db.Close()
}

func (m *LevelDbNeedleMap) Destroy() error {
	m.Close()
	os.Remove(m.indexFile.Name())
	return os.RemoveAll(m.dbFileName)
}
