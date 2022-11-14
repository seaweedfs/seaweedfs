package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/util"

	"github.com/syndtr/goleveldb/leveldb"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// mark it every watermarkBatchSize operations
const watermarkBatchSize = 10000

var watermarkKey = []byte("idx_entry_watermark")

type LevelDbNeedleMap struct {
	baseNeedleMapper
	dbFileName    string
	db            *leveldb.DB
	ldbOpts       *opt.Options
	ldbAccessLock sync.RWMutex
	exitChan      chan bool
	// no need to use atomic
	accessFlag  int64
	ldbTimeout  int64
	recordCount uint64
}

func NewLevelDbNeedleMap(dbFileName string, indexFile *os.File, opts *opt.Options, ldbTimeout int64) (m *LevelDbNeedleMap, err error) {
	m = &LevelDbNeedleMap{dbFileName: dbFileName}
	m.indexFile = indexFile
	if !isLevelDbFresh(dbFileName, indexFile) {
		glog.V(1).Infof("Start to Generate %s from %s", dbFileName, indexFile.Name())
		generateLevelDbFile(dbFileName, indexFile)
		glog.V(1).Infof("Finished Generating %s from %s", dbFileName, indexFile.Name())
	}
	if stat, err := indexFile.Stat(); err != nil {
		glog.Fatalf("stat file %s: %v", indexFile.Name(), err)
	} else {
		m.indexFileOffset = stat.Size()
	}
	glog.V(1).Infof("Opening %s...", dbFileName)

	if m.ldbTimeout == 0 {
		if m.db, err = leveldb.OpenFile(dbFileName, opts); err != nil {
			if errors.IsCorrupted(err) {
				m.db, err = leveldb.RecoverFile(dbFileName, opts)
			}
			if err != nil {
				return
			}
		}
		glog.V(0).Infof("Loading %s... , watermark: %d", dbFileName, getWatermark(m.db))
		m.recordCount = uint64(m.indexFileOffset / NeedleMapEntrySize)
		watermark := (m.recordCount / watermarkBatchSize) * watermarkBatchSize
		err = setWatermark(m.db, watermark)
		if err != nil {
			glog.Fatalf("set watermark for %s error: %s\n", dbFileName, err)
			return
		}
	}
	mm, indexLoadError := newNeedleMapMetricFromIndexFile(indexFile)
	if indexLoadError != nil {
		return nil, indexLoadError
	}
	m.mapMetric = *mm
	m.ldbTimeout = ldbTimeout
	if m.ldbTimeout > 0 {
		m.ldbOpts = opts
		m.exitChan = make(chan bool, 1)
		m.accessFlag = 0
		go lazyLoadingRoutine(m)
	}
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

	watermark := getWatermark(db)
	if stat, err := indexFile.Stat(); err != nil {
		glog.Fatalf("stat file %s: %v", indexFile.Name(), err)
		return err
	} else {
		if watermark*NeedleMapEntrySize > uint64(stat.Size()) {
			glog.Warningf("wrong watermark %d for filesize %d", watermark, stat.Size())
		}
		glog.V(0).Infof("generateLevelDbFile %s, watermark %d, num of entries:%d", dbFileName, watermark, (uint64(stat.Size())-watermark*NeedleMapEntrySize)/NeedleMapEntrySize)
	}
	return idx.WalkIndexFile(indexFile, watermark, func(key NeedleId, offset Offset, size Size) error {
		if !offset.IsZero() && size.IsValid() {
			levelDbWrite(db, key, offset, size, false, 0)
		} else {
			levelDbDelete(db, key)
		}
		return nil
	})
}

func (m *LevelDbNeedleMap) Get(key NeedleId) (element *needle_map.NeedleValue, ok bool) {
	bytes := make([]byte, NeedleIdSize)
	if m.ldbTimeout > 0 {
		m.ldbAccessLock.RLock()
		defer m.ldbAccessLock.RUnlock()
		loadErr := reloadLdb(m)
		if loadErr != nil {
			return nil, false
		}
	}
	NeedleIdToBytes(bytes[0:NeedleIdSize], key)
	data, err := m.db.Get(bytes, nil)
	if err != nil || len(data) != OffsetSize+SizeSize {
		return nil, false
	}
	offset := BytesToOffset(data[0:OffsetSize])
	size := BytesToSize(data[OffsetSize : OffsetSize+SizeSize])
	return &needle_map.NeedleValue{Key: key, Offset: offset, Size: size}, true
}

func (m *LevelDbNeedleMap) Put(key NeedleId, offset Offset, size Size) error {
	var oldSize Size
	var watermark uint64
	if m.ldbTimeout > 0 {
		m.ldbAccessLock.RLock()
		defer m.ldbAccessLock.RUnlock()
		loadErr := reloadLdb(m)
		if loadErr != nil {
			return loadErr
		}
	}
	if oldNeedle, ok := m.Get(key); ok {
		oldSize = oldNeedle.Size
	}
	m.logPut(key, oldSize, size)
	// write to index file first
	if err := m.appendToIndexFile(key, offset, size); err != nil {
		return fmt.Errorf("cannot write to indexfile %s: %v", m.indexFile.Name(), err)
	}
	m.recordCount++
	if m.recordCount%watermarkBatchSize != 0 {
		watermark = 0
	} else {
		watermark = (m.recordCount / watermarkBatchSize) * watermarkBatchSize
		glog.V(1).Infof("put cnt:%d for %s,watermark: %d", m.recordCount, m.dbFileName, watermark)
	}
	return levelDbWrite(m.db, key, offset, size, watermark == 0, watermark)
}

func getWatermark(db *leveldb.DB) uint64 {
	data, err := db.Get(watermarkKey, nil)
	if err != nil || len(data) != 8 {
		glog.V(1).Infof("read previous watermark from db: %v, %d", err, len(data))
		return 0
	}
	return util.BytesToUint64(data)
}

func setWatermark(db *leveldb.DB, watermark uint64) error {
	glog.V(3).Infof("set watermark %d", watermark)
	var wmBytes = make([]byte, 8)
	util.Uint64toBytes(wmBytes, watermark)
	if err := db.Put(watermarkKey, wmBytes, nil); err != nil {
		return fmt.Errorf("failed to setWatermark: %v", err)
	}
	return nil
}

func levelDbWrite(db *leveldb.DB, key NeedleId, offset Offset, size Size, updateWatermark bool, watermark uint64) error {

	bytes := needle_map.ToBytes(key, offset, size)

	if err := db.Put(bytes[0:NeedleIdSize], bytes[NeedleIdSize:NeedleIdSize+OffsetSize+SizeSize], nil); err != nil {
		return fmt.Errorf("failed to write leveldb: %v", err)
	}
	// set watermark
	if updateWatermark {
		return setWatermark(db, watermark)
	}
	return nil
}

func levelDbDelete(db *leveldb.DB, key NeedleId) error {
	bytes := make([]byte, NeedleIdSize)
	NeedleIdToBytes(bytes, key)
	return db.Delete(bytes, nil)
}

func (m *LevelDbNeedleMap) Delete(key NeedleId, offset Offset) error {
	var watermark uint64
	if m.ldbTimeout > 0 {
		m.ldbAccessLock.RLock()
		defer m.ldbAccessLock.RUnlock()
		loadErr := reloadLdb(m)
		if loadErr != nil {
			return loadErr
		}
	}
	oldNeedle, found := m.Get(key)
	if !found || oldNeedle.Size.IsDeleted() {
		return nil
	}
	m.logDelete(oldNeedle.Size)

	// write to index file first
	if err := m.appendToIndexFile(key, offset, TombstoneFileSize); err != nil {
		return err
	}
	m.recordCount++
	if m.recordCount%watermarkBatchSize != 0 {
		watermark = 0
	} else {
		watermark = (m.recordCount / watermarkBatchSize) * watermarkBatchSize
	}
	return levelDbWrite(m.db, key, oldNeedle.Offset, -oldNeedle.Size, watermark == 0, watermark)
}

func (m *LevelDbNeedleMap) Close() {
	if m.indexFile != nil {
		indexFileName := m.indexFile.Name()
		if err := m.indexFile.Sync(); err != nil {
			glog.Warningf("sync file %s failed: %v", indexFileName, err)
		}
		if err := m.indexFile.Close(); err != nil {
			glog.Warningf("close index file %s failed: %v", indexFileName, err)
		}
	}

	if m.db != nil {
		if err := m.db.Close(); err != nil {
			glog.Warningf("close levelDB failed: %v", err)
		}
	}
	if m.ldbTimeout > 0 {
		m.exitChan <- true
	}
}

func (m *LevelDbNeedleMap) Destroy() error {
	m.Close()
	os.Remove(m.indexFile.Name())
	return os.RemoveAll(m.dbFileName)
}

func (m *LevelDbNeedleMap) UpdateNeedleMap(v *Volume, indexFile *os.File, opts *opt.Options, ldbTimeout int64) error {
	if v.nm != nil {
		v.nm.Close()
		v.nm = nil
	}
	defer func() {
		if v.tmpNm != nil {
			v.tmpNm.Close()
			v.tmpNm = nil
		}
	}()
	levelDbFile := v.FileName(".ldb")
	m.indexFile = indexFile
	err := os.RemoveAll(levelDbFile)
	if err != nil {
		return err
	}
	if err = os.Rename(v.FileName(".cpldb"), levelDbFile); err != nil {
		return fmt.Errorf("rename %s: %v", levelDbFile, err)
	}

	db, err := leveldb.OpenFile(levelDbFile, opts)
	if err != nil {
		if errors.IsCorrupted(err) {
			db, err = leveldb.RecoverFile(levelDbFile, opts)
		}
		if err != nil {
			return err
		}
	}
	m.db = db

	stat, e := indexFile.Stat()
	if e != nil {
		glog.Fatalf("stat file %s: %v", indexFile.Name(), e)
		return e
	}
	m.indexFileOffset = stat.Size()
	m.recordCount = uint64(stat.Size() / NeedleMapEntrySize)

	//set watermark
	watermark := (m.recordCount / watermarkBatchSize) * watermarkBatchSize
	err = setWatermark(db, uint64(watermark))
	if err != nil {
		glog.Fatalf("setting watermark failed %s: %v", indexFile.Name(), err)
		return err
	}
	v.nm = m
	v.tmpNm = nil
	m.ldbTimeout = ldbTimeout
	if m.ldbTimeout > 0 {
		m.ldbOpts = opts
		m.exitChan = make(chan bool, 1)
		m.accessFlag = 0
		go lazyLoadingRoutine(m)
	}
	return e
}

func (m *LevelDbNeedleMap) DoOffsetLoading(v *Volume, indexFile *os.File, startFrom uint64) (err error) {
	glog.V(0).Infof("loading idx to leveldb from offset %d for file: %s", startFrom, indexFile.Name())
	dbFileName := v.FileName(".cpldb")
	db, dbErr := leveldb.OpenFile(dbFileName, nil)
	defer func() {
		if dbErr == nil {
			db.Close()
		}
		if err != nil {
			os.RemoveAll(dbFileName)
		}

	}()
	if dbErr != nil {
		if errors.IsCorrupted(err) {
			db, dbErr = leveldb.RecoverFile(dbFileName, nil)
		}
		if dbErr != nil {
			return dbErr
		}
	}

	err = idx.WalkIndexFile(indexFile, startFrom, func(key NeedleId, offset Offset, size Size) (e error) {
		m.mapMetric.FileCounter++
		bytes := make([]byte, NeedleIdSize)
		NeedleIdToBytes(bytes[0:NeedleIdSize], key)
		// fresh loading
		if startFrom == 0 {
			m.mapMetric.FileByteCounter += uint64(size)
			e = levelDbWrite(db, key, offset, size, false, 0)
			return e
		}
		// increment loading
		data, err := db.Get(bytes, nil)
		if err != nil {
			if !strings.Contains(strings.ToLower(err.Error()), "not found") {
				// unexpected error
				return err
			}
			// new needle, unlikely happen
			m.mapMetric.FileByteCounter += uint64(size)
			e = levelDbWrite(db, key, offset, size, false, 0)
		} else {
			// needle is found
			oldSize := BytesToSize(data[OffsetSize : OffsetSize+SizeSize])
			oldOffset := BytesToOffset(data[0:OffsetSize])
			if !offset.IsZero() && size.IsValid() {
				// updated needle
				m.mapMetric.FileByteCounter += uint64(size)
				if !oldOffset.IsZero() && oldSize.IsValid() {
					m.mapMetric.DeletionCounter++
					m.mapMetric.DeletionByteCounter += uint64(oldSize)
				}
				e = levelDbWrite(db, key, offset, size, false, 0)
			} else {
				// deleted needle
				m.mapMetric.DeletionCounter++
				m.mapMetric.DeletionByteCounter += uint64(oldSize)
				e = levelDbDelete(db, key)
			}
		}
		return e
	})
	return err
}

func reloadLdb(m *LevelDbNeedleMap) (err error) {
	if m.db != nil {
		return nil
	}
	glog.V(1).Infof("reloading leveldb %s", m.dbFileName)
	m.accessFlag = 1
	if m.db, err = leveldb.OpenFile(m.dbFileName, m.ldbOpts); err != nil {
		if errors.IsCorrupted(err) {
			m.db, err = leveldb.RecoverFile(m.dbFileName, m.ldbOpts)
		}
		if err != nil {
			glog.Fatalf("RecoverFile %s failed:%v", m.dbFileName, err)
			return err
		}
	}
	return nil
}

func unloadLdb(m *LevelDbNeedleMap) (err error) {
	m.ldbAccessLock.Lock()
	defer m.ldbAccessLock.Unlock()
	if m.db != nil {
		glog.V(1).Infof("reached max idle count, unload leveldb, %s", m.dbFileName)
		m.db.Close()
		m.db = nil
	}
	return nil
}

func lazyLoadingRoutine(m *LevelDbNeedleMap) (err error) {
	glog.V(1).Infof("lazyLoadingRoutine %s", m.dbFileName)
	var accessRecord int64
	accessRecord = 1
	for {
		select {
		case exit := <-m.exitChan:
			if exit {
				glog.V(1).Infof("exit from lazyLoadingRoutine")
				return nil
			}
		case <-time.After(time.Hour * 1):
			glog.V(1).Infof("timeout %s", m.dbFileName)
			if m.accessFlag == 0 {
				accessRecord++
				glog.V(1).Infof("accessRecord++")
				if accessRecord >= m.ldbTimeout {
					unloadLdb(m)
				}
			} else {
				glog.V(1).Infof("reset accessRecord %s", m.dbFileName)
				// reset accessRecord
				accessRecord = 0
			}
			continue
		}
	}
}
