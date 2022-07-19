package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/chrislusf/seaweedfs/weed/storage/idx"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/chrislusf/seaweedfs/weed/util"

	"github.com/syndtr/goleveldb/leveldb"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage/needle_map"
	. "github.com/chrislusf/seaweedfs/weed/storage/types"
)

//use "2 >> 16" to reduce cpu cost
const milestoneCnt = 40
const milestoneKey = 0xffffffffffffffff - 1

type LevelDbNeedleMap struct {
	baseNeedleMapper
	dbFileName string
	db         *leveldb.DB
	recordNum  uint64
}

func NewLevelDbNeedleMap(dbFileName string, indexFile *os.File, opts *opt.Options) (m *LevelDbNeedleMap, err error) {
	glog.V(0).Infof("NewLevelDbNeedleMap pocessing %s...", indexFile.Name())
	db, errd := leveldb.OpenFile(dbFileName, opts)
	glog.V(0).Infof("begain %v  %s %d", errd, dbFileName, getMileStone(db))
	db.Close()

	m = &LevelDbNeedleMap{dbFileName: dbFileName}
	m.indexFile = indexFile
	if !isLevelDbFresh(dbFileName, indexFile) {
		glog.V(0).Infof("Start to Generate %s from %s", dbFileName, indexFile.Name())
		generateLevelDbFile(dbFileName, indexFile)
		glog.V(0).Infof("Finished Generating %s from %s", dbFileName, indexFile.Name())
	}
	if stat, err := indexFile.Stat(); err != nil {
		glog.Fatalf("stat file %s: %v", indexFile.Name(), err)
	} else {
		m.indexFileOffset = stat.Size()
	}
	glog.V(0).Infof("Opening %s...", dbFileName)

	if m.db, err = leveldb.OpenFile(dbFileName, opts); err != nil {
		if errors.IsCorrupted(err) {
			m.db, err = leveldb.RecoverFile(dbFileName, opts)
		}
		if err != nil {
			return
		}
	}
	glog.V(0).Infof("getMileStone %s : %d", dbFileName, getMileStone(m.db))
	m.recordNum = uint64(m.indexFileOffset / types.NeedleMapEntrySize)
	milestone := (m.recordNum / milestoneCnt) * milestoneCnt
	err = setMileStone(m.db, milestone)
	if err != nil {
		return
	}
	glog.V(0).Infof("Loading %s... %d %d", indexFile.Name(), milestone, getMileStone(m.db))
	mm, indexLoadError := newNeedleMapMetricFromIndexFile(indexFile)
	if indexLoadError != nil {
		return nil, indexLoadError
	}
	glog.V(0).Infof("finish Loading %s...", indexFile.Name())
	m.mapMetric = *mm
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

	milestone := getMileStone(db)
	if stat, err := indexFile.Stat(); err != nil {
		glog.Fatalf("stat file %s: %v", indexFile.Name(), err)
		return err
	} else {
		if milestone*types.NeedleMapEntrySize > uint64(stat.Size()) {
			glog.Warningf("wrong milestone %d for filesize %d, set milestone to 0", milestone, stat.Size())
			milestone = 0
		}
		glog.V(0).Infof("generateLevelDbFile %s, milestone %d, num of entries:%d", dbFileName, milestone, (uint64(stat.Size())-milestone*types.NeedleMapEntrySize)/types.NeedleMapEntrySize)
	}
	return idx.WalkIndexFileIncrent(indexFile, milestone, func(key NeedleId, offset Offset, size Size) error {
		if !offset.IsZero() && size.IsValid() {
			levelDbWrite(db, key, offset, size, 0)
		} else {
			levelDbDelete(db, key)
		}
		return nil
	})
}

func (m *LevelDbNeedleMap) Get(key NeedleId) (element *needle_map.NeedleValue, ok bool) {
	bytes := make([]byte, NeedleIdSize)
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
	var milestone uint64
	if oldNeedle, ok := m.Get(key); ok {
		oldSize = oldNeedle.Size
	}
	m.logPut(key, oldSize, size)
	// write to index file first
	if err := m.appendToIndexFile(key, offset, size); err != nil {
		return fmt.Errorf("cannot write to indexfile %s: %v", m.indexFile.Name(), err)
	}
	//atomic.AddUint64(&m.recordNum, 1)
	//milestone = atomic.LoadUint64(&m.recordNum)
	m.recordNum++
	if m.recordNum%milestoneCnt != 0 {
		milestone = 0
	} else {
		milestone = (m.recordNum / milestoneCnt) * milestoneCnt
		glog.V(0).Infof("put cnt:%d milestone:%s %d", m.recordNum, m.dbFileName, milestone)
	}
	return levelDbWrite(m.db, key, offset, size, milestone)
}

func getMileStone(db *leveldb.DB) uint64 {
	var mskBytes = make([]byte, 8)
	util.Uint64toBytes(mskBytes, milestoneKey)
	data, err := db.Get(mskBytes, nil)
	if err != nil || len(data) != 8 {
		glog.Warningf("get milestone from db error: %v, %d", err, len(data))
		if !strings.Contains(strings.ToLower(err.Error()), "not found") {
			err = setMileStone(db, 0)
			if err != nil {
				glog.Errorf("failed to set milestone: %v", err)
			}
		}

		return 0
	}
	return util.BytesToUint64(data)
}

func setMileStone(db *leveldb.DB, milestone uint64) error {
	glog.V(0).Infof("set milestone %d", milestone)
	var mskBytes = make([]byte, 8)
	util.Uint64toBytes(mskBytes, milestoneKey)
	var msBytes = make([]byte, 8)
	util.Uint64toBytes(msBytes, milestone)
	if err := db.Put(mskBytes, msBytes, nil); err != nil {
		return fmt.Errorf("failed to setMileStone: %v", err)
	}
	glog.V(0).Infof("ssset milestone %d, %d", milestone, getMileStone(db))
	return nil
}

func levelDbWrite(db *leveldb.DB, key NeedleId, offset Offset, size Size, milestone uint64) error {

	bytes := needle_map.ToBytes(key, offset, size)

	if err := db.Put(bytes[0:NeedleIdSize], bytes[NeedleIdSize:NeedleIdSize+OffsetSize+SizeSize], nil); err != nil {
		return fmt.Errorf("failed to write leveldb: %v", err)
	}
	// set milestone
	if milestone != 0 {
		glog.V(0).Infof("actually set milestone %d", milestone)
		return setMileStone(db, milestone)
	}
	return nil
}
func levelDbDelete(db *leveldb.DB, key NeedleId) error {
	bytes := make([]byte, NeedleIdSize)
	NeedleIdToBytes(bytes, key)
	return db.Delete(bytes, nil)
}

func (m *LevelDbNeedleMap) Delete(key NeedleId, offset Offset) error {
	var milestone uint64
	oldNeedle, found := m.Get(key)
	if !found || oldNeedle.Size.IsDeleted() {
		return nil
	}
	m.logDelete(oldNeedle.Size)

	// write to index file first
	if err := m.appendToIndexFile(key, offset, TombstoneFileSize); err != nil {
		return err
	}
	//atomic.AddUint64(&m.recordNum, 1)
	//milestone = atomic.LoadUint64(&m.recordNum)
	m.recordNum++
	if m.recordNum%milestoneCnt != 0 {
		milestone = 0
	} else {
		milestone = (m.recordNum / milestoneCnt) * milestoneCnt
	}
	glog.V(0).Infof("delete cnt:%d milestone:%s %d", m.recordNum, m.dbFileName, milestone)
	return levelDbWrite(m.db, key, oldNeedle.Offset, -oldNeedle.Size, milestone)
}

func (m *LevelDbNeedleMap) Close() {
	indexFileName := m.indexFile.Name()
	if err := m.indexFile.Sync(); err != nil {
		glog.Warningf("sync file %s failed: %v", indexFileName, err)
	}
	if err := m.indexFile.Close(); err != nil {
		glog.Warningf("close index file %s failed: %v", indexFileName, err)
	}

	if m.db != nil {
		if err := m.db.Close(); err != nil {
			glog.Warningf("close levelDB failed: %v", err)
		}
	}
}

func (m *LevelDbNeedleMap) Destroy() error {
	m.Close()
	os.Remove(m.indexFile.Name())
	return os.RemoveAll(m.dbFileName)
}
