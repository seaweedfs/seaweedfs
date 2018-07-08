package storage

import (
	"fmt"
	"os"

	"github.com/boltdb/bolt"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	. "github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type BoltDbNeedleMap struct {
	dbFileName string
	db         *bolt.DB
	baseNeedleMapper
}

var boltdbBucket = []byte("weed")

// TODO avoid using btree to count deletions.
func NewBoltDbNeedleMap(dbFileName string, indexFile *os.File) (m *BoltDbNeedleMap, err error) {
	m = &BoltDbNeedleMap{dbFileName: dbFileName}
	m.indexFile = indexFile
	if !isBoltDbFresh(dbFileName, indexFile) {
		glog.V(1).Infof("Start to Generate %s from %s", dbFileName, indexFile.Name())
		generateBoltDbFile(dbFileName, indexFile)
		glog.V(1).Infof("Finished Generating %s from %s", dbFileName, indexFile.Name())
	}
	glog.V(1).Infof("Opening %s...", dbFileName)
	if m.db, err = bolt.Open(dbFileName, 0644, nil); err != nil {
		return
	}
	glog.V(1).Infof("Loading %s...", indexFile.Name())
	mm, indexLoadError := newNeedleMapMetricFromIndexFile(indexFile)
	if indexLoadError != nil {
		return nil, indexLoadError
	}
	m.mapMetric = *mm
	return
}

func isBoltDbFresh(dbFileName string, indexFile *os.File) bool {
	// normally we always write to index file first
	dbLogFile, err := os.Open(dbFileName)
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

func generateBoltDbFile(dbFileName string, indexFile *os.File) error {
	db, err := bolt.Open(dbFileName, 0644, nil)
	if err != nil {
		return err
	}
	defer db.Close()
	return WalkIndexFile(indexFile, func(key NeedleId, offset Offset, size uint32) error {
		if offset > 0 && size != TombstoneFileSize {
			boltDbWrite(db, key, offset, size)
		} else {
			boltDbDelete(db, key)
		}
		return nil
	})
}

func (m *BoltDbNeedleMap) Get(key NeedleId) (element *needle.NeedleValue, ok bool) {
	var offset Offset
	var size uint32
	bytes := make([]byte, NeedleIdSize)
	NeedleIdToBytes(bytes, key)
	err := m.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(boltdbBucket)
		if bucket == nil {
			return fmt.Errorf("Bucket %q not found!", boltdbBucket)
		}

		data := bucket.Get(bytes)

		if len(data) != OffsetSize+SizeSize {
			glog.V(0).Infof("wrong data length: %d", len(data))
			return fmt.Errorf("wrong data length: %d", len(data))
		}

		offset = BytesToOffset(data[0:OffsetSize])
		size = util.BytesToUint32(data[OffsetSize:OffsetSize+SizeSize])

		return nil
	})

	if err != nil {
		return nil, false
	}
	return &needle.NeedleValue{Key: NeedleId(key), Offset: offset, Size: size}, true
}

func (m *BoltDbNeedleMap) Put(key NeedleId, offset Offset, size uint32) error {
	var oldSize uint32
	if oldNeedle, ok := m.Get(key); ok {
		oldSize = oldNeedle.Size
	}
	m.logPut(key, oldSize, size)
	// write to index file first
	if err := m.appendToIndexFile(key, offset, size); err != nil {
		return fmt.Errorf("cannot write to indexfile %s: %v", m.indexFile.Name(), err)
	}
	return boltDbWrite(m.db, key, offset, size)
}

func boltDbWrite(db *bolt.DB,
	key NeedleId, offset Offset, size uint32) error {

	bytes := make([]byte, NeedleIdSize+OffsetSize+SizeSize)
	NeedleIdToBytes(bytes[0:NeedleIdSize], key)
	OffsetToBytes(bytes[NeedleIdSize:NeedleIdSize+OffsetSize], offset)
	util.Uint32toBytes(bytes[NeedleIdSize+OffsetSize:NeedleIdSize+OffsetSize+SizeSize], size)

	return db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(boltdbBucket)
		if err != nil {
			return err
		}

		err = bucket.Put(bytes[0:NeedleIdSize], bytes[NeedleIdSize:NeedleIdSize+OffsetSize+SizeSize])
		if err != nil {
			return err
		}
		return nil
	})
}
func boltDbDelete(db *bolt.DB, key NeedleId) error {
	bytes := make([]byte, NeedleIdSize)
	NeedleIdToBytes(bytes, key)
	return db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(boltdbBucket)
		if err != nil {
			return err
		}

		err = bucket.Delete(bytes)
		if err != nil {
			return err
		}
		return nil
	})
}

func (m *BoltDbNeedleMap) Delete(key NeedleId, offset Offset) error {
	if oldNeedle, ok := m.Get(key); ok {
		m.logDelete(oldNeedle.Size)
	}
	// write to index file first
	if err := m.appendToIndexFile(key, offset, TombstoneFileSize); err != nil {
		return err
	}
	return boltDbDelete(m.db, key)
}

func (m *BoltDbNeedleMap) Close() {
	m.indexFile.Close()
	m.db.Close()
}

func (m *BoltDbNeedleMap) Destroy() error {
	m.Close()
	os.Remove(m.indexFile.Name())
	return os.Remove(m.dbFileName)
}
