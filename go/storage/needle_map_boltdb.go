package storage

import (
	"fmt"
	"os"

	"github.com/boltdb/bolt"

	"github.com/chrislusf/seaweedfs/go/glog"
	"github.com/chrislusf/seaweedfs/go/util"
)

type BoltDbNeedleMap struct {
	dbFileName string
	db         *bolt.DB
	baseNeedleMapper
}

var boltdbBucket = []byte("weed")

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
	nm, indexLoadError := LoadNeedleMap(indexFile)
	if indexLoadError != nil {
		return nil, indexLoadError
	}
	m.mapMetric = nm.mapMetric
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
	return WalkIndexFile(indexFile, func(key uint64, offset, size uint32) error {
		if offset > 0 {
			boltDbWrite(db, key, offset, size)
		} else {
			boltDbDelete(db, key)
		}
		return nil
	})
}

func (m *BoltDbNeedleMap) Get(key uint64) (element *NeedleValue, ok bool) {
	bytes := make([]byte, 8)
	var data []byte
	util.Uint64toBytes(bytes, key)
	err := m.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(boltdbBucket)
		if bucket == nil {
			return fmt.Errorf("Bucket %q not found!", boltdbBucket)
		}

		data = bucket.Get(bytes)
		return nil
	})

	if err != nil || len(data) != 8 {
		return nil, false
	}
	offset := util.BytesToUint32(data[0:4])
	size := util.BytesToUint32(data[4:8])
	return &NeedleValue{Key: Key(key), Offset: offset, Size: size}, true
}

func (m *BoltDbNeedleMap) Put(key uint64, offset uint32, size uint32) error {
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
	key uint64, offset uint32, size uint32) error {
	bytes := make([]byte, 16)
	util.Uint64toBytes(bytes[0:8], key)
	util.Uint32toBytes(bytes[8:12], offset)
	util.Uint32toBytes(bytes[12:16], size)
	return db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(boltdbBucket)
		if err != nil {
			return err
		}

		err = bucket.Put(bytes[0:8], bytes[8:16])
		if err != nil {
			return err
		}
		return nil
	})
}
func boltDbDelete(db *bolt.DB, key uint64) error {
	bytes := make([]byte, 8)
	util.Uint64toBytes(bytes, key)
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

func (m *BoltDbNeedleMap) Delete(key uint64) error {
	if oldNeedle, ok := m.Get(key); ok {
		m.logDelete(oldNeedle.Size)
	}
	// write to index file first
	if err := m.appendToIndexFile(key, 0, 0); err != nil {
		return err
	}
	return boltDbDelete(m.db, key)
}

func (m *BoltDbNeedleMap) Close() {
	m.db.Close()
}

func (m *BoltDbNeedleMap) Destroy() error {
	m.Close()
	os.Remove(m.indexFile.Name())
	return os.Remove(m.dbFileName)
}
