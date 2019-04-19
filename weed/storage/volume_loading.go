package storage

import (
	"fmt"
	"os"
	"time"

	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

func loadVolumeWithoutIndex(dirname string, collection string, id needle.VolumeId, needleMapKind NeedleMapType) (v *Volume, e error) {
	v = &Volume{dir: dirname, Collection: collection, Id: id}
	v.SuperBlock = SuperBlock{}
	v.needleMapKind = needleMapKind
	e = v.load(false, false, needleMapKind, 0)
	return
}

func (v *Volume) load(alsoLoadIndex bool, createDatIfMissing bool, needleMapKind NeedleMapType, preallocate int64) error {
	var e error
	fileName := v.FileName()
	alreadyHasSuperBlock := false

	if exists, canRead, canWrite, modifiedTime, fileSize := checkFile(fileName + ".dat"); exists {
		if !canRead {
			return fmt.Errorf("cannot read Volume Data file %s.dat", fileName)
		}
		if canWrite {
			v.dataFile, e = os.OpenFile(fileName+".dat", os.O_RDWR|os.O_CREATE, 0644)
			v.lastModifiedTsSeconds = uint64(modifiedTime.Unix())
		} else {
			glog.V(0).Infoln("opening " + fileName + ".dat in READONLY mode")
			v.dataFile, e = os.Open(fileName + ".dat")
			v.readOnly = true
		}
		if fileSize >= _SuperBlockSize {
			alreadyHasSuperBlock = true
		}
	} else {
		if createDatIfMissing {
			v.dataFile, e = createVolumeFile(fileName+".dat", preallocate)
		} else {
			return fmt.Errorf("Volume Data file %s.dat does not exist.", fileName)
		}
	}

	if e != nil {
		if !os.IsPermission(e) {
			return fmt.Errorf("cannot load Volume Data %s.dat: %v", fileName, e)
		} else {
			return fmt.Errorf("load data file %s.dat: %v", fileName, e)
		}
	}

	if alreadyHasSuperBlock {
		e = v.readSuperBlock()
	} else {
		e = v.maybeWriteSuperBlock()
	}
	if e == nil && alsoLoadIndex {
		var indexFile *os.File
		if v.readOnly {
			glog.V(1).Infoln("open to read file", fileName+".idx")
			if indexFile, e = os.OpenFile(fileName+".idx", os.O_RDONLY, 0644); e != nil {
				return fmt.Errorf("cannot read Volume Index %s.idx: %v", fileName, e)
			}
		} else {
			glog.V(1).Infoln("open to write file", fileName+".idx")
			if indexFile, e = os.OpenFile(fileName+".idx", os.O_RDWR|os.O_CREATE, 0644); e != nil {
				return fmt.Errorf("cannot write Volume Index %s.idx: %v", fileName, e)
			}
		}
		if v.lastAppendAtNs, e = CheckVolumeDataIntegrity(v, indexFile); e != nil {
			v.readOnly = true
			glog.V(0).Infof("volumeDataIntegrityChecking failed %v", e)
		}
		switch needleMapKind {
		case NeedleMapInMemory:
			glog.V(0).Infoln("loading index", fileName+".idx", "to memory readonly", v.readOnly)
			if v.nm, e = LoadCompactNeedleMap(indexFile); e != nil {
				glog.V(0).Infof("loading index %s to memory error: %v", fileName+".idx", e)
			}
		case NeedleMapLevelDb:
			glog.V(0).Infoln("loading leveldb", fileName+".ldb")
			opts := &opt.Options{
				BlockCacheCapacity: 2 * 1024 * 1024, // default value is 8MiB
				WriteBuffer:        1 * 1024 * 1024, // default value is 4MiB
			}
			if v.nm, e = NewLevelDbNeedleMap(fileName+".ldb", indexFile, opts); e != nil {
				glog.V(0).Infof("loading leveldb %s error: %v", fileName+".ldb", e)
			}
		case NeedleMapLevelDbMedium:
			glog.V(0).Infoln("loading leveldb medium", fileName+".ldb")
			opts := &opt.Options{
				BlockCacheCapacity: 4 * 1024 * 1024, // default value is 8MiB
				WriteBuffer:        2 * 1024 * 1024, // default value is 4MiB
			}
			if v.nm, e = NewLevelDbNeedleMap(fileName+".ldb", indexFile, opts); e != nil {
				glog.V(0).Infof("loading leveldb %s error: %v", fileName+".ldb", e)
			}
		case NeedleMapLevelDbLarge:
			glog.V(0).Infoln("loading leveldb large", fileName+".ldb")
			opts := &opt.Options{
				BlockCacheCapacity: 8 * 1024 * 1024, // default value is 8MiB
				WriteBuffer:        4 * 1024 * 1024, // default value is 4MiB
			}
			if v.nm, e = NewLevelDbNeedleMap(fileName+".ldb", indexFile, opts); e != nil {
				glog.V(0).Infof("loading leveldb %s error: %v", fileName+".ldb", e)
			}
		}
	}

	return e
}

func checkFile(filename string) (exists, canRead, canWrite bool, modTime time.Time, fileSize int64) {
	exists = true
	fi, err := os.Stat(filename)
	if os.IsNotExist(err) {
		exists = false
		return
	}
	if fi.Mode()&0400 != 0 {
		canRead = true
	}
	if fi.Mode()&0200 != 0 {
		canWrite = true
	}
	modTime = fi.ModTime()
	fileSize = fi.Size()
	return
}
