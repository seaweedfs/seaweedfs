package storage

import (
	"fmt"
	"os"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

func loadVolumeWithoutIndex(dirname string, collection string, id VolumeId, needleMapKind NeedleMapType) (v *Volume, e error) {
	v = &Volume{dir: dirname, Collection: collection, Id: id}
	v.SuperBlock = SuperBlock{}
	v.needleMapKind = needleMapKind
	e = v.load(false, false, needleMapKind)
	return
}

func (v *Volume) load(alsoLoadIndex bool, createDatIfMissing bool, needleMapKind NeedleMapType) error {
	var e error
	fileName := v.FileName()

	if exists, canRead, canWrite, modifiedTime := checkFile(fileName + ".dat"); exists {
		if !canRead {
			return fmt.Errorf("cannot read Volume Data file %s.dat", fileName)
		}
		if canWrite {
			v.dataFile, e = os.OpenFile(fileName+".dat", os.O_RDWR|os.O_CREATE, 0644)
			v.lastModifiedTime = uint64(modifiedTime.Unix())
		} else {
			glog.V(0).Infoln("opening " + fileName + ".dat in READONLY mode")
			v.dataFile, e = os.Open(fileName + ".dat")
			v.readOnly = true
		}
	} else {
		if createDatIfMissing {
			v.dataFile, e = os.OpenFile(fileName+".dat", os.O_RDWR|os.O_CREATE, 0644)
		} else {
			return fmt.Errorf("Volume Data file %s.dat does not exist.", fileName)
		}
	}

	if e != nil {
		if !os.IsPermission(e) {
			return fmt.Errorf("cannot load Volume Data %s.dat: %v", fileName, e)
		}
	}

	if v.ReplicaPlacement == nil {
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
		if e = CheckVolumeDataIntegrity(v, indexFile); e != nil {
			v.readOnly = true
			glog.V(0).Infof("volumeDataIntegrityChecking failed %v", e)
		}
		switch needleMapKind {
		case NeedleMapInMemory:
			glog.V(0).Infoln("loading index file", fileName+".idx", "readonly", v.readOnly)
			if v.nm, e = LoadNeedleMap(indexFile); e != nil {
				glog.V(0).Infof("loading index %s error: %v", fileName+".idx", e)
			}
		case NeedleMapLevelDb:
			glog.V(0).Infoln("loading leveldb file", fileName+".ldb")
			if v.nm, e = NewLevelDbNeedleMap(fileName+".ldb", indexFile); e != nil {
				glog.V(0).Infof("loading leveldb %s error: %v", fileName+".ldb", e)
			}
		case NeedleMapBoltDb:
			glog.V(0).Infoln("loading boltdb file", fileName+".bdb")
			if v.nm, e = NewBoltDbNeedleMap(fileName+".bdb", indexFile); e != nil {
				glog.V(0).Infof("loading boltdb %s error: %v", fileName+".bdb", e)
			}
		}
	}
	return e
}

func checkFile(filename string) (exists, canRead, canWrite bool, modTime time.Time) {
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
	return
}
