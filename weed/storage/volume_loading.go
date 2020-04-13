package storage

import (
	"fmt"
	"os"

	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/storage/backend"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func loadVolumeWithoutIndex(dirname string, collection string, id needle.VolumeId, needleMapKind NeedleMapType) (v *Volume, err error) {
	v = &Volume{dir: dirname, Collection: collection, Id: id}
	v.SuperBlock = super_block.SuperBlock{}
	v.needleMapKind = needleMapKind
	err = v.load(false, false, needleMapKind, 0)
	return
}

func (v *Volume) load(alsoLoadIndex bool, createDatIfMissing bool, needleMapKind NeedleMapType, preallocate int64) (err error) {
	fileName := v.FileName()
	alreadyHasSuperBlock := false

	hasVolumeInfoFile := v.maybeLoadVolumeInfo() && v.volumeInfo.Version != 0

	if v.HasRemoteFile() {
		v.noWriteCanDelete = true
		v.noWriteOrDelete = false
		glog.V(0).Infof("loading volume %d from remote %v", v.Id, v.volumeInfo.Files)
		v.LoadRemoteFile()
		alreadyHasSuperBlock = true
	} else if exists, canRead, canWrite, modifiedTime, fileSize := util.CheckFile(fileName + ".dat"); exists {
		// open dat file
		if !canRead {
			return fmt.Errorf("cannot read Volume Data file %s.dat", fileName)
		}
		var dataFile *os.File
		if canWrite {
			dataFile, err = os.OpenFile(fileName+".dat", os.O_RDWR|os.O_CREATE, 0644)
		} else {
			glog.V(0).Infoln("opening " + fileName + ".dat in READONLY mode")
			dataFile, err = os.Open(fileName + ".dat")
			v.noWriteOrDelete = true
		}
		v.lastModifiedTsSeconds = uint64(modifiedTime.Unix())
		if fileSize >= super_block.SuperBlockSize {
			alreadyHasSuperBlock = true
		}
		v.DataBackend = backend.NewDiskFile(dataFile)
	} else {
		if createDatIfMissing {
			v.DataBackend, err = backend.CreateVolumeFile(fileName+".dat", preallocate, v.MemoryMapMaxSizeMb)
		} else {
			return fmt.Errorf("Volume Data file %s.dat does not exist.", fileName)
		}
	}

	if err != nil {
		if !os.IsPermission(err) {
			return fmt.Errorf("cannot load Volume Data %s.dat: %v", fileName, err)
		} else {
			return fmt.Errorf("load data file %s.dat: %v", fileName, err)
		}
	}

	if alreadyHasSuperBlock {
		err = v.readSuperBlock()
	} else {
		if !v.SuperBlock.Initialized() {
			return fmt.Errorf("volume %s.dat not initialized", fileName)
		}
		err = v.maybeWriteSuperBlock()
	}
	if err == nil && alsoLoadIndex {
		var indexFile *os.File
		if v.noWriteOrDelete {
			glog.V(0).Infoln("open to read file", fileName+".idx")
			if indexFile, err = os.OpenFile(fileName+".idx", os.O_RDONLY, 0644); err != nil {
				return fmt.Errorf("cannot read Volume Index %s.idx: %v", fileName, err)
			}
		} else {
			glog.V(1).Infoln("open to write file", fileName+".idx")
			if indexFile, err = os.OpenFile(fileName+".idx", os.O_RDWR|os.O_CREATE, 0644); err != nil {
				return fmt.Errorf("cannot write Volume Index %s.idx: %v", fileName, err)
			}
		}
		if v.lastAppendAtNs, err = CheckVolumeDataIntegrity(v, indexFile); err != nil {
			v.noWriteOrDelete = true
			glog.V(0).Infof("volumeDataIntegrityChecking failed %v", err)
		}

		if v.IsReadOnly() {
			if v.nm, err = NewSortedFileNeedleMap(fileName, indexFile); err != nil {
				glog.V(0).Infof("loading sorted db %s error: %v", fileName+".sdx", err)
			}
		} else {
			switch needleMapKind {
			case NeedleMapInMemory:
				glog.V(0).Infoln("loading index", fileName+".idx", "to memory")
				if v.nm, err = LoadCompactNeedleMap(indexFile); err != nil {
					glog.V(0).Infof("loading index %s to memory error: %v", fileName+".idx", err)
				}
			case NeedleMapLevelDb:
				glog.V(0).Infoln("loading leveldb", fileName+".ldb")
				opts := &opt.Options{
					BlockCacheCapacity:            2 * 1024 * 1024, // default value is 8MiB
					WriteBuffer:                   1 * 1024 * 1024, // default value is 4MiB
					CompactionTableSizeMultiplier: 10,              // default value is 1
				}
				if v.nm, err = NewLevelDbNeedleMap(fileName+".ldb", indexFile, opts); err != nil {
					glog.V(0).Infof("loading leveldb %s error: %v", fileName+".ldb", err)
				}
			case NeedleMapLevelDbMedium:
				glog.V(0).Infoln("loading leveldb medium", fileName+".ldb")
				opts := &opt.Options{
					BlockCacheCapacity:            4 * 1024 * 1024, // default value is 8MiB
					WriteBuffer:                   2 * 1024 * 1024, // default value is 4MiB
					CompactionTableSizeMultiplier: 10,              // default value is 1
				}
				if v.nm, err = NewLevelDbNeedleMap(fileName+".ldb", indexFile, opts); err != nil {
					glog.V(0).Infof("loading leveldb %s error: %v", fileName+".ldb", err)
				}
			case NeedleMapLevelDbLarge:
				glog.V(0).Infoln("loading leveldb large", fileName+".ldb")
				opts := &opt.Options{
					BlockCacheCapacity:            8 * 1024 * 1024, // default value is 8MiB
					WriteBuffer:                   4 * 1024 * 1024, // default value is 4MiB
					CompactionTableSizeMultiplier: 10,              // default value is 1
				}
				if v.nm, err = NewLevelDbNeedleMap(fileName+".ldb", indexFile, opts); err != nil {
					glog.V(0).Infof("loading leveldb %s error: %v", fileName+".ldb", err)
				}
			}
		}
	}

	if !hasVolumeInfoFile {
		v.volumeInfo.Version = uint32(v.SuperBlock.Version)
		v.SaveVolumeInfo()
	}

	stats.VolumeServerVolumeCounter.WithLabelValues(v.Collection, "volume").Inc()

	return err
}
